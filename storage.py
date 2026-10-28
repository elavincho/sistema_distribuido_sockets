"""
storage.py - Capa de Persistencia Distribuida (PostgreSQL y S3/MinIO)

Maneja dos niveles de almacenamiento:
1. Base de Datos Relacional (PostgreSQL): Registro estructurado de tareas, estado, tiempos.
2. Almacenamiento de Objetos (Amazon S3 / MinIO): Cargas pesadas, matrices, imágenes procesadas.
Incluye fallback automático a SQLite y sistema de archivos local para entornos sin Docker.
"""

import os
import json
import sqlite3
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("DistributedStorage")

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


class StorageEngine:
    def __init__(self):
        self.pg_conn = None
        self.s3_client = None
        self._init_postgres()
        self._init_s3()

    def _init_postgres(self):
        pg_host = os.getenv("POSTGRES_HOST")
        if HAS_PSYCOPG2 and pg_host:
            try:
                self.pg_conn = psycopg2.connect(
                    host=pg_host,
                    port=int(os.getenv("POSTGRES_PORT", "5432")),
                    dbname=os.getenv("POSTGRES_DB", "tasks_db"),
                    user=os.getenv("POSTGRES_USER", "postgres"),
                    password=os.getenv("POSTGRES_PASSWORD", "postgres")
                )
                self.pg_conn.autocommit = True
                with self.pg_conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS distributed_tasks (
                            task_id VARCHAR(64) PRIMARY KEY,
                            client_id VARCHAR(64) NOT NULL,
                            task_type VARCHAR(32) NOT NULL,
                            priority VARCHAR(16) NOT NULL,
                            status VARCHAR(32) NOT NULL,
                            payload_summary TEXT,
                            worker_id VARCHAR(64),
                            duration_ms REAL,
                            s3_artifact_url TEXT,
                            error_message TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            completed_at TIMESTAMP
                        );
                    """)
                logger.info("[Storage: PostgreSQL] Conectado a base de datos de producción.")
                return
            except Exception as e:
                logger.warning(f"No se pudo conectar a PostgreSQL ({e}). Usando SQLite local.")

        # Fallback local con SQLite
        self.sqlite_conn = sqlite3.connect("distributed_tasks.db", check_same_thread=False)
        with self.sqlite_conn:
            self.sqlite_conn.execute("""
                CREATE TABLE IF NOT EXISTS distributed_tasks (
                    task_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_summary TEXT,
                    worker_id TEXT,
                    duration_ms REAL,
                    s3_artifact_url TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                );
            """)
        logger.info("[Storage: SQLite Fallback] Base de datos local inicializada.")

    def _init_s3(self):
        s3_endpoint = os.getenv("S3_ENDPOINT_URL")  # Para MinIO
        if HAS_BOTO3 and (s3_endpoint or os.getenv("AWS_ACCESS_KEY_ID")):
            try:
                self.s3_client = boto3.client(
                    "s3",
                    endpoint_url=s3_endpoint,
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
                )
                logger.info("[Storage: S3/MinIO] Cliente de objetos configurado.")
                return
            except Exception as e:
                logger.warning(f"No se pudo inicializar cliente S3 ({e}). Usando directorio local.")

        # Fallback local
        os.makedirs("local_s3_bucket/artifacts", exist_ok=True)
        logger.info("[Storage: S3 Local Fallback] Usando almacenamiento en disco local.")

    def record_task_created(self, task_id: str, client_id: str, task_type: str, priority: str, payload_summary: str):
        sql = """
            INSERT INTO distributed_tasks (task_id, client_id, task_type, priority, status, payload_summary)
            VALUES (%s, %s, %s, %s, 'PENDING', %s)
            ON CONFLICT (task_id) DO UPDATE SET status = 'PENDING';
        """
        if self.pg_conn:
            with self.pg_conn.cursor() as cur:
                cur.execute(sql, (task_id, client_id, task_type, priority, payload_summary))
        else:
            with self.sqlite_conn:
                self.sqlite_conn.execute(
                    "INSERT OR REPLACE INTO distributed_tasks (task_id, client_id, task_type, priority, status, payload_summary) VALUES (?, ?, ?, ?, 'PENDING', ?)",
                    (task_id, client_id, task_type, priority, payload_summary)
                )

    def record_task_completed(self, task_id: str, worker_id: str, duration_ms: float, s3_url: str, status: str = "COMPLETED"):
        sql = """
            UPDATE distributed_tasks 
            SET status = %s, worker_id = %s, duration_ms = %s, s3_artifact_url = %s, completed_at = CURRENT_TIMESTAMP
            WHERE task_id = %s;
        """
        if self.pg_conn:
            with self.pg_conn.cursor() as cur:
                cur.execute(sql, (status, worker_id, duration_ms, s3_url, task_id))
        else:
            with self.sqlite_conn:
                self.sqlite_conn.execute(
                    "UPDATE distributed_tasks SET status = ?, worker_id = ?, duration_ms = ?, s3_artifact_url = ?, completed_at = CURRENT_TIMESTAMP WHERE task_id = ?",
                    (status, worker_id, duration_ms, s3_url, task_id)
                )

    def record_task_failed(self, task_id: str, worker_id: str, error: str):
        sql = """
            UPDATE distributed_tasks 
            SET status = 'FAILED', worker_id = %s, error_message = %s, completed_at = CURRENT_TIMESTAMP
            WHERE task_id = %s;
        """
        if self.pg_conn:
            with self.pg_conn.cursor() as cur:
                cur.execute(sql, (worker_id, error, task_id))
        else:
            with self.sqlite_conn:
                self.sqlite_conn.execute(
                    "UPDATE distributed_tasks SET status = 'FAILED', worker_id = ?, error_message = ?, completed_at = CURRENT_TIMESTAMP WHERE task_id = ?",
                    (worker_id, error, task_id)
                )

    def upload_s3_artifact(self, bucket: str, key: str, data: Dict[str, Any]) -> str:
        data_bytes = json.dumps(data, indent=2).encode("utf-8")
        if self.s3_client:
            try:
                self.s3_client.put_object(Bucket=bucket, Key=key, Body=data_bytes, ContentType="application/json")
                return f"s3://{bucket}/{key}"
            except Exception as e:
                logger.warning(f"Error subiendo a S3 ({e}), guardando localmente.")

        # Fallback local
        local_path = os.path.join("local_s3_bucket", key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(data_bytes)
        return f"file://{os.path.abspath(local_path)}"


_STORAGE = None

def get_storage_engine():
    global _STORAGE
    if _STORAGE is None:
        _STORAGE = StorageEngine()
    return _STORAGE
