"""
worker.py - Servidor Worker Distribuido con Pool de Hilos

Consume tareas desde RabbitMQ, distribuye la ejecución de cada tarea en un
ThreadPoolExecutor configurable, procesa cómputos intensivos, almacena artefactos
pesados en S3/MinIO, actualiza metadatos en PostgreSQL y publica el resultado
en la cola de resultados.
"""

import time
import math
import hashlib
import logging
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

from message_queue import get_message_broker
from storage import get_storage_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [WORKER-%(process)d-T%(thread)d] %(message)s"
)
logger = logging.getLogger("DistributedWorker")


# --- Funciones de Computación Real (Workloads) ---

def execute_matrix_multiplication(size: int, seed: int = 42) -> Dict[str, Any]:
    """Cálculo matricial intensivo en CPU para simular carga analítica."""
    # Generar matrices pseudoaleatorias determinísticas
    size = min(max(size, 8), 512)
    matrix_a = [[((i * 31 + j * 17 + seed) % 100) / 10.0 for j in range(size)] for i in range(size)]
    matrix_b = [[((i * 13 + j * 29 + seed) % 100) / 10.0 for j in range(size)] for i in range(size)]
    
    # Multiplicación matricial (O(N^3))
    result_matrix = [[0.0 for _ in range(size)] for _ in range(size)]
    for i in range(size):
        for k in range(size):
            r = matrix_a[i][k]
            for j in range(size):
                result_matrix[i][j] += r * matrix_b[k][j]
                
    trace = sum(result_matrix[i][i] for i in range(size))
    return {
        "dimensions": [size, size],
        "matrix_trace": round(trace, 4),
        "sample_elements": [result_matrix[0][0], result_matrix[size//2][size//2], result_matrix[size-1][size-1]]
    }


def execute_image_filter(filter_name: str, width: int = 120, height: int = 120) -> Dict[str, Any]:
    """Simula procesamiento de imágenes (convolución de matriz / blur / edge detection)."""
    # Crear buffer de imagen en memoria
    pixels = [[(x ^ y) % 256 for x in range(width)] for y in range(height)]
    # Aplicar convolución 3x3 simulada
    kernel = [1/9] * 9
    processed = 0
    for y in range(1, height - 1):
        for x in range(1, width - 1):
            val = sum(pixels[y + dy][x + dx] * kernel[i] for i, (dy, dx) in enumerate(
                [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 0), (0, 1), (1, -1), (1, 0), (1, 1)]
            ))
            processed += int(val)
            
    return {
        "filter_applied": filter_name,
        "resolution": f"{width}x{height}",
        "processed_pixel_sum": processed,
        "format": "RGB_8BIT"
    }


def execute_data_aggregation(sample_count: int) -> Dict[str, Any]:
    """Simula agregación estadística distribuida sobre lotes de datos."""
    count = min(max(sample_count, 100), 100_000)
    data = [(math.sin(i * 0.05) * 50.0 + 50.0) for i in range(count)]
    total = sum(data)
    mean = total / count
    variance = sum((x - mean) ** 2 for x in data) / count
    std_dev = math.sqrt(variance)
    return {
        "samples_analyzed": count,
        "mean": round(mean, 4),
        "variance": round(variance, 4),
        "std_dev": round(std_dev, 4),
        "min": round(min(data), 4),
        "max": round(max(data), 4)
    }


def execute_hash_mining(difficulty: int) -> Dict[str, Any]:
    """Prueba de trabajo / hash cryptográfico (Proof of Work)."""
    prefix = "0" * min(max(difficulty, 1), 6)
    nonce = 0
    start = time.time()
    while True:
        candidate = f"distrib-task-{nonce}".encode("utf-8")
        h = hashlib.sha256(candidate).hexdigest()
        if h.startswith(prefix):
            return {
                "difficulty": difficulty,
                "nonce": nonce,
                "hash": h,
                "iterations": nonce + 1,
                "calc_seconds": round(time.time() - start, 4)
            }
        nonce += 1
        if nonce > 2_000_000:
            return {"difficulty": difficulty, "status": "ABORTED_LIMIT", "nonce": nonce}


class DistributedWorkerService:
    def __init__(self, worker_id: str, thread_pool_size: int = 4):
        self.worker_id = worker_id
        self.pool_size = thread_pool_size
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_pool_size, thread_name_prefix=f"{worker_id}-th")
        self.broker = get_message_broker()
        self.storage = get_storage_engine()
        self.running = True
        logger.info(f"Worker [{worker_id}] inicializado con {thread_pool_size} hilos en el pool.")

    def process_task_job(self, task_envelope: Dict[str, Any]):
        """Esta función se ejecuta dentro de un hilo específico del ThreadPoolExecutor."""
        thread_id = threading.get_ident()
        task_id = task_envelope["task_id"]
        task_type = task_envelope.get("task_type", "unknown")
        payload = task_envelope.get("payload", {})
        start_time = time.time()

        logger.info(f"[Hilo {thread_id}] Procesando tarea [{task_id}] ({task_type})")

        try:
            # 1. Ejecutar la lógica de cómputo según el tipo
            if task_type == "matrix_mult":
                size = payload.get("size", 64)
                result_data = execute_matrix_multiplication(size)
            elif task_type == "image_filter":
                filt = payload.get("filter_name", "gaussian_blur")
                result_data = execute_image_filter(filt)
            elif task_type == "data_aggregate":
                count = payload.get("sample_count", 10000)
                result_data = execute_data_aggregation(count)
            elif task_type == "hash_mining":
                diff = payload.get("difficulty", 3)
                result_data = execute_hash_mining(diff)
            else:
                result_data = {"echo": payload, "note": "Tarea estándar completada"}

            duration_ms = (time.time() - start_time) * 1000.0

            # 2. Subir artefacto pesado a S3/MinIO
            s3_key = f"artifacts/{task_id}_{task_type}.json"
            s3_url = self.storage.upload_s3_artifact(
                bucket="distributed-tasks",
                key=s3_key,
                data={
                    "task_id": task_id,
                    "task_type": task_type,
                    "worker_id": self.worker_id,
                    "result": result_data,
                    "duration_ms": duration_ms
                }
            )

            # 3. Actualizar metadatos en PostgreSQL
            self.storage.record_task_completed(
                task_id=task_id,
                worker_id=self.worker_id,
                duration_ms=duration_ms,
                s3_url=s3_url,
                status="COMPLETED"
            )

            # 4. Publicar resultado en la cola de resultados de RabbitMQ
            response_envelope = {
                "task_id": task_id,
                "status": "COMPLETED",
                "worker_id": self.worker_id,
                "thread_id": thread_id,
                "execution_time_ms": round(duration_ms, 2),
                "s3_artifact_url": s3_url,
                "result_summary": result_data,
                "error": None
            }
            self.broker.publish_result(response_envelope)
            logger.info(f"[Hilo {thread_id}] Tarea [{task_id}] completada en {duration_ms:.2f}ms")

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000.0
            logger.exception(f"[Hilo {thread_id}] Error procesando tarea [{task_id}]: {e}")
            
            # Registrar fallo en Postgres
            self.storage.record_task_failed(task_id=task_id, worker_id=self.worker_id, error=str(e))
            
            # Publicar error en RabbitMQ para que el cliente lo reciba
            err_response = {
                "task_id": task_id,
                "status": "FAILED",
                "worker_id": self.worker_id,
                "thread_id": thread_id,
                "execution_time_ms": round(duration_ms, 2),
                "error": str(e)
            }
            self.broker.publish_result(err_response)

    def run_consumer_loop(self):
        """Bucle principal de consumo de la cola de RabbitMQ."""
        logger.info(f"Worker [{self.worker_id}] escuchando cola 'tasks.queue'...")
        while self.running:
            try:
                # Obtiene tarea de RabbitMQ (o cola simulada)
                task_envelope = self.broker.consume_task(timeout=1.0)
                if task_envelope:
                    # Encolar la ejecución en el pool de hilos
                    self.thread_pool.submit(self.process_task_job, task_envelope)
            except Exception as e:
                logger.error(f"Error en loop de consumo del worker: {e}")
                time.sleep(1.0)

    def stop(self):
        self.running = False
        self.thread_pool.shutdown(wait=True)
        logger.info(f"Worker [{self.worker_id}] apagado correctamente.")


def main():
    parser = argparse.ArgumentParser(description="Servidor Worker Distribuido")
    parser.add_argument("--worker-id", default="worker-1", help="Identificador del worker")
    parser.add_argument("--threads", type=int, default=4, help="Número de hilos en el ThreadPoolExecutor")
    args = parser.parse_args()

    worker = DistributedWorkerService(worker_id=args.worker_id, thread_pool_size=args.threads)
    try:
        worker.run_consumer_loop()
    except KeyboardInterrupt:
        worker.stop()


if __name__ == "__main__":
    main()
