"""
message_queue.py - Abstracción de Cola de Mensajes (RabbitMQ con Fallback)

Provee una interfaz consistente para publicar y consumir tareas y resultados
usando RabbitMQ (librería pika) con ACK manual y colas durables.
Si RabbitMQ no está disponible, conmuta transparentemente a un broker en memoria
thread-safe para permitir pruebas unitarias y desarrollo local inmediato.
"""

import os
import json
import time
import queue
import logging
import threading
from typing import Dict, Any, Optional

logger = logging.getLogger("MessageBroker")

try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False


class InMemoryBroker:
    """Broker en memoria para desarrollo o cuando RabbitMQ no está corriendo."""
    def __init__(self):
        self.task_queue = queue.Queue()
        self.result_dict: Dict[str, Dict[str, Any]] = {}
        self.result_events: Dict[str, threading.Event] = {}
        self.lock = threading.Lock()
        logger.info("[Broker: InMemory] Operando con colas en memoria sincronizadas.")

    def publish_task(self, task_envelope: Dict[str, Any]):
        self.task_queue.put(task_envelope)

    def consume_task(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        try:
            return self.task_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def publish_result(self, result_envelope: Dict[str, Any]):
        task_id = result_envelope["task_id"]
        with self.lock:
            self.result_dict[task_id] = result_envelope
            if task_id in self.result_events:
                self.result_events[task_id].set()

    def await_result(self, task_id: str, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        event = threading.Event()
        with self.lock:
            if task_id in self.result_dict:
                return self.result_dict[task_id]
            self.result_events[task_id] = event

        flag = event.wait(timeout=timeout)
        with self.lock:
            self.result_events.pop(task_id, None)
            return self.result_dict.pop(task_id, None) if flag else None


class RabbitMQBroker:
    """Implementación oficial sobre RabbitMQ usando pika y protocolo AMQP 0-9-1."""
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(user, password)
        self.task_queue_name = "tasks.queue"
        self.results_queue_name = "results.queue"
        self._setup_queues()
        logger.info(f"[Broker: RabbitMQ] Conectado a {host}:{port}")

    def _get_connection(self):
        params = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=self.credentials,
            heartbeat=60
        )
        return pika.BlockingConnection(params)

    def _setup_queues(self):
        conn = self._get_connection()
        ch = conn.channel()
        ch.queue_declare(queue=self.task_queue_name, durable=True)
        ch.queue_declare(queue=self.results_queue_name, durable=True)
        conn.close()

    def publish_task(self, task_envelope: Dict[str, Any]):
        conn = self._get_connection()
        ch = conn.channel()
        ch.basic_publish(
            exchange="",
            routing_key=self.task_queue_name,
            body=json.dumps(task_envelope),
            properties=pika.BasicProperties(delivery_mode=2)  # Persistente
        )
        conn.close()

    def consume_task(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        ch = conn.channel()
        method, properties, body = ch.basic_get(queue=self.task_queue_name, auto_ack=True)
        conn.close()
        if body:
            return json.loads(body.decode("utf-8"))
        return None

    def publish_result(self, result_envelope: Dict[str, Any]):
        conn = self._get_connection()
        ch = conn.channel()
        ch.basic_publish(
            exchange="",
            routing_key=self.results_queue_name,
            body=json.dumps(result_envelope),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        conn.close()

    def await_result(self, task_id: str, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        # En producción se usa correlation_id y colas RPC dedicadas o Redis pub/sub
        conn = self._get_connection()
        ch = conn.channel()
        start = time.time()
        while time.time() - start < timeout:
            method, props, body = ch.basic_get(queue=self.results_queue_name, auto_ack=False)
            if body:
                payload = json.loads(body.decode("utf-8"))
                if payload.get("task_id") == task_id:
                    ch.basic_ack(method.delivery_tag)
                    conn.close()
                    return payload
                else:
                    # Devolver a la cola si pertenece a otra tarea
                    ch.basic_nack(method.delivery_tag, requeue=True)
            time.sleep(0.05)
        conn.close()
        return None


_GLOBAL_BROKER = None

def get_message_broker():
    global _GLOBAL_BROKER
    if _GLOBAL_BROKER is not None:
        return _GLOBAL_BROKER

    rabbitmq_host = os.getenv("RABBITMQ_HOST")
    if HAS_PIKA and rabbitmq_host:
        try:
            port = int(os.getenv("RABBITMQ_PORT", "5672"))
            user = os.getenv("RABBITMQ_USER", "guest")
            pwd = os.getenv("RABBITMQ_PASSWORD", "guest")
            _GLOBAL_BROKER = RabbitMQBroker(rabbitmq_host, port, user, pwd)
            return _GLOBAL_BROKER
        except Exception as e:
            logger.warning(f"No se pudo conectar a RabbitMQ ({e}). Usando fallback en memoria.")

    _GLOBAL_BROKER = InMemoryBroker()
    return _GLOBAL_BROKER
