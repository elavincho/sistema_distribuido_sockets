"""
server.py - Servidor Gateway de Sockets Multihilo

Recibe conexiones TCP de clientes (vía HAProxy o directo), extrae las tramas
binarias con protocolo de longitud prefijada, registra la tarea en PostgreSQL,
despacha el mensaje a la cola RabbitMQ y retorna el resultado al cliente.
"""

import sys
import time
import uuid
import logging
import argparse
import socketserver
from typing import Dict, Any

from protocol import pack_message, unpack_message, ProtocolError
from message_queue import get_message_broker
from storage import get_storage_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SERVER-%(process)d] %(message)s"
)
logger = logging.getLogger("SocketServer")


class TaskSocketHandler(socketserver.BaseRequestHandler):
    """
    Manejador para cada conexión TCP individual. Se ejecuta en un hilo separado
    gestionado por ThreadingTCPServer.
    """

    def handle(self):
        client_address = self.client_address
        logger.info(f"Nueva conexión TCP aceptada desde {client_address}")
        broker = self.server.broker
        storage = self.server.storage
        server_id = self.server.server_id

        try:
            while True:
                try:
                    # Leer trama con encabezado binario
                    data = unpack_message(self.request)
                except (ConnectionResetError, ConnectionError):
                    logger.info(f"Cliente {client_address} cerró la conexión.")
                    break
                except ProtocolError as pe:
                    logger.error(f"Violación de protocolo de {client_address}: {pe}")
                    err_response = {
                        "status": "ERROR",
                        "error": str(pe),
                        "server_id": server_id
                    }
                    self.request.sendall(pack_message(err_response))
                    break

                task_id = data.get("task_id", str(uuid.uuid4()))
                client_id = data.get("client_id", "anonymous")
                task_type = data.get("task_type", "generic")
                priority = data.get("priority", "normal")
                payload = data.get("payload", {})
                sync_mode = data.get("sync", True)

                logger.info(f"Tarea recibida [{task_id}] tipo='{task_type}' de '{client_id}'")

                # 1. Registrar metadatos en PostgreSQL
                storage.record_task_created(
                    task_id=task_id,
                    client_id=client_id,
                    task_type=task_type,
                    priority=priority,
                    payload_summary=str(payload)[:200]
                )

                # 2. Publicar la tarea en la cola RabbitMQ
                task_envelope = {
                    "task_id": task_id,
                    "client_id": client_id,
                    "task_type": task_type,
                    "priority": priority,
                    "payload": payload,
                    "enqueued_at": time.time(),
                    "gateway_server_id": server_id
                }
                
                # Si el cliente solicitó modo asincrónico ("fire & forget" con poll posterior)
                if not sync_mode:
                    broker.publish_task(task_envelope)
                    ack_response = {
                        "task_id": task_id,
                        "status": "ACCEPTED",
                        "message": "Tarea encolada con éxito",
                        "server_id": server_id
                    }
                    self.request.sendall(pack_message(ack_response))
                    continue

                # Modo sincrónico: Publicar y esperar respuesta correlacionada en cola de resultados
                broker.publish_task(task_envelope)
                logger.info(f"Esperando resultado de worker para [{task_id}]...")
                
                result = broker.await_result(task_id, timeout=30.0)
                
                if result:
                    logger.info(f"Resultado obtenido para [{task_id}] en {result.get('execution_time_ms', 0):.1f}ms")
                    self.request.sendall(pack_message(result))
                else:
                    logger.warning(f"Timeout esperando respuesta de worker para tarea [{task_id}]")
                    timeout_resp = {
                        "task_id": task_id,
                        "status": "TIMEOUT",
                        "error": "El cluster de workers no respondió en el tiempo límite (30s)",
                        "server_id": server_id
                    }
                    self.request.sendall(pack_message(timeout_resp))

        except Exception as e:
            logger.exception(f"Error inesperado procesando socket de {client_address}: {e}")
        finally:
            logger.info(f"Conexión finalizada con {client_address}")


class DistributedSocketServer(socketserver.ThreadingTCPServer):
    """Servidor TCP multihilo reutilizable con soporte de broker y base de datos."""
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, handler_cls, server_id: str):
        super().__init__(server_address, handler_cls)
        self.server_id = server_id
        self.broker = get_message_broker()
        self.storage = get_storage_engine()


def main():
    parser = argparse.ArgumentParser(description="Servidor Socket Gateway para Sistema Distribuido")
    parser.add_argument("--host", default="0.0.0.0", help="Dirección IP de escucha (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=9001, help="Puerto TCP de escucha (default: 9001)")
    parser.add_argument("--server-id", default="srv-node-1", help="Identificador único del servidor")
    args = parser.parse_args()

    logger.info(f"Iniciando Servidor Socket [{args.server_id}] en {args.host}:{args.port}")
    server = DistributedSocketServer((args.host, args.port), TaskSocketHandler, server_id=args.server_id)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Deteniendo servidor socket ordenadamente...")
        server.shutdown()
        server.server_close()
        sys.exit(0)


if __name__ == "__main__":
    main()
