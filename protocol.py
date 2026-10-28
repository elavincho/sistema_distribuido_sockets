"""
protocol.py - Módulo de Protocolo y Tramado Binario de Sockets

Implementa el protocolo de comunicación sobre TCP con prefijo de longitud:
[ 4 Bytes: Longitud del Payload (Big-Endian uint32) ] + [ Payload JSON en UTF-8 ]
Evita la fragmentación y concatenación de tramas de TCP (Stream Framing).
"""

import json
import struct
import socket
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict

# Encabezado estándar: 4 bytes unsigned integer en orden de red (Big-Endian)
HEADER_FORMAT = "!I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAX_MESSAGE_SIZE = 16 * 1024 * 1024  # Límite de seguridad: 16 MB


class ProtocolError(Exception):
    """Excepción para errores de framing, encoding o violación de protocolo."""
    pass


@dataclass
class TaskRequest:
    task_id: str
    client_id: str
    task_type: str
    priority: str
    payload: Dict[str, Any]
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TaskResponse:
    task_id: str
    status: str
    worker_id: str
    thread_id: int
    execution_time_ms: float
    s3_artifact_url: Optional[str] = None
    result_summary: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def pack_message(data: Dict[str, Any]) -> bytes:
    """
    Serializa un diccionario a JSON en UTF-8 y le añade el encabezado binario
    de 4 bytes con la longitud exacta.
    """
    try:
        json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
        payload_len = len(json_bytes)
        if payload_len > MAX_MESSAGE_SIZE:
            raise ProtocolError(f"Mensaje excede el tamaño máximo permitido ({payload_len} > {MAX_MESSAGE_SIZE})")
        header = struct.pack(HEADER_FORMAT, payload_len)
        return header + json_bytes
    except Exception as e:
        raise ProtocolError(f"Error empaquetando mensaje: {e}") from e


def read_exact(sock: socket.socket, num_bytes: int) -> bytes:
    """
    Lee exactamente num_bytes del socket TCP acumulando fragmentos.
    Si el socket se cierra antes de completar los bytes, lanza ConnectionError.
    """
    buf = bytearray()
    while len(buf) < num_bytes:
        chunk = sock.recv(num_bytes - len(buf))
        if not chunk:
            if len(buf) == 0:
                raise ConnectionResetError("Conexión cerrada por el peer remoto")
            raise ConnectionError(f"Stream truncado: esperados {num_bytes} bytes, recibidos {len(buf)}")
        buf.extend(chunk)
    return bytes(buf)


def unpack_message(sock: socket.socket) -> Dict[str, Any]:
    """
    Lee una trama completa del socket: primero 4 bytes de longitud, luego el payload.
    """
    header_bytes = read_exact(sock, HEADER_SIZE)
    (payload_len,) = struct.unpack(HEADER_FORMAT, header_bytes)
    
    if payload_len > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"Tamaño de trama inválido: {payload_len} bytes")
        
    payload_bytes = read_exact(sock, payload_len)
    try:
        return json.loads(payload_bytes.decode("utf-8"))
    except UnicodeDecodeError as e:
        raise ProtocolError(f"Error decodificando UTF-8: {e}") from e
    except json.JSONDecodeError as e:
        raise ProtocolError(f"JSON malformado recibido en socket: {e}") from e
