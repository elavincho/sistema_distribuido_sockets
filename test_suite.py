"""
test_suite.py - Pruebas Unitarias y de Integración para el Sistema Distribuido
"""

import io
import json
import time
import struct
import socket
import threading
import pytest

from protocol import pack_message, unpack_message, read_exact, HEADER_FORMAT, ProtocolError
from worker import execute_matrix_multiplication, execute_image_filter, execute_data_aggregation, execute_hash_mining


def test_protocol_packing_and_unpacking():
    """Valida que el empaquetado binario coincida con el encabezado de 4 bytes."""
    data = {
        "task_id": "test-1234",
        "task_type": "matrix_mult",
        "payload": {"size": 32}
    }
    packed = pack_message(data)
    
    # 4 bytes de header
    assert len(packed) > 4
    header = packed[:4]
    (payload_len,) = struct.unpack(HEADER_FORMAT, header)
    
    payload_raw = packed[4:]
    assert len(payload_raw) == payload_len
    
    decoded = json.loads(payload_raw.decode("utf-8"))
    assert decoded["task_id"] == "test-1234"
    assert decoded["payload"]["size"] == 32


def test_matrix_computation_deterministic():
    """Verifica que el cómputo matricial devuelva traza y dimensiones correctas."""
    res = execute_matrix_multiplication(size=16, seed=42)
    assert res["dimensions"] == [16, 16]
    assert "matrix_trace" in res
    assert len(res["sample_elements"]) == 3


def test_image_filter_computation():
    res = execute_image_filter("gaussian_blur", width=30, height=30)
    assert res["filter_applied"] == "gaussian_blur"
    assert res["resolution"] == "30x30"
    assert res["processed_pixel_sum"] > 0


def test_data_aggregation_metrics():
    res = execute_data_aggregation(sample_count=1000)
    assert res["samples_analyzed"] == 1000
    assert "mean" in res
    assert "std_dev" in res
    assert res["min"] <= res["max"]


def test_hash_mining_pow():
    res = execute_hash_mining(difficulty=2)
    assert res["hash"].startswith("00")
    assert res["nonce"] >= 0


def test_socket_echo_loopback():
    """Prueba de socket real en puerto libre loopback."""
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(1)
    port = server_sock.getsockname()[1]

    received = {}

    def server_thread():
        client_conn, _ = server_sock.accept()
        msg = unpack_message(client_conn)
        received.update(msg)
        resp = {"status": "OK", "echo": msg["task_id"]}
        client_conn.sendall(pack_message(resp))
        client_conn.close()

    t = threading.Thread(target=server_thread, daemon=True)
    t.start()

    # Cliente
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_sock.connect(("127.0.0.1", port))
    client_sock.sendall(pack_message({"task_id": "echo-abc-99"}))
    resp = unpack_message(client_sock)
    client_sock.close()
    server_sock.close()

    assert received.get("task_id") == "echo-abc-99"
    assert resp.get("status") == "OK"
    assert resp.get("echo") == "echo-abc-99"
