"""
client.py - Cliente TCP Socket para Sistema Distribuido

Se conecta al Balanceador de Carga (o Servidor Socket directo), empaqueta
peticiones con prefijo de longitud binaria (Big-Endian) y recibe respuestas JSON.
Incluye comandos para envío síncrono, asíncrono, consulta por ID y benchmarks.
"""

import time
import uuid
import socket
import argparse
import statistics
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from protocol import pack_message, unpack_message, ProtocolError


class DistributedSocketClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 9000, client_id: str = "cli_client"):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.sock = None

    def connect(self):
        """Abre conexión de socket TCP con timeout."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(35.0)  # Timeout para evitar bloqueos infinitos
        self.sock.connect((self.host, self.port))

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def send_task_sync(self, task_type: str, payload: Dict[str, Any], priority: str = "normal") -> Dict[str, Any]:
        """Envía una tarea y espera sincrónicamente la respuesta del cluster."""
        self.connect()
        try:
            task_id = str(uuid.uuid4())
            request_data = {
                "task_id": task_id,
                "client_id": self.client_id,
                "task_type": task_type,
                "priority": priority,
                "payload": payload,
                "sync": True,
                "timestamp": time.time()
            }
            # Enviar trama binaria prefijada
            self.sock.sendall(pack_message(request_data))
            
            # Recibir respuesta binaria prefijada
            response = unpack_message(self.sock)
            return response
        finally:
            self.close()

    def send_task_async(self, task_type: str, payload: Dict[str, Any], priority: str = "normal") -> Dict[str, Any]:
        """Envía una tarea en modo asíncrono y retorna de inmediato con el task_id."""
        self.connect()
        try:
            task_id = str(uuid.uuid4())
            request_data = {
                "task_id": task_id,
                "client_id": self.client_id,
                "task_type": task_type,
                "priority": priority,
                "payload": payload,
                "sync": False,
                "timestamp": time.time()
            }
            self.sock.sendall(pack_message(request_data))
            ack = unpack_message(self.sock)
            ack["task_id"] = task_id
            return ack
        finally:
            self.close()


def run_benchmark(host: str, port: int, total_tasks: int, concurrency: int, task_type: str):
    """Ejecuta una prueba de estrés concurrente contra el cluster de sockets."""
    print(f"\nIniciando Benchmark: {total_tasks} tareas, concurrencia={concurrency}, tipo={task_type}...")
    latencies: List[float] = []
    success_count = 0
    failure_count = 0
    start_total = time.time()

    def worker_job(index: int) -> Tuple[bool, float]:
        client = DistributedSocketClient(host=host, port=port, client_id=f"bench_client_{index}")
        t0 = time.time()
        try:
            payload = {"size": 32} if task_type == "matrix_mult" else {"sample_count": 5000}
            resp = client.send_task_sync(task_type, payload)
            elapsed_ms = (time.time() - t0) * 1000.0
            if resp.get("status") == "COMPLETED":
                return True, elapsed_ms
            return False, elapsed_ms
        except Exception as e:
            return False, (time.time() - t0) * 1000.0

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker_job, i) for i in range(total_tasks)]
        for f in as_completed(futures):
            ok, lat = f.result()
            latencies.append(lat)
            if ok:
                success_count += 1
            else:
                failure_count += 1

    total_time = time.time() - start_total
    throughput = total_tasks / total_time if total_time > 0 else 0

    print("\n" + "=" * 50)
    print("           REPORTE DE RENDIMIENTO (BENCHMARK)")
    print("=" * 50)
    print(f"Total de tareas ejecutadas:  {total_tasks}")
    print(f"Clientes concurrentes:       {concurrency}")
    print(f"Tareas exitosas:             {success_count} ({success_count/total_tasks*100:.1f}%)")
    print(f"Tareas fallidas:             {failure_count}")
    print(f"Tiempo total:                {total_time:.2f} s")
    print(f"Throughput:                  {throughput:.2f} tareas/segundo")
    if latencies:
        latencies.sort()
        print(f"Latencia Mínima:             {latencies[0]:.2f} ms")
        print(f"Latencia Media (p50):        {statistics.median(latencies):.2f} ms")
        p95_idx = int(len(latencies) * 0.95)
        print(f"Latencia Percentil 95 (p95): {latencies[p95_idx]:.2f} ms")
        print(f"Latencia Máxima:             {latencies[-1]:.2f} ms")
    print("=" * 50 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Cliente Socket para Sistema Distribuido")
    parser.add_argument("--host", default="127.0.0.1", help="Host del Balanceador o Servidor (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9000, help="Puerto TCP (default: 9000)")
    parser.add_argument("--client-id", default="cli_user_01", help="ID del cliente emisor")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcomando: submit (síncrono)
    p_submit = subparsers.add_parser("submit", help="Enviar tarea y esperar resultado")
    p_submit.add_argument("--task-type", default="matrix_mult", choices=["matrix_mult", "image_filter", "data_aggregate", "hash_mining"])
    p_submit.add_argument("--size", type=int, default=64, help="Tamaño de matriz (para matrix_mult)")
    p_submit.add_argument("--filter-name", default="gaussian_blur", help="Filtro (para image_filter)")
    p_submit.add_argument("--sample-count", type=int, default=10000, help="Muestras (para data_aggregate)")
    p_submit.add_argument("--difficulty", type=int, default=3, help="Dificultad (para hash_mining)")

    # Subcomando: async-submit (asíncrono)
    p_async = subparsers.add_parser("async-submit", help="Enviar tarea asíncrona")
    p_async.add_argument("--task-type", default="matrix_mult")
    p_async.add_argument("--size", type=int, default=64)

    # Subcomando: benchmark
    p_bench = subparsers.add_parser("benchmark", help="Ejecutar prueba de estrés concurrente")
    p_bench.add_argument("--tasks", type=int, default=20, help="Total de tareas")
    p_bench.add_argument("--concurrency", type=int, default=5, help="Hilos concurrentes")
    p_bench.add_argument("--task-type", default="matrix_mult")

    args = parser.parse_args()

    client = DistributedSocketClient(host=args.host, port=args.port, client_id=args.client_id)

    if args.command == "submit":
        payload = {}
        if args.task_type == "matrix_mult":
            payload = {"size": args.size}
        elif args.task_type == "image_filter":
            payload = {"filter_name": args.filter_name}
        elif args.task_type == "data_aggregate":
            payload = {"sample_count": args.sample_count}
        elif args.task_type == "hash_mining":
            payload = {"difficulty": args.difficulty}

        print(f"-> Conectando a {args.host}:{args.port}...")
        t0 = time.time()
        result = client.send_task_sync(args.task_type, payload)
        elapsed = (time.time() - t0) * 1000.0

        print(f"<- Respuesta recibida en {elapsed:.1f}ms:")
        import json
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif args.command == "async-submit":
        payload = {"size": args.size}
        ack = client.send_task_async(args.task_type, payload)
        print("<- Confirmación del servidor:")
        import json
        print(json.dumps(ack, indent=2, ensure_ascii=False))

    elif args.command == "benchmark":
        run_benchmark(args.host, args.port, args.tasks, args.concurrency, args.task_type)


if __name__ == "__main__":
    main()
