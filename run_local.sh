#!/bin/bash
# Script de inicio rápido local sin Docker
echo "Creando entorno virtual..."
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

echo "Para iniciar los servicios en terminales separadas:"
echo "Terminal 1: python server.py --port 9001"
echo "Terminal 2: python worker.py --worker-id worker-1 --threads 4"
echo "Terminal 3: python client.py submit --task-type matrix_mult"
