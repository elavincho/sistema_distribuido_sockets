FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema mínimas
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY protocol.py .
COPY storage.py .
COPY message_queue.py .
COPY server.py .
COPY worker.py .
COPY client.py .

EXPOSE 9001 9002

CMD ["python", "server.py"]
