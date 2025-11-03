FROM python:3.11-slim

# Install system deps required for some Python packages (librdkafka for confluent_kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    librdkafka-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN ls -alrt

RUN pip install --no-cache-dir -r /app/requirements.txt

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
