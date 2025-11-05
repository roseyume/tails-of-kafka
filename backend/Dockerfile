FROM python:3.11-slim

ENV DOCKER_HOST="unix:///var/run/docker.sock"

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

# Create a non-root user and add it to the docker group
RUN groupadd -f docker && useradd -m -s /bin/bash fastApiUser && usermod -aG docker fastApiUser

USER fastApiUser

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
