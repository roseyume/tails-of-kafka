#!/usr/bin/env bash
set -e

cp backup/docker-compose.bkup docker-compose.yml

echo "Starting Docker services..."
docker compose up -d

echo "Waiting for Kafka to get ready..."
docker compose ps

echo "Waiting until services are healthy..."
until [ "$(docker compose ps -q kafka1 | xargs docker inspect -f '{{.State.Health.Status}}')" = "healthy" ]; do
  sleep 2
  echo "Kafka not healthy yet..."
done

echo "Launching FastAPI..."
cd backend
uvicorn main:app --port 8000 --host 0.0.0.0 >/tmp/api.log 2>&1 &
cd ..

BACKEND_URL="https://$CODESPACE_NAME-8000.githubpreview.dev"
echo "VITE_API_BASE_URL=$BACKEND_URL" > frontend/.env.development
echo "API URL set to $BACKEND_URL"

cd frontend
echo "Starting Vite frontend..."
npm run dev >/tmp/frontend.log 2>&1 &
cd ..

echo "Environment online!"
