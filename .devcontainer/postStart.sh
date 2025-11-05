#!/usr/bin/env bash

cp backup/docker-compose.bkup docker-compose.yml

echo "Starting Docker services..."
docker compose up -d

# echo "Starting backend and frontend containers via docker compose..."
# docker compose up -d --build backend frontend || {
#   echo "Failed to start backend/frontend via docker compose. You can try running:\n  docker compose up --build backend frontend" >&2
# }

echo "Environment online (backend + frontend started as containers)."
echo 'Reminder: ports 8000 (backend) and 3000 (frontend) are forwarded to the Codespace. To expose them publicly, open the Ports panel and set visibility to Public.'