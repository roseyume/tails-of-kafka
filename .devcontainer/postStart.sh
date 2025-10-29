#!/usr/bin/env bash
set -e

cp backup/docker-compose.bkup docker-compose.yml

echo "FastAPI and Vite are now managed as containers in docker-compose."

echo "Writing external API URL for frontend and starting backend + frontend containers..."

# Derive an external backend URL suitable for opening the frontend in a browser.
# In Codespaces the preview URL pattern is: https://$CODESPACE_NAME-8000.githubpreview.dev
if [ -n "$CODESPACE_NAME" ]; then
  BACKEND_URL="https://${CODESPACE_NAME}-8000.githubpreview.dev"
else
  # Fallback to PUBLIC_IP (or localhost) if not in Codespaces
  BACKEND_URL="http://${PUBLIC_IP:-127.0.0.1}:8000"
fi

echo "VITE_API_BASE_URL=$BACKEND_URL" > frontend/.env.development
echo "API URL set to $BACKEND_URL"

echo "Starting Docker services..."
docker compose up -d

echo "Waiting for Kafka to get ready..."
docker compose ps


# echo "Starting backend and frontend containers via docker compose..."
# docker compose up -d --build backend frontend || {
#   echo "Failed to start backend/frontend via docker compose. You can try running:\n  docker compose up --build backend frontend" >&2
# }

echo "Environment online (backend + frontend started as containers)."
