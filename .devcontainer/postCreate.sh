#!/usr/bin/env bash
docker version || echo 'Docker not available yet'

set -e
export PUBLIC_IP=127.0.0.1

echo "Installing Python deps..."
pip install -r backend/requirements.txt

echo "Installing npm deps..."
cd frontend
npm install
cd ..
