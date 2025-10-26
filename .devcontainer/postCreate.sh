#!/usr/bin/env bash
set -e
export PUBLIC_IP=127.0.0.1

echo "Installing Python deps..."
pip install -r requirements.txt

echo "Installing npm deps..."
cd frontend
npm install
cd ..
