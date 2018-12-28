#!/usr/bin/env bash

docker-compose down
docker-compose build
docker-compose up -d
echo "Starting services..."
sleep 300
echo "Services should be up. Logs..."
docker-compose logs
