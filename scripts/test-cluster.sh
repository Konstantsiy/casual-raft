#!/bin/bash

PORTS=(8001 8002 8003 8004 8005)

echo "=== Finding Leader ==="
for port in "${PORTS[@]}"; do
  echo -n "Node on port $port: "
  curl -s http://localhost:$port/health || echo "Not responding"
done

echo -e "\n=== Sending Command to Leader ==="
for port in "${PORTS[@]}"; do
  response=$(curl -s -X POST http://localhost:$port/command \
    -H "Content-Type: application/json" \
    -d '["test-command-'$(date +%s)'"]' 2>&1)

  if [[ $response != *"not leader"* ]] && [[ $response != *"error"* ]]; then
    echo "✓ Command sent successfully to port $port"
    break
  fi
done

echo -e "\n=== Checking Logs on All Nodes ==="
for port in "${PORTS[@]}"; do
  echo "Node on port $port:"
  curl -s http://localhost:$port/logs 2>/dev/null || echo "  Not available"
done