#!/bin/bash

curl -s -X POST  http://localhost:1/command \
  -H "Content-Type: application/json" \
  -d '["test-command-'$(uuidgen)'"]' 2>&1s