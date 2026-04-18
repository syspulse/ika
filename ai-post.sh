#!/bin/bash

OPENAI_API_KEY=${OPENAI_API_KEY:-sk-1234567890-11111111111111-0001}

SERVICE_URI=${SERVICE_URI:-http://localhost:8080/api/v1/ika/v1/chat/completions}

DATA_JSON=${1:-test/ai/REQ_chat_completion-1.json}

COUNT=${COUNT:-0}
SLEEP=${SLEEP:-0}

curl -S -s -D /dev/stderr \
  -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d @"$DATA_JSON" \
  $SERVICE_URI

