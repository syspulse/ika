#!/bin/bash

set -euo pipefail

SERVICE=${SERVICE:-v1/responses}

#SERVICE_URI=${SERVICE_URI:-https://api.openai.com/${SERVICE}}
SERVICE_URI=${SERVICE_URI:-http://localhost:8080/api/v1/ika/${SERVICE}}

# Optional. If empty, Authorization header is omitted (useful for local proxy).
OPENAI_API_KEY=${OPENAI_API_KEY:-}

AUTH_HEADER=()
if [[ -n "${OPENAI_API_KEY}" ]]; then
  AUTH_HEADER=(-H "Authorization: Bearer ${OPENAI_API_KEY}")
fi

curl -S -s -D /dev/stderr \
  -X POST \
  -H "Content-Type: application/json" \
  "${AUTH_HEADER[@]}" \
  -d "${DATA}" \
  "${SERVICE_URI}"
