#!/bin/bash

set -euo pipefail

# Anthropic "Messages" API shape
SERVICE=${SERVICE:-v1/messages}
MODEL=${MODEL:-claude/claude-haiku-4-5-20251001}
INPUT=${INPUT:-"What model are you ?"}
MAX_TOKENS=${MAX_TOKENS:-128}
PID=${PID:-13}
TID=${TID:-400}
CUSTOMER_ID=${CUSTOMER_ID:-"customer-1"}

DATA=$(cat <<EOF
{
  "metadata": {
    "pid": ${PID},
    "tid": ${TID},
    "customer_id": "${CUSTOMER_ID}"
  },
  "model": "${MODEL}",
  "max_tokens": ${MAX_TOKENS},
  "messages": [
    { "role": "user", "content": "${INPUT}" }
  ]
}
EOF
)

export SERVICE MODEL INPUT MAX_TOKENS PID TID CUSTOMER_ID DATA
exec "$(dirname "$0")/ai-post.sh"

