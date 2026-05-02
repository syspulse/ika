#!/bin/bash

set -euo pipefail

# OpenAI legacy "Completions" API shape
SERVICE=${SERVICE:-v1/completions}
MODEL=${MODEL:-openai/gpt-4o-mini}
PROMPT=${PROMPT:-${INPUT:-"What model are you ?"}}
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
  "prompt": "${PROMPT}",
  "max_tokens": ${MAX_TOKENS}
}
EOF
)

export SERVICE MODEL MAX_TOKENS PID TID CUSTOMER_ID DATA
exec "$(dirname "$0")/ai-post.sh"

