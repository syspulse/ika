#!/bin/bash

set -euo pipefail

# OpenAI "Responses" API shape
SERVICE=${SERVICE:-v1/responses}
MODEL=${MODEL:-openai/gpt-4o-mini}
INPUT=${INPUT:-"What model are you ?"}
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
  "input": "${INPUT}"
}
EOF
)

export SERVICE MODEL INPUT PID TID CUSTOMER_ID DATA
exec "$(dirname "$0")/ai-post.sh"

