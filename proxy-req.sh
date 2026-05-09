#!/bin/bash
set -euo pipefail

URL="${1:-}"
shift || true

if [[ -z "$URL" ]]; then
  >&2 echo "Usage: $(basename "$0") <http://url> [body...]"
  >&2 echo ""
  >&2 echo "Env:"
  >&2 echo "  PROXY / HTTP_PROXY / http_proxy  HTTP proxy URL, e.g. http://127.0.0.1:8080"
  >&2 echo "  METHOD                         HTTP method (default: POST)"
  >&2 echo "  CONTENT_TYPE                   Content-Type header (default: application/json)"
  exit 2
fi

# This helper is intentionally HTTP-only to mimic a browser configured with an HTTP proxy
# while testing plain HTTP browsing/requests.
if [[ "$URL" != http://* ]]; then
  >&2 echo "ERROR: Only http:// URLs are supported: '$URL'"
  exit 2
fi

PROXY="${PROXY:-${HTTP_PROXY:-${http_proxy:-}}}"
if [[ -z "$PROXY" ]]; then
  >&2 echo "ERROR: Proxy is not set. Provide PROXY or HTTP_PROXY (http://... only)."
  exit 2
fi
if [[ "$PROXY" != http://* ]]; then
  >&2 echo "ERROR: Proxy must be an HTTP proxy URL (http://...): '$PROXY'"
  exit 2
fi

METHOD="${METHOD:-GET}"
CONTENT_TYPE="${CONTENT_TYPE:-application/json}"
BODY="$*"

curl -S -s -v -D /dev/stderr \
  --proto '=http' --proto-redir '=http' \
  --proxy "$PROXY" \
  -X "$METHOD" \
  --data-binary "$BODY" \
  -H "Content-Type: $CONTENT_TYPE" \
  "$URL"
