#!/usr/bin/env bash
set -euo pipefail

URL="${1:?usage: smoke.sh <dashboard-base-url> [api-base-url]}"
API="${2:-}"

echo "1. Health check"
curl -fsS "$URL/healthz" | grep -q "ok"

echo "2. Static SPA loads"
curl -fsS -H "Accept: text/html" "$URL/" | grep -q '<div id="root">'

echo "3. Unauthenticated /api/v1/dashboard/auth/me returns 401 (same-origin proxy)"
[ "$(curl -s -o /dev/null -w "%{http_code}" "$URL/api/v1/dashboard/auth/me")" = "401" ]

echo "4. CSP header present"
curl -sI "$URL/" | grep -qi "content-security-policy"

echo "5. CSS chunk has long-cache"
ASSET=$(curl -fsS "$URL/" | grep -oE '/assets/[^"]+\.css' | head -1)
curl -sI "$URL$ASSET" | grep -qi "max-age=31536000"

if [ -n "$API" ]; then
  echo "6. API host health"
  curl -fsS "$API/api/v1/system/health" | grep -q '"status"'

  echo "7. CSP connect-src allows the API host"
  curl -sI "$URL/" | grep -i "content-security-policy" | grep -q "$API"

  echo "8. CORS preflight from dashboard origin is accepted"
  curl -sS -o /dev/null -D - -X OPTIONS "$API/api/v1/public/leads" \
    -H "Origin: $URL" -H "Access-Control-Request-Method: POST" \
    -H "Access-Control-Request-Headers: content-type" \
    | grep -qi "access-control-allow-origin: $URL"

  echo "9. CORS preflight from unknown origin is rejected"
  ! curl -sS -o /dev/null -D - -X OPTIONS "$API/api/v1/public/leads" \
    -H "Origin: https://unknown.example" -H "Access-Control-Request-Method: POST" \
    | grep -qi "access-control-allow-origin"

  echo "10. API host does not expose docs"
  [ "$(curl -s -o /dev/null -w "%{http_code}" "$API/docs")" = "404" ]
fi

echo "smoke OK"
