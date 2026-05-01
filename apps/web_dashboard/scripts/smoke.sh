#!/usr/bin/env bash
set -euo pipefail

URL="${1:?usage: smoke.sh <base-url>}"

echo "1. Health check"
curl -fsS "$URL/healthz" | grep -q "ok"

echo "2. Static SPA loads"
curl -fsS -H "Accept: text/html" "$URL/" | grep -q '<div id="root">'

echo "3. Unauthenticated /api/v1/dashboard/auth/me returns 401"
[ "$(curl -s -o /dev/null -w "%{http_code}" "$URL/api/v1/dashboard/auth/me")" = "401" ]

echo "4. CSP header present"
curl -sI "$URL/" | grep -qi "content-security-policy"

echo "5. CSS chunk has long-cache"
ASSET=$(curl -fsS "$URL/" | grep -oE '/assets/[^"]+\.css' | head -1)
curl -sI "$URL$ASSET" | grep -qi "max-age=31536000"

echo "smoke OK"
