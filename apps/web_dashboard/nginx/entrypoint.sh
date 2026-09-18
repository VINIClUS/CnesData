#!/bin/sh
set -e

: "${UPSTREAM_API:=central-api:8000}"
: "${OIDC_AUTHORITY:=}"
: "${API_ORIGIN:=}"
: "${ROBOTS_PRECOS:=noindex}"

envsubst '${UPSTREAM_API} ${OIDC_AUTHORITY} ${ROBOTS_PRECOS}' \
  < /etc/nginx/templates/default.conf.template \
  > /etc/nginx/conf.d/default.conf

envsubst '${OIDC_AUTHORITY} ${API_ORIGIN}' \
  < /etc/nginx/templates/security-headers.conf \
  > /etc/nginx/conf.d/security-headers.conf
