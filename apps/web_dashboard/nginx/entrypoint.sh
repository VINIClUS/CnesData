#!/bin/sh
set -e

: "${UPSTREAM_API:=central-api:8000}"
: "${OIDC_AUTHORITY:=}"

envsubst '${UPSTREAM_API} ${OIDC_AUTHORITY}' \
  < /etc/nginx/templates/default.conf.template \
  > /etc/nginx/conf.d/default.conf

envsubst '${OIDC_AUTHORITY}' \
  < /etc/nginx/templates/security-headers.conf \
  > /etc/nginx/conf.d/security-headers.conf
