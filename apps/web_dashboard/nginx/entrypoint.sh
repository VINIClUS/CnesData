#!/bin/sh
set -e

: "${UPSTREAM_API:=central-api:8000}"
: "${OIDC_AUTHORITY:=}"
: "${API_ORIGIN:=}"
# Pre-launch switch. "true" keeps X-Robots-Tag: noindex on /precos; any other
# value removes the header so the page can be indexed at public launch.
# Must not use ${VAR:=default}: an explicitly empty value would be overwritten.
: "${PRECOS_NOINDEX:=true}"

if [ "$PRECOS_NOINDEX" = "true" ]; then
  ROBOTS_PRECOS="noindex"
else
  ROBOTS_PRECOS=""
fi
export ROBOTS_PRECOS

envsubst '${UPSTREAM_API} ${OIDC_AUTHORITY} ${ROBOTS_PRECOS}' \
  < /etc/nginx/templates/default.conf.template \
  > /etc/nginx/conf.d/default.conf

envsubst '${OIDC_AUTHORITY} ${API_ORIGIN}' \
  < /etc/nginx/templates/security-headers.conf \
  > /etc/nginx/conf.d/security-headers.conf
