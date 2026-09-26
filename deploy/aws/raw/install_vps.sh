#!/usr/bin/env bash
set -euo pipefail

environment="${1:?environment_required}"
case "$environment" in
  dev)
    stack=/opt/cnesdata-dev
    compose=docker-compose.dev.yml
    ;;
  prod)
    stack=/opt/cnesdata
    compose=docker-compose.prod.yml
    ;;
  *)
    echo "environment_invalid" >&2
    exit 1
    ;;
esac

staging="${2:?staging_directory_required}"
test -f "$staging/$compose"
test -f "$staging/$environment.env.raw"
test -f "$stack/.env"
test -f "$stack/.env.image"
backup="/opt/cnesdata-config-backups/$environment/$(date -u +%Y%m%dT%H%M%SZ)"
install -d -m 700 "$backup"
cp -p "$stack/.env" "$backup/.env"
cp -p "$stack/.env.image" "$backup/.env.image"
cp -p "$stack/$compose" "$backup/$compose"

rollback() {
  cp -p "$backup/.env" "$stack/.env"
  cp -p "$backup/$compose" "$stack/$compose"
  echo "config_restored backup=$backup" >&2
}
trap rollback ERR

owner="$(stat -c '%u:%g' "$stack/.env")"
tmp="$(mktemp "$stack/.env.raw.XXXXXX")"
grep -v '^RAW_' "$stack/.env" > "$tmp"
cat "$staging/$environment.env.raw" >> "$tmp"
chmod 600 "$tmp"
chown "$owner" "$tmp"
mv "$tmp" "$stack/.env"
install -m 644 "$staging/$compose" "$stack/$compose"
chown "$owner" "$stack/$compose"

cd "$stack"
docker compose --env-file .env --env-file .env.image -f "$compose" config --quiet
trap - ERR
echo "config_installed environment=$environment backup=$backup"
