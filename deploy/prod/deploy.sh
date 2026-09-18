#!/usr/bin/env bash
# Installed on the VPS as /usr/local/bin/cnesdata-prod-deploy.
# Invoked only via the cnesdeployprod SSH forced-command
# (see deploy/prod/bootstrap.sh) — the image tag is the SSH command itself,
# never a free-form shell.
set -euo pipefail

STACK_DIR=/opt/cnesdata
COMPOSE="docker compose --env-file .env --env-file .env.image -f docker-compose.prod.yml"
LOG=/var/log/cnesdata-prod-deploy.log

log() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$LOG"
}

TAG="${SSH_ORIGINAL_COMMAND:-}"
if ! [[ "$TAG" =~ ^main-[0-9a-f]{7,40}$ ]]; then
  log "rejected tag='$TAG' (must match main-<git-sha>)"
  exit 1
fi

cd "$STACK_DIR"
log "deploy start tag=$TAG"

PREV_TAG=""
if [ -f .env.image ]; then
  PREV_TAG="$(grep -oP '(?<=^IMAGE_TAG=).*' .env.image || true)"
fi

rollback() {
  log "deploy failed tag=$TAG, rolling back to prev=$PREV_TAG"
  if [ -n "$PREV_TAG" ]; then
    echo "IMAGE_TAG=$PREV_TAG" > .env.image
    $COMPOSE pull >>"$LOG" 2>&1 || true
    $COMPOSE up -d --remove-orphans >>"$LOG" 2>&1 || true
  fi
  exit 1
}
trap rollback ERR

echo "IMAGE_TAG=$TAG" > .env.image

$COMPOSE pull >>"$LOG" 2>&1
$COMPOSE run --rm migrator >>"$LOG" 2>&1
$COMPOSE up -d --remove-orphans >>"$LOG" 2>&1

log "waiting for central-api health"
deadline=$((SECONDS + 120))
until [ "$($COMPOSE ps -q central-api | xargs -r docker inspect -f '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    log "central-api did not become healthy within 120s"
    exit 1
  fi
  sleep 3
done

trap - ERR
log "deploy ok tag=$TAG"

docker image prune -f --filter "until=168h" >>"$LOG" 2>&1 || true
