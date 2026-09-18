#!/usr/bin/env bash
# One-time, idempotent provisioning of GHCR-based deploy for the production
# stack (/opt/cnesdata), which already exists and runs live traffic — unlike
# deploy/dev/bootstrap.sh this script never touches .env, secrets/, or
# keycloak/realm.json. Run as root on the target host:
#
#   CI_PUBLIC_KEY="ssh-ed25519 AAAA... cnesdata-prod-ci" bash bootstrap.sh
#
# Safe to re-run: every step checks current state before mutating it.
set -euo pipefail

: "${CI_PUBLIC_KEY:?set CI_PUBLIC_KEY to the deploy CI public SSH key}"

STACK_DIR=/opt/cnesdata
DEPLOY_USER=cnesdeployprod
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> stack directory exists"
if [ ! -d "$STACK_DIR" ]; then
  echo "    FATAL: $STACK_DIR not found — this script assumes the stack" >&2
  echo "    already exists (docker-compose.prod.yml, .env, secrets/ca.*)." >&2
  exit 1
fi

echo "==> deploy user"
if ! id "$DEPLOY_USER" &>/dev/null; then
  useradd -m -s /bin/bash -G docker "$DEPLOY_USER"
fi

echo "==> forced-command SSH key"
install -d -m 700 -o "$DEPLOY_USER" -g "$DEPLOY_USER" "/home/$DEPLOY_USER/.ssh"
AUTH_KEYS="/home/$DEPLOY_USER/.ssh/authorized_keys"
RESTRICTED_KEY="command=\"/usr/local/bin/cnesdata-prod-deploy\",no-port-forwarding,no-agent-forwarding,no-X11-forwarding,no-pty $CI_PUBLIC_KEY"
touch "$AUTH_KEYS"
grep -qF "$CI_PUBLIC_KEY" "$AUTH_KEYS" || echo "$RESTRICTED_KEY" >> "$AUTH_KEYS"
chmod 600 "$AUTH_KEYS"
chown "$DEPLOY_USER:$DEPLOY_USER" "$AUTH_KEYS"

echo "==> deploy.sh"
install -m 755 -o root -g root "$SCRIPT_DIR/deploy.sh" /usr/local/bin/cnesdata-prod-deploy
touch /var/log/cnesdata-prod-deploy.log
chown "$DEPLOY_USER:$DEPLOY_USER" /var/log/cnesdata-prod-deploy.log

echo "==> compose file ownership (needed for \$DEPLOY_USER to write .env.image)"
chown "$DEPLOY_USER:$DEPLOY_USER" "$STACK_DIR"
chown "$DEPLOY_USER:$DEPLOY_USER" "$STACK_DIR/.env" "$STACK_DIR/secrets" 2>/dev/null || true

echo "==> shared edge network"
docker network inspect cnesdata_edge &>/dev/null || docker network create cnesdata_edge

echo "==> NOTE: docker-compose.prod.yml and caddy/Caddyfile are NOT written by"
echo "    this script. The one-time migration from build-on-box to GHCR pull"
echo "    (deploy/prod/docker-compose.prod.yml here) is a separate, manual,"
echo "    supervised step — see docs/runbooks/deploy-main.md."

echo "==> done. First deploy: ssh -i <ci-key> $DEPLOY_USER@<host> main-<sha>"
