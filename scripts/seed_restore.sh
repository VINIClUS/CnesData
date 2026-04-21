#!/usr/bin/env bash
# Executa SQL seed dentro do container firebird-shadow do docker-compose.shadow.yml.
# Uso: scripts/seed_restore.sh [seed_dir]
#
# Espera container nomeado <project>-firebird-shadow-1. COMPOSE_PROJECT env var
# override o prefixo; default 'cnesdata'.

set -euo pipefail

SEED_DIR="${1:-docs/fixtures/shadow-seed}"
PROJECT="${COMPOSE_PROJECT:-cnesdata}"
CONTAINER="${PROJECT}-firebird-shadow-1"
DB_PATH="/firebird/data/shadow.fdb"

if [ ! -d "$SEED_DIR" ]; then
    echo "seed_dir_not_found=$SEED_DIR" >&2
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "container_not_running=$CONTAINER" >&2
    echo "  run 'docker compose -f docker-compose.shadow.yml up -d' first" >&2
    exit 1
fi

# Descobre path do isql (jacobalberty image usa /usr/local/firebird/bin/isql;
# outras imagens podem ter isql-fb no PATH)
ISQL=$(docker exec "$CONTAINER" bash -c \
    "command -v isql || command -v isql-fb || ls /usr/local/firebird/bin/isql 2>/dev/null" \
    | tr -d '\r' | head -n1)
if [ -z "$ISQL" ]; then
    echo "isql_binary_not_found_in_container" >&2
    exit 1
fi
echo "isql_path=$ISQL"

# Aguarda FB estar pronto
echo "waiting_for_firebird..."
ready=0
for i in $(seq 1 60); do
    if docker exec "$CONTAINER" bash -c \
        "echo 'SELECT 1 FROM RDB\$DATABASE;' | $ISQL ${DB_PATH} -u SYSDBA -p masterkey" \
        >/dev/null 2>&1; then
        echo "firebird_ready_after=${i}s"
        ready=1
        break
    fi
    sleep 1
done
if [ "$ready" -ne 1 ]; then
    echo "firebird_not_ready_after_60s" >&2
    docker logs "$CONTAINER" | tail -30 >&2
    exit 1
fi

# Executa cada .sql em seed_dir na ordem alfabética
shopt -s nullglob
for sql in "$SEED_DIR"/*.sql; do
    base=$(basename "$sql")
    echo "executing_sql=$base"
    # Copia arquivo pro container temp + executa
    docker cp "$sql" "$CONTAINER:/tmp/$base"
    docker exec "$CONTAINER" bash -c \
        "$ISQL ${DB_PATH} -u SYSDBA -p masterkey -i /tmp/$base"
    echo "executed=$base"
done

echo "seed_restore_complete"
