# cnes_db_migrator — Alembic init-container

## Executive Summary

Runner mínimo que invoca `alembic upgrade head` antes de `central_api` e
`data_processor` iniciarem. Desenhado como initContainer Kubernetes — roda
uma vez por deploy, sai com código 0 em sucesso ou 1 em falha de migration.
Zero estado próprio; toda configuração vem de env vars.

## Role

Boot-time DB migrator. Sem loop, sem persistência. Artefato único:
container image minimalista.

## Functionalities

- `alembic -c alembic.ini upgrade head` contra `DB_URL` do env
- Retry em falha de conexão transitória (até 30s) — espera Postgres subir
- Exit 0 em sucesso, exit 1 em falha de migration

## Objectives

- Execução < 10s em deploy steady-state (migrations já aplicadas → no-op)
- Zero race entre réplicas (Alembic file-level lock garante)
- Imagem docker < 150MB (só Python + Alembic + deps + `cnes_infra`)

## Limitations

- **Não aplica data migrations complexas** — só schema DDL
- **Não roda em prod contra DB de produção sem backup recente** (responsabilidade do operador)
- **Não gera migrations** — só aplica existentes. `alembic revision --autogenerate` roda em dev local

## Requirements

**Runtime deps (apps/cnes_db_migrator/pyproject.toml):** `alembic`,
`sqlalchemy`, `psycopg`, `cnes_infra` (por causa de `env.py` importar
`cnes_infra.storage.schema`).

**Env vars:**

| Var | Obrigatória | Descrição |
|---|---|---|
| `DB_URL` | sim | Postgres URL (target do upgrade) |

## Module Map

| Arquivo | Responsabilidade |
|---|---|
| `src/cnes_db_migrator/run.py` | CLI simples: `python -m cnes_db_migrator` invoca `command.upgrade(cfg, "head")` |
| `packages/cnes_infra/alembic.ini` | Config Alembic (`script_location = src/cnes_infra/alembic`) |
| `packages/cnes_infra/src/cnes_infra/alembic/env.py` | Env script com `_resolver_db_url` |
| `packages/cnes_infra/src/cnes_infra/alembic/versions/*.py` | Migrations numeradas (omitidas em coverage) |

## Gotchas

- **`env.py._resolver_db_url`:** prefere `sqlalchemy.url` do Config (setado
  por fixtures de teste via `cfg.set_main_option`) e cai em `DB_URL` env
  var senão. Isso destrava testes sem precisar setar env global.
- **Migration paths:** `script_location = src/cnes_infra/alembic` (relativo
  a `packages/cnes_infra/` onde fica o `alembic.ini`). CLI precisa rodar
  a partir de `packages/cnes_infra/` OU passar `-c packages/cnes_infra/alembic.ini`.
- **Init-container em k8s:** configurar `restartPolicy: OnFailure` e
  `backoffLimit: 3`. Timeout de 60s recomendado (primeiro boot após criar
  DB pode levar até 20s com 6 migrations).
