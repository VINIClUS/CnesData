# CnesData

Plataforma distribuida para ingestao, reconciliacao e persistencia de dados
publicos de saude municipal em um schema Gold multi-tenant.

O piloto atual e Presidente Epitacio/SP. A arquitetura ja isola tenants por
middleware, `ContextVar` e Row-Level Security no Postgres.

## Arquitetura Atual

O repositorio e um monorepo com pacotes compartilhados e apps implantaveis.

| Path | Tipo | Responsabilidade |
|---|---|---|
| `packages/cnes_contracts/` | Biblioteca | Contratos Pydantic, Protocols e JSON Schema |
| `packages/cnes_domain/` | Biblioteca | Ports, modelos, pipeline primitives, tenant context |
| `packages/cnes_infra/` | Biblioteca | Postgres, MinIO, ingestion clients, migrations, telemetry |
| `apps/dump_agent_go/` | Edge agent | Extrai CNES, SIHD, BPA e SIA no ambiente municipal |
| `apps/central_api/` | FastAPI | Orquestra jobs, dashboard API, device flow e provisionamento |
| `apps/data_processor/` | Worker | Consome `landing.extractions` e aplica rotas Gold v2 |
| `apps/cnes_db_migrator/` | Init container | Executa Alembic `upgrade head` |
| `apps/web_dashboard/` | SPA | Dashboard Bun, React, OIDC, ativacao de agentes e overview |

Fluxo operacional:

1. `dump_agent_go` descobre fontes locais, extrai dados e gera Parquet.
2. `central_api` cria ou registra jobs em `landing.extractions`.
3. Artefatos ficam no MinIO com hash SHA-256 registrado.
4. `data_processor` reclama jobs pendentes e marca o ciclo Gold v2.
5. O dashboard consome endpoints autenticados do `central_api`.
6. Regras de auditoria rodam fora deste repo, sobre o Gold.

## Fontes Ativas

| Fonte | Edge | Downstream |
|---|---|---|
| CNES local | Firebird municipal via `dump_agent_go` | Gold CNES |
| CNES nacional | BigQuery / DATASUS adapters | Gold CNES |
| SIHD | Base hospitalar local | Gold hospitalar |
| BPA-Mag | Firebird 1.5 `BPAMAG.GDB` | Producao ambulatorial |
| SIA | Arquivos DBF DATASUS | Producao ambulatorial e dimensoes |

## API

Com o `central_api` em execucao:

| Recurso | URL |
|---|---|
| Swagger UI | `http://localhost:8000/docs` |
| ReDoc | `http://localhost:8000/redoc` |
| OpenAPI runtime | `http://localhost:8000/openapi.json` |
| OpenAPI versionado | `docs/openapi.json` |
| OpenAPI para dashboard codegen | `docs/contracts/openapi.json` |

Regenerar o contrato versionado:

```powershell
.venv/Scripts/python.exe scripts/gen_openapi.py --output docs/openapi.json
.venv/Scripts/python.exe scripts/gen_openapi.py --output docs/contracts/openapi.json
```

## Desenvolvimento Local

Pre-requisitos:

- Python 3.13 e `uv`
- Docker e Docker Compose
- Go 1.26 para `apps/dump_agent_go`
- Bun 1.3 para `apps/web_dashboard`
- Git LFS para os fixtures Firebird

Instalar as dependencias e criar a configuracao local:

```powershell
uv sync
Copy-Item .env.example .env
```

Subir stack central, dashboard e Keycloak dev:

```powershell
docker compose --profile dev up -d
```

Servicos principais:

| Servico | Porta |
|---|---|
| Postgres | `5433` |
| MinIO API | `9000` |
| MinIO Console | `9001` |
| Central API | `8000` |
| Web dashboard | `5173` |
| Keycloak dev | `8080` |

Executar API local fora do compose:

```powershell
uv run uvicorn central_api.app:create_app --factory --reload
```

## Testes e Qualidade

Comandos rapidos sem Docker:

```powershell
.venv/Scripts/ruff.exe check .
.venv/Scripts/python.exe -m pytest -m "not integration and not postgres and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" -q
```

Go edge agent:

```powershell
cd apps/dump_agent_go
go test -race -count=1 -coverprofile=coverage.out ./...
```

Dashboard:

```powershell
cd apps/web_dashboard
bun install
bun run typecheck
bun run test
bun run build
```

## Documentacao

| Documento | Conteudo |
|---|---|
| `docs/architecture.md` | Arquitetura, fluxos, deploy e contratos |
| `docs/development.md` | Setup local, verificacoes e comandos equivalentes ao CI |
| `docs/roadmap.md` | Escopo ativo, proximo e removido |
| `docs/data-dictionary-cnes.md` | Schema canonico Gold CNES |
| `docs/data-dictionary-gold-v2.md` | Landing e Gold v2 |
| `docs/data-dictionary-bpa.md` | BPA-Mag |
| `docs/data-dictionary-sia.md` | SIA |
| `docs/data-dictionary-sihd-hospital.md` | SIHD |
| `docs/perf-testing.md` | Tiers de performance |
| `docs/runbooks/` | Runbooks operacionais |

## Variaveis Minimas

```ini
DB_URL=postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test
COD_MUN_IBGE=354130
ID_MUNICIPIO_IBGE7=3541308
CNPJ_MANTENEDORA=55293427000117
COMPETENCIA_ANO=2026
COMPETENCIA_MES=1
```

Credenciais de Firebird, MinIO, OIDC e certificados ficam nos apps ou no
ambiente de deploy. Nao hardcode caminhos ou segredos no codigo.
