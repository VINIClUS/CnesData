# data_processor — Transform & persist worker

## Executive Summary

Worker assíncrono Gold v2 que consome `landing.extractions` diretamente no
Postgres, define o tenant do job reclamado e marca conclusão/falha. Mantém
rotas auxiliares para validar SHA-256, ler Parquet delta (`_op`) e delegar
I/U para callbacks/upserts específicos quando uma ingestão usa esse caminho.

## Role

**Central worker**. Stateless entre jobs; estado inteiramente em Postgres.
Horizontalmente escalável — múltiplas réplicas puxam da mesma fila sem
colisão (lease-based).

## Functionalities

- Claim global de `landing.extractions` via `extractions_repo.claim_next`
- `set_tenant_id(claimed.tenant_id)` antes de mutar estado do job
- `mark_completed` / `mark_failed` no mesmo storage repository
- `integrity_check.verify_parquet` para SHA-256 quando esperado
- `cdc_merger.merge_delta` para linhas `_op ∈ {I,U,D}`
- Adapters CNES/SIHD/BPA/SIA preservados para rotas de ingestão específicas

## Objectives

- Claim idempotente e seguro entre réplicas horizontais
- Zero cross-tenant leak em marcação de jobs
- Integridade verificável quando `sha256` vem do edge agent

## Limitations

- **Não faz extract** — só consome metadata/artefatos já produzidos
- **Não aplica regras de auditoria** — persiste dados canônicos; regras
  rodam em serviço externo que consome Gold via SQL JOINs
- **Não tem UI** — é daemon puro, monitorado via logs + OTel
- **Não conecta ao Firebird** — totalmente desacoplado das fontes
- **Não orquestra jobs** — só executa; `central_api` decide prioridade/fila

## Requirements

**Runtime deps (apps/data_processor/pyproject.toml):** `sqlalchemy`, `psycopg`,
`polars`, `httpx`, `boto3` (via `cnes_infra`), `cnes_domain`, `cnes_infra`.

**Env vars:**

| Var | Obrigatória | Descrição |
|---|---|---|
| `DB_URL` | sim | Postgres Gold (mesmo cluster do `central_api`) |
| `CENTRAL_API_URL` | sim | Para polling da fila |
| `AWS_ACCESS_KEY_ID` | sim (endpoint não-AWS) | Cadeia padrão do boto3; obrigatório se `S3_ENDPOINT_URL` setado |
| `AWS_SECRET_ACCESS_KEY` | sim (endpoint não-AWS) | Idem |
| `S3_ENDPOINT_URL` | opcional | Vazio = S3 real; `http://minio:9000` em dev, LocalStack em CI |
| `S3_PUBLIC_ENDPOINT_URL` | opcional | Host usado nas URLs presigned (B4); default = `S3_ENDPOINT_URL` |
| `S3_REGION` | opcional | Default `sa-east-1` |
| `S3_BUCKET` | opcional | Default `cnesdata-landing` |
| `WORKER_POLL_INTERVAL` | opcional | Default `5s` |

## Module Map

| Arquivo | Responsabilidade |
|---|---|
| `src/data_processor/main.py` | Entrypoint async + `_setup_logging` + `_create_storage` + run_processor |
| `src/data_processor/consumer.py` | Compat wrapper para `poll.loop` |
| `src/data_processor/poll.py` | Claim `landing.extractions` + mark completed/failed |
| `src/data_processor/processor.py` | SHA-256 + delta route helpers |
| `src/data_processor/config.py` | Config do worker (bucket, intervalos) |
| `src/data_processor/adapters/cnes_local_adapter.py` | Parquet CNES raw → DataFrame canônico |
| `src/data_processor/adapters/cnes_nacional_adapter.py` | Parquet BigQuery nacional → canônico |
| `src/data_processor/adapters/sihd_local_adapter.py` | Parquet SIHD/AIH → canônico |
| `src/data_processor/adapters/bpa_adapter.py` | Transformações Polars puras do raw `S_PRD` (sem lookup SQL) |
| `src/data_processor/adapters/sia_adapter.py` | `canonicalize_apa` / `canonicalize_bpi` + `map_*_to_fato` legado |
| `src/data_processor/adapters/sia_dim_sync.py` | `build_reference_sigtap` / `build_reference_municipio` (Polars puro, sem SQL) |
| `src/data_processor/sources/sia/` | `normalize_sia` / `reconcile_sia` / `materialize_sia` — plugin Parquet SIA_LOCAL |
| `src/data_processor/cdc_merger.py` | `merge_delta` — roteia `_op ∈ {I,U,D}` |
| `src/data_processor/integrity_check.py` | `verify_parquet` — SHA-256 sobre Parquet baixado |
| `src/data_processor/pipeline/normalize_cnes_local.py` | `normalize_cnes_local` — reconstrói FULL+DELTA |
| `src/data_processor/pipeline/normalize_cnes_nacional.py` | `normalize_cnes_nacional` — raw FULL único |
| `src/data_processor/pipeline/reconcile_cnes.py` | `reconcile_cnes` — precedência LOCAL/NACIONAL |
| `src/data_processor/pipeline/materialize_cnes.py` | `materialize_cnes` — serving JSON agregado |
| `src/data_processor/pipeline/delta_reconstruction.py` | `reconstruct_from_deltas` — CDC por natural key |
| `src/data_processor/sources/bpa/` | Plugin BPA: `normalize_bpa`, `reconcile_bpa`, `materialize_bpa` |
| `src/data_processor/sources/sihd/contract.py` | Layout, colunas, domínios, deny-list PII |
| `src/data_processor/sources/sihd/normalize.py` | `normalize_sihd` (FULL+DELTA) |
| `src/data_processor/sources/sihd/reconcile.py` | `reconcile_sihd` (join por AIH) |
| `src/data_processor/sources/sihd/serving.py` | `materialize_sihd` (sem PII) |

## Gotchas

- **`fontes` JSONB merge + `vinculo_repo` upsert:** ver
  `packages/cnes_infra/CLAUDE.md` (Gotchas) — regras únicas, não duplicar aqui.
- **CircuitBreaker é sync + async, APIs separadas:** use `.call()` para
  função síncrona, `.call_async()` para coroutine. Misturar dispara
  `TypeError` explícito (evita falha silenciosa que motivou o fix original).
- **Column names do BigQuery nacional** (confirmados empiricamente):
  `cbo_2002` (não `id_cbo`), `indicador_atende_sus` inteiro 1/0 (não
  `indicador_sus` string "S"/"N"). Ver `docs/data-dictionary-firebird-bigquery.md`.
- **Worker é global (multi-tenant):** poll varre `landing.extractions`
  de todos os tenants via `SET LOCAL row_security = off` scoped à
  transação do `claim_next`. A cada job reclamado, `process_one` chama
  `set_tenant_id(claimed.tenant_id)` antes de qualquer
  `mark_completed`/`mark_failed`/escrita Gold subsequente. Sem env
  `TENANT_ID`; o tenant vem do row reclamado.
- **Streaming download gzip:** parquet baixado chunk a chunk via httpx
  stream para evitar OOM em arquivos grandes. Marcado `# pragma: no cover`
  nos fallbacks de tempfile.
- **Delta é o único formato de entrada** (sem flag, sem snapshot legado):
  `cdc_merger.merge_delta` roteia por `_op ∈ {I,U,D}`; D vira `DELETE` inline
  por template PK `(source, intent)` alinhado com `delta/profiles.go` do
  edge agent; I/U passam por `apply_iu_fn` (upsert existente). `_op` ausente
  → `ValueError("missing_op_column")`.
- **`verify_and_route_delta`** encadeia `verify_parquet` (SHA-256, pula se
  `expected_sha256 None`) → `pl.read_parquet` → `route_delta`; mismatch
  propaga `IntegrityError` e falha o job. `landing.extractions.sha256` é
  nullable (Alembic 018).
- **BPA raw é `S_PRD`, não `BPA_*_LINHAS`:** o GDB real tem uma tabela única
  separada pelo Edge em `PRD_ORG='BPI'`→BPA_I e resto→BPA_C (origem ≠ `BPA`
  vira `origem_divergente`); introspecção em
  `apps/data_processor/tests/fixtures/bpa/fixture-manifest.json`. PII de
  paciente é descartada em `prepare_raw`; `prd_cnsmed` só vira o booleano
  `tem_cns_profissional` — nenhum CNS/CPF chega ao normalizado ou ao serving.
- **SIHD PII sai no normalize:** `_canonicalize` projeta só as colunas da
  allow-list (`*_SOURCE_SCHEMA` em `sources/sihd/contract.py`); o
  `PII_DENY_LIST` é rechecado em `materialize_sihd` (`pii_field_in_serving`).
- **Colunas SIHD homônimas enganam (#295):** no SIHD2 real `AH_DIAG_SEC` é legado
  (`'0000'`) e `AH_PACIENTE_MUN_ORIGEM` é sempre NULL; `DIAG_SEC` vem de
  `AH_DIAG_SEC_1` e `PACIENTE_MUN_ORIGEM` de `AH_PACIENTE_LOGR_MUNICIPIO`. `AH_IDENT`
  tem zero à esquerda (`'01'`). Ver `docs/data-dictionary-sihd-hospital.md`.
- **Identidade da AIH é `(COMPETENCIA, OE_GESTOR, SEQ = SEQ_PRINC)`, não
  `NUM_AIH`:** é a chave CDC da internação e o join internação × procedimento
  em `reconcile_sihd`. A chave CDC de procedimento acrescenta `INDX` (sem ele,
  procedimentos da mesma AIH colapsam). `NUM_AIH` só compõe o `SIHD_KEY`.
- **Delta SIHD com `_op` fora de I/U/D é rejeitado:** `invalid_cdc_op op=...`
  antes da reconstrução (o FULL base não é checado).
- **SIA raw usa os nomes reais dos DBFs SIASUS** (`prd_*`/`apa_*`, `bpi_*`,
  datas `AAAAMMDD` em texto); `quantidade` = `*_qt_p`, valor APAC = `prd_vl_a`.
  O Parquet do Edge em `tests/fixtures/sia/edge_golden/` é o contrato
  verificado pelos dois lados; ver `docs/data-dictionary-sia.md`.

Histórico de fases (T12/T13, P2, P3): `CHANGELOG.md`.
