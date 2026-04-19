# cnes_domain — Domain core (pure, zero-infra)

## Executive Summary

Biblioteca de tipos, protocolos e primitivas do domínio. **Zero dependências
de infra** (nada de SQLAlchemy, MinIO, httpx, fdb). Define o contrato entre
os 4 apps via PEP 544 Protocols e modelos Pydantic. É consumida por
`cnes_infra` (implementações concretas) e direta ou indiretamente por todos
os apps deployáveis.

## Scope

- **Ports (Protocols):** `ObjectStoragePort`, `UnitOfWork`,
  `EstabelecimentoRepository`, `ProfissionalRepository`, `VinculoRepository`
- **Models:** `ExtractionIntent`, `ExtractionParams`, request/response de API
- **Contracts:** sets de colunas canônicas (CNES + SIHD)
- **Pipeline primitives:** `CircuitBreaker` (sync + async, thread-safe)
- **Processing:** `transformer.transformar()` (pipeline CPF),
  `row_mapper` (raw→dims/fato)
- **Tenant context:** `set_tenant_id()` / `get_tenant_id()` via ContextVar
- **Competência:** parsing YYYY-MM + validação
- **Observability:** `tracer` OTel (no-op se SDK não instalado)

## Conventions

- **Nada aqui pode importar de `cnes_infra`, `sqlalchemy`, `httpx`, `minio`, `fdb`**
- Ports são Protocols (PEP 544), não ABCs — `@runtime_checkable` em
  `object_storage.py` e `repository.py` para suporte a `isinstance`
- Funções de processing recebem/retornam `polars.DataFrame`
- 100% branch coverage é gate de CI para este pacote
- `_LAZY_ATTRS` em `cnes_infra.config` não tem equivalente aqui — domínio
  não lê env vars diretamente (receber via parâmetro)

## Module Map

| Path | Responsabilidade |
|---|---|
| `contracts/columns.py` | Colunas canônicas CNES (set + listas) |
| `contracts/sihd_columns.py` | Colunas canônicas SIHD / AIH |
| `contracts/schemas.py` | PEP 544 Protocols para DataFrames canônicos |
| `models/api.py` | Pydantic: `JobCreate`, `JobStatus`, `LeaseResponse` etc. |
| `models/extraction.py` | `ExtractionIntent` enum + `ExtractionParams` (regex CNES/IBGE) |
| `pipeline/circuit_breaker.py` | Thread-safe CB com `call()` + `call_async()` |
| `ports/object_storage.py` | `ObjectStoragePort` Protocol + `NullObjectStoragePort` |
| `ports/repository.py` | 3 Repository Protocols runtime_checkable |
| `ports/storage.py` | `UnitOfWork` + `NullUnitOfWork` + `NullStorage` |
| `processing/transformer.py` | Pipeline CPF (strip_chars → replace_all \D → pad_start → RQ-002), flag CH zero |
| `processing/row_mapper.py` | DataFrame → row dicts (`mapear_estabelecimentos`, `mapear_profissionais`, `mapear_vinculos`, `extrair_fonte`) |
| `competencia.py` | YYYY-MM parsing + range |
| `tenant.py` | ContextVar + `set_tenant_id` / `get_tenant_id` |
| `observability.py` | `tracer` no-op ou wrapper OTel |
| `config.py` | `validar_formato` (regex) + `_exigir` / `_exigir_inteiro` (usados por `cnes_infra`) |

## Gotchas

- **CircuitBreaker lock não cruza await:** usa `threading.Lock`. Seção
  crítica pequena e não contém `await` — seguro em async.
- **Backoff expoente capado em `_MAX_BACKOFF_EXPONENTE=30`:** evita
  `OverflowError` quando `_falhas_consecutivas > 1023` (Python `2**N` para
  float). Sem isso, o breaker quebra em cenários de contenção prolongada.
- **Transformer CPF ordem:** `strip_chars → replace_all(r"\D", "") →
  pad_start(11, "0") → _aplicar_rq002`. Pontuação é removida ANTES de pad.
  Entrada `".-./"` vira `""` (filtrada por RQ-002 via sentinela).
- **Tenant ContextVar:** herdada por tasks async via
  `contextvars.copy_context()`. Se criar threads manuais, repassar o contexto
  explicitamente.
- **Protocol + `@runtime_checkable`:** só adicionar a decorator quando
  precisa de `isinstance()` check. Custo em performance é desprezível mas
  expõe atributos não-públicos em `dir()`.
