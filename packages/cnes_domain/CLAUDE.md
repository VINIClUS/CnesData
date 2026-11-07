# cnes_domain — Domain core (pure, zero-infra)

## Executive Summary

Biblioteca de tipos, protocolos e primitivas do domínio. **Zero dependências
de infra** (nada de SQLAlchemy, MinIO, httpx, fdb). Define regras e ports usados pelos
apps via PEP 544 Protocols e modelos Pydantic. É consumida por
adapters das capabilities (implementações concretas) e direta ou indiretamente por todos
os apps deployáveis.

## Scope

- **Ports (Protocols):** `ObjectStoragePort`, `UnitOfWork`,
  `EstabelecimentoRepository`, `ProfissionalRepository`, `VinculoRepository`
- **DTOs externos:** ver `cnes_contracts.api` e `cnes_contracts.extraction`
- **Contracts:** sets de colunas canônicas (CNES + SIHD)
- **Pipeline primitives:** `CircuitBreaker` (sync + async, thread-safe)
- **Processing:** `transformer.transformar()` (pipeline CPF),
  `row_mapper` (raw→dims/fato)
- **Tenant context:** `set_tenant_id()` / `get_tenant_id()` via ContextVar
- **Competência:** parsing YYYY-MM + validação

## Conventions

- **Nada aqui pode importar de adapters das capabilities, `sqlalchemy`, `httpx`, `minio`, `fdb`**
- Ports são Protocols (PEP 544), não ABCs — `@runtime_checkable` em
  `object_storage.py` e `repository.py` para suporte a `isinstance`
- Funções de processing recebem/retornam `polars.DataFrame`
- 100% branch coverage é gate de CI para este pacote
- `_LAZY_ATTRS` em `cnes_runtime.config` não tem equivalente aqui — domínio
  não lê env vars diretamente (receber via parâmetro)

## Module Map

| Path | Responsabilidade |
|---|---|
| `cnes_contracts.columns` | Colunas canônicas CNES (set + listas) |
| `cnes_contracts.sihd_columns` | Colunas canônicas SIHD / AIH |
| `orchestration/source_definitions/sihd.py` | Pipeline SIHD: `SIHD_DEFINITION`, `SIHD_LAYOUT` |
| `validation/dataframes.py` | PEP 544 Protocols para DataFrames canônicos |
| `cnes_contracts.api` | Pydantic: `JobCreate`, `JobStatus`, `LeaseResponse` etc. |
| `cnes_contracts.extraction` | `ExtractionIntent` enum + `ExtractionParams` (regex CNES/IBGE) |
| `pipeline/circuit_breaker.py` | Thread-safe CB com `call()` + `call_async()` |
| `ports/object_storage.py` | `ObjectStoragePort` Protocol + `NullObjectStoragePort` |
| `ports/repository.py` | 3 Repository Protocols runtime_checkable |
| `ports/storage.py` | `UnitOfWork` + `NullUnitOfWork` + `NullStorage` |
| `processing/transformer.py` | Pipeline CPF (strip_chars → replace_all \D → pad_start → RQ-002), flag CH zero |
| `processing/row_mapper.py` | DataFrame → row dicts (`mapear_estabelecimentos`, `mapear_profissionais`, `mapear_vinculos`, `extrair_fonte`) |
| `competencia.py` | YYYY-MM parsing + range |
| `tenant.py` | ContextVar + `set_tenant_id` / `get_tenant_id` |
| `config.py` | `validar_formato` (regex) + `_exigir` / `_exigir_inteiro` (usados por adapters das capabilities) |
| `billing/models.py`, `commands.py`, `execution.py` | Domínio de billing imutável (snapshot, quotas, companion `RunBillingState`) |
| `billing/policy.py`, `gate.py` | `EntitlementPolicy(mode)` + `EntitlementGate` (leitura forte, nega com `EntitlementDenied`) |
| `billing/shadow.py` | `ShadowEntitlementObserver`: negação hipotética de shadow vira audit; nunca levanta |
| `billing/execution_policy.py` | `BillingConcurrencyPolicy`, `BillingExecutionStarted`, `apply_execution_binding` (CAS do companion) |

## Gotchas

- **CircuitBreaker lock não cruza await:** usa `threading.Lock`. Seção
  crítica pequena e não contém `await` — seguro em async.
- **Backoff expoente capado em `_MAX_BACKOFF_EXPONENTE=30`:** evita
  `OverflowError` quando `_falhas_consecutivas > 1023` (Python `2**N` para
  float). Sem isso, o breaker quebra em cenários de contenção prolongada.
- **Transformer CPF ordem:** `strip_chars → replace_all(r"\D", "") →
  pad_start(11, "0") → _aplicar_rq002`. Pontuação é removida ANTES de pad.
  Entrada `".-./"` vira `""` (filtrada por RQ-002 via sentinela).
- **`.str.*` falha em coluna `Null`-typed:** cast para `pl.Utf8` antes de
  `strip_chars`/`pad_start`. Sem cast, Polars lança erro de tipo em coluna
  totalmente nula.
- **`replace_strict(..., default=None)` falha se a coluna não tiver nulos:**
  usar `pl.when().then().otherwise()` para mapeamentos SUS em vez de
  `replace_strict`.
- **Tenant ContextVar:** herdada por tasks async via
  `contextvars.copy_context()`. Se criar threads manuais, repassar o contexto
  explicitamente.
- **Protocol + `@runtime_checkable`:** só adicionar a decorator quando
  precisa de `isinstance()` check. Custo em performance é desprezível mas
  expõe atributos não-públicos em `dir()`.

Código diretamente nesta pasta, ao lado de tests/. Empacotamento wheel mantém
o namespace público. Fronteiras atuais: docs/architecture.md.
