# dump_agent — Edge Agent

## Executive Summary

Daemon assíncrono que roda **próximo à fonte de dados** (máquina do município
com Firebird CNES, ou servidor hospitalar com SIHD) e faz ELT mínimo: extrai
via cursor paginado, serializa para Parquet com streaming gzip, e faz upload
incremental para MinIO via URL pré-assinada emitida pelo `central_api`.
Stateless — não mantém estado além do Parquet local temporário.

## Role

**Edge Agent** no pattern edge/central. Implementação Python; cada município
com Firebird precisa de 1 instância rodando (Windows Service ou systemd).
Registra-se via `machine_id` estável e faz polling de jobs pendentes para
seu tenant.

## Functionalities

- Polling de fila de jobs via `GET /api/v1/jobs/next` (long-poll + heartbeat)
- Extração Firebird CNES (`CnesExtractor`) — 3 queries + merge em Python
  devido bug -501 do LEFT JOIN via `fdb`
- Extração SIHD hospitalar (`SihdExtractor`) — AIH + procedimentos
- Streaming gzip → Parquet (sem carregar tudo em memória via `SpoolGuard`)
- Upload multipart para MinIO via presigned PUT URL
- Single-instance lock (mutex Windows `Global\CnesDumpAgent-<mid>` / fcntl POSIX)
- Graceful shutdown (SIGTERM/SIGINT POSIX, Ctrl-Break Windows)
- Jitter de startup (max 30min configurável) para evitar thundering-herd
- Heartbeat periódico para estender lease do job no `central_api`

## Objectives

- ≥ 99% dos jobs completam sem intervenção humana
- Latência extract → upload ≤ 10min para 100k rows
- RSS estável em soak 30min (slope < 1 MB/min — gate de soak test)
- Zero file descriptor leak (delta < 100 em 30min — gate de soak test)

## Limitations

- **Não aplica regras de negócio** — só extrai raw e entrega
- **Não transforma dados** (exceto tipagem Polars) — transform fica no `data_processor`
- **Não lê/escreve em Postgres Gold** — comunica só com `central_api` + MinIO
- **Não funciona sem rede** para o `central_api` — sem modo offline
- **Exige Firebird 2.5 embedded + `fbclient.dll` 64-bit** (via `FIREBIRD_DLL`)
- Windows-only: requer SeCreateGlobalPrivilege para named mutex Global\

## Requirements

**Runtime deps (apps/dump_agent/pyproject.toml):** `fdb`, `polars`, `httpx`,
`cnes_domain`, `cnes_infra` (client-side: `object_storage` + `ingestion/db_client`).

**Env vars:**

| Var | Obrigatória | Descrição |
|---|---|---|
| `CENTRAL_API_URL` | sim | Base URL do `central_api` (ex.: `https://api.cnesdata.gov.br`) |
| `TENANT_ID` | sim | ID do município (IBGE 6-dígitos, ex.: `354130`) |
| `DB_PATH` | sim (CNES) | Caminho para `CNES.GDB` |
| `DB_PASSWORD` | sim (CNES) | Senha Firebird |
| `FIREBIRD_DLL` | sim (CNES) | Path para `fbclient.dll` 64-bit |
| `SIHD_DB_PATH` | sim (SIHD) | Caminho para DB SIHD |
| `MACHINE_ID` | opcional | Override (senão gera UUID estável em `app_data_dir/machine_id`) |
| `DUMP_MAX_JITTER_SECONDS` | opcional | Jitter de startup (default `1800`) |
| `DUMP_LOGS_DIR` | opcional | Override do dir de logs (default `%LOCALAPPDATA%/CnesAgent/logs` ou `~/.local/state/cnes-agent/logs`) |

**Infra:** acesso de rede ao `central_api` (HTTPS) e ao MinIO (via presigned URLs).

## Module Map

| Arquivo | Responsabilidade |
|---|---|
| `src/dump_agent/main.py` | Entrypoint daemon — async loop + install_shutdown_handler |
| `src/dump_agent/cli.py` | CLI (`--version`, `--help`) — não aceita flags de conexão |
| `src/dump_agent/platform_runtime.py` | POSIX vs Win32: lock, signals, `logs_dir`, `fbclient_dll_path`, `resolve_machine_id` |
| `src/dump_agent/io_guard.py` | `SpoolGuard(max_bytes)` — limite de bytes em disco para parquet temp |
| `src/dump_agent/worker/consumer.py` | Loop de `_acquire_job` + `_execute_job` + `_heartbeat_loop` |
| `src/dump_agent/worker/connection.py` | Factory `fdb.connect(charset="WIN1252")` |
| `src/dump_agent/worker/streaming_executor.py` | Pipeline extract → parquet gzip → presigned upload |
| `src/dump_agent/extractors/cnes_extractor.py` | 3 queries CNES + merge em `SEQ_EQUIPE` |
| `src/dump_agent/extractors/sihd_extractor.py` | Extração SIHD / AIH |
| `src/dump_agent/extractors/protocol.py` | `Extractor` Protocol (contrato do registry) |
| `src/dump_agent/extractors/registry.py` | `intent → extractor` lookup dinâmico |

## Gotchas

- **Firebird LEFT JOIN -501:** `pd.read_sql` com LEFT JOIN a `LFCES060`
  retorna NULLs silenciosamente. Workaround: 3 queries separadas + merge
  Polars por prefixo de 4 chars em `SEQ_EQUIPE`. **NÃO simplificar para
  1 query** — o teste de regressão só pega a falha em produção.
- **WIN1252 encoding:** `fdb.connect(..., charset="WIN1252")` obrigatório.
  Nomes acentuados (ex.: "Atenção Básica") precisam NFKD antes de merges
  para não dar false-negative em cross-check com nacional.
- **ORDER BY posicional** não suportado em tabelas `RDB$` do Firebird 2.5.
- **Windows mutex:** `acquire_single_instance_lock()` usa named mutex
  `Global\CnesDumpAgent-<machine_id>`. Requer `SeCreateGlobalPrivilege` —
  em sessão limitada, cai para mutex local (apenas 1 sessão protegida).
  POSIX usa `fcntl.flock` em lock file em `app_data_dir/.lock`.
- **Frozen PyInstaller:** `fbclient_dll_path()` tem branch que lê de
  `sys._MEIPASS`. Marcado `# pragma: no cover - frozen only`.
- **PlatformRuntime dual-pragma:** blocos inteiros `if sys.platform == "win32"`
  e `!= "win32"` têm `# pragma: no cover - windows_only` / `posix_only`.
  Coverage é por plataforma — CI Linux cobre o bloco POSIX, rodar local
  Windows cobre o Windows.
- **Startup jitter:** `random.uniform(0, DUMP_MAX_JITTER_SECONDS)` antes do
  primeiro poll. Em N edges, reduz pico de carga contra `central_api`.
  **Não remover.**
