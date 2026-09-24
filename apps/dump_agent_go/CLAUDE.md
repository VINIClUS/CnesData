# dump_agent_go — Edge Agent (Go implementation)

## Executive Summary

Port Go do `dump_agent` Python (migração COMPLETE). Roda no edge (Firebird
CNES/SIHD/BPA + DBF SIA) extraindo Parquet delta e enviando para MinIO via
presigned PUT. mTLS por padrão via `dumpagent register`. Histórico
de fases: `CHANGELOG.md` (não autoritativo).

## Role

Edge Agent. Binário estático Windows amd64 (alvo primário) + Linux amd64
(dev/CI). Long-poll `central_api`, 3 goroutines coordenadas por errgroup
(extract/write/upload), panic recovery sandboxed.

## Layout

- `cmd/dumpagent/` — entrypoint + subcommands (`run`, `register`,
  `discover`, `set-secret`, `diagnose`, `audit verify`)
- `internal/obs/` — SafeRun, SafeGo, WithBackoff, classify, slog, Event Log
- `internal/platform/` — Win32/POSIX boundary (build tags)
- `internal/fbdriver/` — DSN builder sobre `database/sql`
- `internal/extractor/` — CNES (3-query+merge), SIHD, BPA (`bpa.go`,
  FB 1.5), SIA (`sia.go`, DBF)
- `internal/delta/` — SHA-256 row-fingerprint delta (types/profiles/
  fingerprint/compute/store/writer)
- `internal/auth/` — mTLS cert lifecycle: CA pin, rotation (`rotate.go`)
- `internal/queue/` — bbolt outbox (CompleteJob/FailJob persistence)
- `internal/breaker/` — circuit breaker CLOSED/OPEN/HALF_OPEN
- `internal/writer/` — Parquet+gzip streaming via io.Pipe
- `internal/upload/` — HTTP PUT streaming
- `internal/apiclient/` — gerado via oapi-codegen
- `internal/worker/` — JobExecutor, Consumer.Loop, heartbeat

## Build

`make build-windows VERSION=v0.1.0` → `dist/dumpagent.exe` (cross-compile de
Linux se driver pure-Go). `VERSION` fica em `main.Version` via `-X`; default
é `git describe --tags --match 'dumpagent-go-v*'` (cai para `dev` sem tag
correspondente — nunca string vazia). Sem `VERSION`, todo build local
reporta `dev`.

## Release

GitHub Releases é o registro de versões; Cloudflare R2 é a distribuição
primária (fallback: asset do Release). Tag `dumpagent-go-v<versão>` dispara
`.github/workflows/dump-agent-go-release.yml`. Contrato do manifesto de
update: `docs/contracts/dumpagent-update-manifest.schema.json`. Corte de
release, canais `stable`/`rc`, dry-run e rollback:
`docs/runbooks/dumpagent-release.md`. `min_supported_version` do manifesto
vem de `MIN_SUPPORTED_VERSION` (arquivo versionado, bump manual em PR).
Notas de release são geradas por `gh release create --generate-notes`
(títulos de PR); `CHANGELOG.md` continua não-autoritativo. Cliente de update
check / self-update no agente ainda não existe (roadmap `Next`).

## Test

- `make test` — unit (mocks). `make test-e2e` — smoke com stub API + FB fake.
- Coverage gate: 65% on filtered set (excludes `generated.go`, `cmd/`,
  `internal/service/`, `*_windows.go`). Reproduce:
  ```bash
  go test -race -count=1 -coverprofile=coverage.out ./...
  grep -v -E "internal/apiclient/generated\.go|cmd/|internal/service/|_windows\.go:" \
    coverage.out > coverage.filtered.out
  go tool cover -func=coverage.filtered.out | tail -1    # gate >= 65%
  ```
- CI label vocab:
  - `run-windows-integration` → runs `integration-windows` (FB 2.5 service
    runs both CNES/SIHD and BPA fixtures via isql). Windows-latest runner.
  - `run-integration` → runs `sia-integration` (Linux, DBF fixtures).
  - Nightly schedule at `30 2 * * *` UTC runs both regardless of label.
- Full layout + filter regex: `apps/dump_agent_go/test/README.md`.

## Limitations

- **Nunca acessa Postgres diretamente** — lê Firebird/DBF/GDB local e envia
  Parquet ao MinIO via presigned PUT. Persistência em Postgres é
  responsabilidade exclusiva do `central_api` / `data_processor`.

## Gotchas

- Bug Firebird -501: `LFCES021 ↔ LFCES060` 1-query retorna NULLs silenciosos.
  Workaround = 3 queries + merge em Go por `SEQ_EQUIPE[:4]`. **NÃO** simplificar.
- Encoding WIN1252 sujo em bancos CNES legados → UTF-8 sanitize obrigatório
  antes de serializar Parquet (ver `internal/extractor/sanitize.go`).
- Clock drift: NTP pré-flight em boot (ver `internal/platform/ntp_check.go`).
  Skew > 60min → exit(1).
- Panic recovery: TODO spawn de goroutine passa por `SafeGo`/`SafeRun`. Nunca
  `go func()...()` direto em código de produção.
- **Delta é o único modo de execução** (sem flag, sem snapshot legado): CNES/
  SIHD/BPA usam fingerprint SHA-256 linha-a-linha (`internal/delta`); SIA
  segue full-extract. Parquet emite coluna `_op ∈ {I,U,D}`.
- **mTLS por padrão:** `dumpagent register` provisiona o cert; sem cert
  registrado, `AGENT_ALLOW_INSECURE=true` é o único fallback (fail-closed
  por padrão). `internal/auth/rotate.go` renova em background quando resta
  menos de 1/3 da validade. Verificação do cert do *servidor* usa o trust
  store da plataforma por padrão; `register --ca-pin <arquivo>` (CA privada,
  dev/staging) persiste o pin em `authDir/ca_pin.pem`, e `run` o lê de volta
  — `--force` sem `--ca-pin` remove um pin persistido de um registro
  anterior.
- **BPA (`--bpa-gdb`/`BPA_GDB_PATH`) requer FB 1.5 x86 no runtime** — driver
  nakagami/firebirdsql. Produção fica em `S_PRD` (não `BPA_*_LINHAS`), subtipo
  por `PRD_ORG` (`BPA`/`BPI`); ver `docs/data-dictionary-bpa.md`.
- **SIA (`--sia-dir`/`SIA_DIR`) lê DBF** via LindsayBradford/go-dbf com
  sanitize cp1252 (S_APA, S_BPI, S_BPIHST, S_CDN, CADMUN).
- **Audit trail HMAC-JSONL:** `%PROGRAMDATA%\dumpagent\audit\events-*.jsonl`,
  lifecycle extracted→uploaded→committed/aborted. Verificar com
  `dumpagent audit verify <path>`.
