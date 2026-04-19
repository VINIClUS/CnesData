# batch_watcher — One-shot batch trigger evaluator

## Executive Summary

Script CLI executado como k8s CronJob (default `*/15 * * * *`). Avalia
pending bytes (`SUM(landing.raw_payload.size_bytes)` onde `jobs.status =
'COMPLETED'`) e idade do mais antigo COMPLETED. Se size >= threshold MB
OR age > threshold dias, UPDATE `queue.batch_trigger` para status='OPEN'.
Sempre refresca `pending_bytes` e `updated_at` mesmo quando condição
não bate. Stateless, idempotente, exit 0 em sucesso.

## Role

CronJob standalone — não daemon. Propósito único: decidir quando a fila
está madura pra drenar, sinalizar via row em Postgres, sair. Desacoplado
do `data_processor` (coordenação só via DB).

## Functionalities

- Conecta ao Postgres via `DB_URL`
- Query agregada `queue.jobs` + `landing.raw_payload`
- UPDATE condicional em `queue.batch_trigger`
- Log estruturado `watcher_tick` por execução
- Telemetria OTel opcional (`init_telemetry("batch-watcher")`)

## Objectives

- Latência p99 < 1s por run (1 SELECT + 1 UPDATE)
- Idempotência total — rodar 2× na mesma janela = mesmo efeito
- Exit 0 sucesso, exit 1 falha (k8s marca Job Failed)

## Limitations

- Não drena jobs — só sinaliza
- Não orquestra retry (k8s CronJob faz via `restartPolicy: OnFailure`)
- Não lê MinIO (size vem de `raw_payload.size_bytes`)

## Requirements

| Env | Obrigatória | Default | Descrição |
|---|---|---|---|
| `DB_URL` | sim | — | Postgres URL |
| `WATCHER_SIZE_THRESHOLD_MB` | opcional | `100` | Threshold em MB |
| `WATCHER_AGE_THRESHOLD_DAYS` | opcional | `2` | Threshold em dias |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | opcional | — | Tracing |

## Module Map

| Arquivo | Responsabilidade |
|---|---|
| `src/batch_watcher/main.py` | Entrypoint CLI + setup logging |
| `src/batch_watcher/config.py` | Env vars |
| `src/batch_watcher/watcher.py` | `run_once(engine) -> int` |

## Gotchas

- `run_once` é SÍNCRONO — k8s CronJob roda Python puro. Não misturar com
  asyncio.
- `engine.dispose()` no `finally` — evita `connection not released` em
  exit code != 0.
- Migrations 007/008 são pré-requisitos — sem `queue.batch_trigger` o
  watcher dá `UndefinedTableError` no primeiro run. Garantido pela ordem
  de deploy (cnes_db_migrator roda antes).
