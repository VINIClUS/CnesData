# CnesData — Roadmap

> Fonte única da verdade sobre prioridades. Atualizar ao fechar/abrir escopo.
> Contexto narrativo em `docs/project-context.md`; arquitetura em
> `docs/architecture.md`.

## Now (implementado / útil)

| Item | Estado | Evidência |
|---|---|---|
| CNES local via Firebird | Ativo | `dump_agent_go/internal/extractor` + data_processor adapter |
| CNES nacional via BigQuery | Ativo | `cnes_infra.ingestion.web_client` + data_processor adapter |
| CNES via DATASUS API | Ativo | `cnes_infra.ingestion.cnes_oficial_web_adapter` |
| SIHD hospitalar | Ativo | `dump_agent_go/internal/extractor` + data_processor adapter |
| BPA (Boletim Produção Ambulatorial) | Ativo | `dump_agent_go.internal.extractor.ExtractBPA` + `data_processor.adapters.bpa_adapter` |
| SIA (Sistema Info Ambulatorial) | Ativo | `dump_agent_go.internal.extractor.ExtractSIA` + `data_processor.adapters.sia_adapter` + `sia_dim_sync` |
| Multi-tenant (RLS + Middleware) | Pronto, piloto PE/SP | `cnes_infra.storage.rls` + `central_api.middleware` |
| Perf test pipeline (5 tiers) | Pronto | `tests/perf/{micro,macro,stress,soak,spike}/` + nightly workflow |
| CI com gates triplos (Python packages 100% branch, apps 90% line; Go agent 65% filtered) | Pronto | `.github/workflows/ci.yml` + `.github/workflows/dump-agent-go.yml` |
| Web dashboard v1.0 | Ativo | `apps/web_dashboard/` (Bun+React+OIDC), `/activate` para P4 device flow + status agentes via landing.extractions |
| Web dashboard v1.1 | Ativo | `/overview` (KPIs + faturamento area chart 12m via Tremor lazy), `/access-pending` (signup JIT + admin SQL — runbook em `docs/runbooks/access-request-approval.md`), dark mode 3-state |
| Edge agent zero-trust | Ativo | `dumpagent register`, mTLS default, `/provision/cert/rotate`, DPAPI secrets |
| Edge agent resiliência | Ativo | bbolt outbox, circuit breaker, jittered backoff, `dumpagent diagnose` |
| Delta + integridade | Ativo | `_op` delta Parquet, `delta.db`, SHA-256 em `landing.extractions.sha256`, HMAC audit JSONL |

## Next (planejado, sem código ainda)

| Item | Prioridade | Bloqueio / Pré-req |
|---|---|---|
| Completar `data_processor` landing -> Gold | Alta | Implementar `extractions_repo.complete/fail/heartbeat/mark_uploaded/reap_expired` e ligar download/adapters/repos |
| End-to-end extraction lifecycle | Alta | Reconciliar status `PENDING/CLAIMED/REGISTERED/UPLOADED/PROCESSING/INGESTED` e presigned URL ownership |
| Rules service externo | Média | Repo separado; consome Gold/landing via SQL JOINs |
| HR PIS->CPF cross-walking | Média | Reativar/reescrever fluxo em monorepo |
| Esus PEC | Alta | Acesso ao DB municipal varia; negociação política |
| Automated DATASUS submission check | Baixa | Alertar quando competência local > nacional por mais de 2 meses |
| Web dashboard v1.2 | Média | Faturamento+regressão, drill estabelecimento, admin UI approve/reject |
| Kubernetes central stack | Média | Completar manifests/charts para API, processor, migrator, Postgres/MinIO ou serviços gerenciados |

## Later (conceitual)

| Item | Motivação |
|---|---|
| Team-level audit | Audit de equipes ESF/EAP/ESB; bloqueado por gap de formato INE (FB 10 chars vs BQ 18) |
| Fontes DATASUS adicionais | SIGTAP e outros módulos |
| Integração Pro-Saúde / CNES-WEB | Validação em tempo real de envios ao DATASUS |

## Removido definitivamente

| Item | Razão |
|---|---|
| CLI monolítico `src/main.py` | Substituído por apps distribuídos edge + central + processor |
| `pipeline/orchestrator.py` | Substituído por landing queue e workers stateless |
| Camada interna de regras RQ-002 a RQ-011 | Movida para serviço externo |
| Excel/CSV exporters (`csv_exporter`, `report_generator`) | Obsoleto com rules service/dashboard |
