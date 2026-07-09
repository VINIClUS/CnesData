# CnesData — Roadmap

> Fonte única da verdade sobre prioridades. Atualizar ao fechar/abrir escopo.
> Contexto narrativo em `docs/project-context.md`; arquitetura em
> `docs/architecture.md`.

## Now (implementado / útil)

| Item | Estado | Evidência |
|---|---|---|
| Contratos canônicos | Ativo | `packages/cnes_contracts/` + `docs/contracts/` |
| CNES local edge | Ativo | `apps/dump_agent_go/internal/extractor/cnes.go` |
| CNES nacional clients/adapters | Ativo em biblioteca | `cnes_infra.ingestion.web_client`, `cnes_infra.ingestion.cnes_oficial_web_adapter`, `data_processor.adapters.cnes_nacional_adapter` |
| SIHD edge | Ativo | `apps/dump_agent_go/internal/extractor/sihd.go` |
| BPA-Mag edge | Ativo | `apps/dump_agent_go/internal/extractor/bpa.go` |
| SIA edge | Ativo | `apps/dump_agent_go/internal/extractor/sia.go` |
| Landing N-file manifest | Ativo parcial | `landing.extractions`, `/api/v1/extractions/enqueue`, `/api/v1/jobs/register` |
| Multi-tenant RLS + middleware | Pronto para piloto | `cnes_infra.storage.rls` + `central_api.middleware` |
| OAuth device activation + mTLS provisioning | Ativo | `central_api.routes.oauth`, `central_api.routes.provision`, `apps/dump_agent_go/internal/auth` |
| Web dashboard v1.1 | Ativo | `apps/web_dashboard/` (`/activate`, `/overview`, `/access-pending`, agent status, dark mode) |
| Quality/perf suites | Ativo | `.github/workflows/ci.yml`, `python-quality.yml`, `dump-agent-go.yml`, `web-dashboard.yml`, `tests/perf/` |

## Next (planejado / pendente)

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
