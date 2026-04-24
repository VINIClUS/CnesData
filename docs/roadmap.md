# CnesData — Roadmap

> Fonte única da verdade sobre prioridades. Atualizar ao fechar/abrir escopo.
> Histórico detalhado em `docs/project-context.md`.

## Now (em produção / ativo)

| Item | Estado | Evidência |
|---|---|---|
| CNES local via Firebird | Ativo | `dump_agent` extractors + data_processor adapter |
| CNES nacional via BigQuery | Ativo | `cnes_infra.ingestion.web_client` + data_processor adapter |
| CNES via DATASUS API | Ativo | `cnes_infra.ingestion.cnes_oficial_web_adapter` |
| SIHD hospitalar | Ativo | `dump_agent.extractors.sihd_extractor` + data_processor adapter |
| BPA (Boletim Produção Ambulatorial) | Ativo | `dump_agent_go.internal.extractor.ExtractBPA` + `data_processor.adapters.bpa_adapter` |
| SIA (Sistema Info Ambulatorial) | Ativo | `dump_agent_go.internal.extractor.ExtractSIA` + `data_processor.adapters.sia_adapter` + `sia_dim_sync` |
| Multi-tenant (RLS + Middleware) | Pronto, piloto PE/SP | `cnes_infra.storage.rls` + `central_api.middleware` |
| Perf test pipeline (5 tiers) | Pronto | `tests/perf/{micro,macro,stress,soak,spike}/` + nightly workflow |
| CI com gates triplos (Python packages 100% branch, apps 90% line; Go agent 65% filtered) | Pronto | `.github/workflows/ci.yml` + `.github/workflows/dump-agent-go.yml` |

## Next (planejado, sem código ainda)

| Item | Prioridade | Bloqueio / Pré-req |
|---|---|---|
| Esus PEC (Prontuário Eletrônico Cidadão) | Alta | Acesso ao DB municipal varia; negociação política |
| HR PIS→CPF cross-walking | Média | `scripts/hr_pre_processor.py` existia em iteração anterior (61% cobertura) — reativar/reescrever no monorepo |
| Rules service (serviço externo) | Média | Repo separado; consome Gold via SQL JOINs |
| Automated DATASUS submission check | Baixa | Alert quando competência local > BigQuery nacional por > 2 meses |

## Later (conceitual — sem comprometimento de escopo)

| Item | Motivação |
|---|---|
| Web dashboard (Streamlit/Metabase/custom) | Substituir relatórios Excel (removidos do escopo) para audiência > 3 pessoas |
| Team-level audit | Audit de equipes ESF/EAP/ESB. Bloqueado por INE format gap (FB 10 chars vs BQ 18) |
| Apps de fontes adicionais | SIGTAP e outros módulos DATASUS |
| Integração Pro-Saúde / CNES-WEB | Validação em tempo real de envios ao DATASUS |

## Removido definitivamente (2026-04)

| Item | Razão |
|---|---|
| Camada de regras de auditoria (RQ-002 a RQ-011) | Movida para serviço externo que consome Gold via SQL JOINs |
| Excel/CSV exporters (`csv_exporter`, `report_generator`) | Obsoleto com rules service externo; dashboards futuros substituem |
| CLI monolítico `src/main.py` | Substituído por apps distribuídos (edge + central + processor) |
| `pipeline/orchestrator.py` | Substituído por job queue em `central_api` + workers stateless |
