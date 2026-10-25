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
| Distribuição do edge agent (GitHub Releases + R2) | Ativo | `.github/workflows/dump-agent-go-release.yml`, `docs/contracts/dumpagent-update-manifest.schema.json`, `docs/runbooks/dumpagent-release.md` |

## Next (backlog ativo, atualizado em 2026-10-25)

Ordem de execução e auditorias em
`docs/superpowers/plans/2026-10-25-backlog-execution-plan.md`. O runtime AWS já
está integrado; infraestrutura parcial em código não significa promoção AWS concluída.

| Item | Prioridade | Bloqueio / Pré-req |
|---|---|---|
| Corrida entre execução e bind do dispatch (#413) | Alta | Regressão determinística, auditoria dos testes e correção antes de avançar o runtime |
| Evidências MIG-012/013 (#278/#279, concluídas) | Aceitas | Listagem não root de 2026-10-25 publicada e auditada; write-fence/snapshots atestados, sem antecipar remoção do legado |
| Identidade e descoberta AWS (#398/#381/#410) | Alta | Capability checks reais; Cognito Essentials/managed login v2; memberships e sessão sem tenant selecionado |
| Outbox e workers isolados (#374/#375) | Alta | Cursor/CAS antes de separar unit/recovery/audit; gate de runtime #377 |
| Infraestrutura AWS e gates (#365) | Alta | #382, #384–#389; custos até USD 8/produto e freeze na previsão agregada USD 15 |
| Cutover final (#280/#281) | Alta | Promoção AWS, equivalentes das features vivas e evidências atuais antes de remover PostgreSQL/MinIO/BigQuery/Keycloak |
| PEN IANA dos certificados (#60) | Pré-produção | Ainda sem atribuição; bloqueia emissão/rollout de certificados de produção |
| Rules service externo | Média | Repo separado; consome Gold/landing via SQL JOINs |
| HR PIS->CPF cross-walking | Média | Reativar/reescrever fluxo em monorepo |
| Esus PEC | Alta | Acesso ao DB municipal varia; negociação política |
| Automated DATASUS submission check | Baixa | Alertar quando competência local > nacional por mais de 2 meses |
| Web dashboard v1.2 | Média | Faturamento+regressão, drill estabelecimento, admin UI approve/reject |
| Billing e entitlements (#95, concluída) | Implementado | Manter `BILLING_MODE=disabled` neste rollout; ativação comercial exige seus próprios pré-requisitos |
| Perfil AWS (#94, concluída) | Implementado | Runtime/gate integrados; promoção de produção pertence ao backlog #365; ver [readiness](production-readiness.md). |
| PII em logs (CPF/nome/PIS em WARNING) | Média | `transformer.py`, `hr_client.py` logam CPF/nome/PIS crus — mascarar antes de logar |
| Update check + self-update no edge agent | Média | Contrato de manifesto já publicado (distribuição acima); falta `internal/updater` no dumpagent, fix do bug `AGENT_VERSION` sempre `dev` (`internal/apiclient/adapter.go`), e assinatura de código para AV estrito |
| Rebase de trabalho ativo | Em execução | Base `develop`; reaproveitar branches antigas somente quando ligadas a issues/PRs pendentes, preservando os demais worktrees |

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
