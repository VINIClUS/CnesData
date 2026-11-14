# Promoção de produção e gate final de operações (#477)

Gate final da EPIC #465 (Task 12 do plano de entrega): as matrizes de pré-produção e de smoke
de produção, o schema da evidência de cada item e os drills de operação.

Este gate não faz deploy: nenhum comando daqui aplica plan, promove release ou muda recurso. A
execução real de cada item fica com o estágio indicado, sob as regras do runbook dono.

`tests/production/delivery/test_operations_acceptance.py` confere este arquivo contra os runbooks
existentes e contra a Task 12 do plano de entrega e operações de 2026-08-31, em
`docs/superpowers/plans/`.

## Limites

- Código completo não é produção aceita. A EPIC só fecha com todos os estágios Produção aceitos
  e esta matriz inteira com evidência `PASS` no SHA integrado de `develop`.
- Os comandos do Step 5 do plano estão desatualizados. Os equivalentes estão em
  [infrastructure-plan.md](infrastructure-plan.md#desvios-dos-comandos-da-issue). A suíte rápida
  mantém `--ignore=tests/production/infra`.
- Procedimentos já documentados ficam no runbook dono:
  - gate de runtime: [runtime-acceptance.md](runtime-acceptance.md);
  - gate PROD-INFRA-012 e checks reais: [infrastructure-plan.md](infrastructure-plan.md);
  - runtime AWS: [aws-runtime-validation.md](aws-runtime-validation.md);
  - capability checks executados pela #398:
    [floci-and-aws-capability.md](floci-and-aws-capability.md);
  - Roles Anywhere: [roles-anywhere-evidence.md](roles-anywhere-evidence.md);
  - Object Lock COMPLIANCE: [object-lock-capability.md](object-lock-capability.md);
  - canaries de modo: [mode-canary.md](mode-canary.md);
  - rollback, drain e poda: [rollback.md](rollback.md).

## Ordem da promoção

1. Candidato e plan/apply exatos (#467, #468): [infrastructure-plan.md](infrastructure-plan.md).
2. Phase A, activation assinada e canary (#469 a #471): [mode-canary.md](mode-canary.md).
3. Fence, drain e Phase B (#472), depois API e estático (#473):
   [infrastructure-plan.md](infrastructure-plan.md#publicador-estático-473).
4. Phase C, recovery e depois audit, com reabertura (#474): [recovery-drill.md](recovery-drill.md).
5. Smoke de produção (#476) e, se preciso, rollback (#475): [rollback.md](rollback.md).

Drills periódicos: [roles-anywhere-incident.md](roles-anywhere-incident.md),
[cost-freeze.md](cost-freeze.md) e [quarterly-restore.md](quarterly-restore.md).

## Como ler as matrizes

Cada item tem um ID estável e estes campos:

- `Plano`: o item literal da Task 12 do plano;
- `Origem`: a fonte de um item que não está no plano (EPIC, comentário da #477, tabela "Só
  documentados" do [infrastructure-plan.md](infrastructure-plan.md));
- `Verificação`: os caminhos que provam o item;
- `Evidência`: o que o registro guarda, sempre redigido;
- `Estágio`: `Código` (CI no SHA integrado) ou `Produção da #N` (execução real aceita na issue).

O resultado de um item nunca entra aqui: ele vive só no registro de evidência do schema abaixo.
A redação segue o formato comum de [infrastructure-plan.md](infrastructure-plan.md): status,
códigos, contagens e booleanos; sem conta, ARN, ID, domínio, token, URL assinada, object key,
corpo sintético, e-mail ou ID de tenant, run ou usuário.

## Matriz de pré-produção

- **PRE-01** Gates CND e AWS no SHA exato
  - Plano: `All CND/AWS gates`
  - Verificação: [runtime-acceptance.md](runtime-acceptance.md),
    [infrastructure-plan.md](infrastructure-plan.md)
  - Evidência: runs de `production-runtime-acceptance` e do PROD-INFRA-012 no SHA.
  - Estágio: Código.
- **PRE-02** DynamoDB e Object Lock reais
  - Plano: `real Dynamo/Object Lock`
  - Verificação: [floci-and-aws-capability.md](floci-and-aws-capability.md),
    [object-lock-capability.md](object-lock-capability.md)
  - Evidência: perfis `ddb-*`, `s3-hold` e `s3-retention` da #398, sem reexecutar.
  - Estágio: Código, com a evidência real da #398.
- **PRE-03** Rotação de JWKS, membership revogada e negação cross-tenant
  - Plano: `OIDC rotation/revocation/cross-tenant`
  - Verificação: `apps/central_api/tests/auth/test_aws_oidc.py`,
    `tests/integration/aws/test_cross_tenant.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-04** Fence antigo e replay de execução duplicada
  - Plano: `fence/dispatch replay`
  - Verificação: `tests/integration/aws/test_fencing_idempotency.py`,
    `tests/chaos/test_outbox_audit_replay.py`, `tests/production/delivery/test_canary_replay.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-05** Fonte obrigatória falha e opcional publica degradado
  - Plano: `source behavior`
  - Verificação: `apps/data_processor/tests/orchestration/test_coordinator.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-06** Cursor do outbox e eventos poison sem starvation
  - Plano: `outbox cursor/poison`
  - Verificação: `packages/cnes_infra/tests/control_plane/test_outbox_cursor.py`,
    `apps/data_processor/tests/test_audit_dispatch_mode.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-07** Dashboard sem `/api` relativo nem chamada fora do client absoluto
  - Plano: `browser contract`
  - Verificação: `apps/web_dashboard/tests/unit/api/production-client.test.ts`
  - Evidência: relatório Vitest do `runtime-dashboard`.
  - Estágio: Código.
- **PRE-08** Refresh de credencial pelo helper do Roles Anywhere
  - Plano: `credential refresh`
  - Verificação: `tests/production/test_credential_process.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-09** Boot do VPS com escopo exato da máquina
  - Plano: `VPS IAM`
  - Verificação: `apps/central_api/tests/test_aws_composition.py`,
    [aws-runtime-validation.md](aws-runtime-validation.md)
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-10** Trust anchor de CA externa e CRL validados
  - Plano: `external CA/CRL`
  - Verificação: `infra/opentofu/env/prod/variables.tf`,
    [roles-anywhere-evidence.md](roles-anywhere-evidence.md)
  - Evidência: `tofu validate` e o gate PROD-INFRA-012.
  - Estágio: Código.
- **PRE-11** Role do Step Functions com trust e ações exatas
  - Plano: `Step Functions IAM`
  - Verificação: `tests/production/infra/test_step_functions_iam.py`,
    `tests/production/infra/test_step_functions_iam_asl.py`
  - Evidência: suíte nativa do PROD-INFRA-012.
  - Estágio: Código.
- **PRE-12** Manifest candidato, activation e Phases A, B e C
  - Plano: `manifest/Phase A/B/C`
  - Verificação: `tests/production/delivery/test_candidate_manifest.py`,
    `tests/production/delivery/test_phase_a.py`, `tests/production/delivery/test_phase_b.py`,
    `tests/production/delivery/test_phase_c.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-13** Canary ligado à release, com COMPLIANCE e retenção
  - Plano: `canary`
  - Verificação: `tests/production/delivery/test_canary.py`, [mode-canary.md](mode-canary.md)
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-14** Drain do trabalho antigo e poda só de revisões fora da retenção
  - Plano: `drain/prune`
  - Verificação: `tests/production/delivery/test_rollback_drain.py`, [rollback.md](rollback.md)
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-15** Modos e deadlines de recovery e audit
  - Plano: `recovery/audit modes/deadlines`
  - Verificação: `apps/data_processor/tests/test_audit_dispatch_deadline.py`,
    `tests/production/infra/test_schedulers.py`
  - Evidência: JUnit do CI e a suíte nativa.
  - Estágio: Código.
- **PRE-16** Corridas do gate: close primeiro, takeover com prova, perdedor sem start
  - Plano: `gate races`
  - Verificação: `tests/integration/aws/test_environment_gate_races.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-17** Orçamento de task-hour e contador de execuções
  - Plano: `task-hour/counter budget`
  - Verificação: `tests/production/infra/test_cost_manifest.py`,
    `packages/cnes_domain/tests/control_plane/test_execution_quota.py`
  - Evidência: JUnit do CI e a suíte nativa.
  - Estágio: Código.
- **PRE-18** Crash em `PUBLISHING` com a role de recovery
  - Plano: `publishing crash IAM`
  - Verificação: `tests/chaos/test_publisher_pointer_recovery.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-19** Override AWS-012 de produção
  - Plano: `AWS-012 override`
  - Verificação: `packages/cnes_infra/tests/executor/test_production_network.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-20** CDN, Free, Cognito, rate limit e custo
  - Plano: `CDN/Free/Cognito/rate/cost policy`
  - Verificação: `tests/production/infra/test_static_web.py`,
    `tests/production/infra/test_cognito.py`, `tests/production/infra/test_opa_policy.py`,
    `apps/central_api/tests/test_app.py`, [cloudfront-free.md](cloudfront-free.md)
  - Evidência: suíte nativa do PROD-INFRA-012 e JUnit do CI.
  - Estágio: Código.
- **PRE-21** Preflight IAM tipado antes das execuções reais assinadas
  - Origem: #477, comentário 6715569897 (preflight IAM tipado antes de canary, B, C e smoke).
  - Verificação: `scripts/promotion/_phase_a_evidence.py`,
    `tests/production/delivery/test_phase_a_contract.py`
  - Evidência: readbacks `iam-readback-and-evaluation` concordantes, sem simular propagação.
  - Estágio: Código.
- **PRE-22** Vínculo entre candidato, release e canary
  - Origem: #477, comentário 6715569897.
  - Verificação: `tests/production/delivery/test_activation_manifest.py`,
    `release/activation.schema.json`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-23** A3 único, com hashes de state e output e proveniência exatos
  - Origem: #477, comentário 6715569897.
  - Verificação: `release/plan-apply-evidence.schema.json`,
    `tests/production/delivery/test_plan_apply_workflows.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-24** Retry e recovery que convergem a rota
  - Origem: #477, comentário 6715569897.
  - Verificação: `tests/integration/aws/test_failure_recovery.py`,
    `tests/production/delivery/test_phase_c.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.
- **PRE-25** Retenção das revisões e digests do processor
  - Origem: #477, comentário 6715569897.
  - Verificação: `packages/cnes_domain/tests/orchestration/test_processor_retention.py`
  - Evidência: JUnit do CI.
  - Estágio: Código.

## Matriz de smoke de produção

- **SMK-01** Estático com a release esperada, TLS e origem S3 privada
  - Plano: `Static/TLS/private origin`
  - Verificação: `scripts/promotion/smoke_production.py`, [cloudfront-free.md](cloudfront-free.md)
  - Evidência: release ID servida, TLS válido e origem negada ao acesso público.
  - Estágio: Produção da #476.
- **SMK-02** API só pelo Tunnel
  - Plano: `Tunnel-only API`
  - Verificação: `scripts/promotion/smoke_production.py`,
    [aws-runtime-validation.md](aws-runtime-validation.md)
  - Evidência: portas do VPS inalcançáveis e health pelo Tunnel.
  - Estágio: Produção da #473.
- **SMK-03** Endpoints padrão do Cognito, PKCE e audience
  - Plano: `standard Cognito endpoints/PKCE/audience`
  - Verificação: `scripts/promotion/smoke_production.py`, `tests/production/infra/test_cognito.py`
  - Evidência: asserções `cognito_pkce_resource` e `aud_refresh`.
  - Estágio: Produção da #476.
- **SMK-04** Leaf exato assume o profile e leaf revogado pela CRL é negado
  - Plano: `exact Roles Anywhere and CRL revocation`
  - Origem: tabela "Só documentados": Aceite e revogação do Roles Anywhere.
  - Verificação: [roles-anywhere-evidence.md](roles-anywhere-evidence.md),
    [roles-anywhere-incident.md](roles-anywhere-incident.md)
  - Evidência: asserção `roles_anywhere_crl` e o tempo até a negação.
  - Estágio: Produção da #476.
- **SMK-05** Evidência das fases, rotas lidas forte e fence
  - Plano: `phase evidence/routes/fence`
  - Verificação: `scripts/promotion/verify_routes.py`, `release/phase-a-evidence.schema.json`
  - Evidência: hashes das evidências A, B e C e as rotas lidas.
  - Estágio: Produção da #474.
- **SMK-06** Falha forçada de recovery e de audit
  - Plano: `recovery/audit failure behavior`
  - Verificação: [recovery-drill.md](recovery-drill.md),
    [aws-runtime-validation.md](aws-runtime-validation.md), [mode-canary.md](mode-canary.md)
  - Evidência: exit 1 com `failed` no resumo e audit entregue a objeto com Object Lock.
  - Estágio: Produção da #474.
- **SMK-07** Recovery travado para no deadline do PID 1
  - Plano: `hung recovery stop`
  - Verificação: [recovery-drill.md](recovery-drill.md),
    [aws-runtime-validation.md](aws-runtime-validation.md)
  - Evidência: saída não zero, task parada no deadline, log e alarme.
  - Estágio: Produção da #474.
- **SMK-08** Recoveries sobrepostas deixam no máximo uma task de unidade
  - Plano: `overlapping recoveries`
  - Verificação: [recovery-drill.md](recovery-drill.md),
    `tests/integration/aws/test_environment_gate_races.py`
  - Evidência: contagem de tasks ativas e perdedores sem `StartExecution`.
  - Estágio: Produção da #474.
- **SMK-09** Uma publicação sintética com uma versão e um pointer novos
  - Plano: `one synthetic publication/pointer`
  - Verificação: [demo-seed.md](demo-seed.md), `scripts/promotion/seed_demo.py`
  - Evidência: uma versão imutável e um pointer `active`.
  - Estágio: Produção da #476.
- **SMK-10** Envelope 200 do serving e segundo fetch ao S3 sem credencial
  - Plano: `200 serving envelope and second S3 fetch`
  - Verificação: `scripts/promotion/smoke_production.py`
  - Evidência: asserção `serving_envelope_body` e `Cache-Control: private, no-store`.
  - Estágio: Produção da #476.
- **SMK-11** Nenhum vazamento em log, trace, state ou artefato
  - Plano: `no leakage`
  - Verificação: `tests/production/delivery/test_smoke_redaction.py`
  - Evidência: varredura de redação da evidência do smoke.
  - Estágio: Produção da #476.
- **SMK-12** Release anterior de API e estático restaurada na janela
  - Plano: `previous release restore`
  - Verificação: [rollback.md](rollback.md), `scripts/promotion/rollback_api_static.py`
  - Evidência: entrypoints restaurados e a ação de host.
  - Estágio: Produção da #475.
- **SMK-13** IAM de serviço do Step Functions
  - Origem: D5 da EPIC #465; tabela "Só documentados": IAM de serviço do Step Functions.
  - Verificação: [infrastructure-plan.md](infrastructure-plan.md),
    `scripts/promotion/smoke_production.py`
  - Evidência: asserção `sfn_trust_task` e execução negada quando o escopo deriva.
  - Estágio: Produção da #476.
- **SMK-14** Métrica de regra EventBridge sem target
  - Origem: D5 da EPIC #465; tabela "Só documentados": `TriggeredRules` de regra sem target.
  - Verificação: [infrastructure-plan.md](infrastructure-plan.md)
  - Evidência: métrica emitida na janela seguinte, sem ARN.
  - Estágio: Produção da #476.
- **SMK-15** GetObject de chave ausente devolve 404 por role
  - Origem: D5 da EPIC #465; tabela "Só documentados": 404 de chave ausente com as três roles
    reais (#441).
  - Verificação: [infrastructure-plan.md](infrastructure-plan.md),
    `tests/production/infra/test_s3_missing_object_invariant.py`
  - Evidência: código HTTP por role, sem ARN nem chave.
  - Estágio: Produção da #476.
- **SMK-16** COMPLIANCE do Object Lock no audit
  - Origem: tabela "Só documentados": Object Lock COMPLIANCE do audit.
  - Verificação: [object-lock-capability.md](object-lock-capability.md),
    `scripts/promotion/smoke_production.py`
  - Evidência: asserção `object_lock_compliance`.
  - Estágio: Produção da #476.
- **SMK-17** Distribuição no plano Free em `ACTIVE`
  - Origem: tabela "Só documentados": CloudFront Free ACTIVE.
  - Verificação: [cloudfront-free.md](cloudfront-free.md),
    `scripts/promotion/reconcile_cloudfront_free.py`
  - Evidência: estado da subscription e vínculo da web ACL.
  - Estágio: Produção da #476.
- **SMK-18** Digest promovido igual ao registrado na task definition
  - Origem: tabela "Só documentados": Digest exato no ECR.
  - Verificação: [infrastructure-plan.md](infrastructure-plan.md),
    `scripts/promotion/promote_ecr.py`
  - Evidência: igualdade booleana de digests e `imageTagMutability=IMMUTABLE`.
  - Estágio: Produção da #469.
- **SMK-19** Nenhuma sentinela `pending-` em `shared_outputs`
  - Origem: #477, comentário 6869274407.
  - Verificação: `infra/opentofu/env/prod/variables.tf`,
    [infrastructure-plan.md](infrastructure-plan.md)
  - Evidência: plan final só de variável, sem diff de recurso, e zero valores `pending-`.
  - Estágio: Produção da #468.
- **SMK-20** `source_closure_sha256` trocado pelo valor do produtor
  - Origem: #477, comentário 6876585935 (raiz delivery do infra-ansible).
  - Verificação: `docs/superpowers/specs/2026-11-07-prod-delivery-external-ownership-amendment.md`
  - Evidência: valor diferente de 64 zeros, por mudança só de variável, antes do secret
    `CNESDATA_DELIVERY_GOVERNANCE` com dados reais.
  - Estágio: Produção da #469.
- **SMK-21** Adaptador real do registry com tag imutável
  - Origem: #475, comentário 6881970002 (pré-requisito do ensaio N+1 de rollback).
  - Verificação: `scripts/promotion/promote_ecr.py`,
    `tests/production/delivery/test_promote_ecr_tag.py`
  - Evidência: `put_manifest` com tag, `describe_image` por digest ou tag e readback da tag.
  - Estágio: Produção da #475.
- **SMK-22** Plan e apply exatos do produto
  - Origem: Estágio Produção da #468 (Task 3), contrato de dependência da #477.
  - Verificação: [infrastructure-plan.md](infrastructure-plan.md),
    `tests/production/delivery/test_plan_apply_workflows.py`
  - Evidência: aceite de Produção comentado na #468.
  - Estágio: Produção da #468.
- **SMK-23** Promoção byte a byte do processor e Phase A
  - Origem: Estágio Produção da #469 (Task 4), contrato de dependência da #477.
  - Verificação: `scripts/promotion/phase_a.py`, `tests/production/delivery/test_phase_a.py`
  - Evidência: aceite de Produção comentado na #469.
  - Estágio: Produção da #469.
- **SMK-24** API candidata e frontend atômico, com as Tasks 5, 6 e 7 reais
  - Origem: Estágio Produção da #473 (Task 8; ajuste A1 da EPIC), contrato de dependência da
    #477.
  - Verificação: `scripts/promotion/promote_api.py`, `scripts/promotion/publish_dashboard.py`
  - Evidência: aceite de Produção comentado na #473.
  - Estágio: Produção da #473.
- **SMK-25** Phase C separada de recovery e audit, com reabertura
  - Origem: Estágio Produção da #474 (Task 9), contrato de dependência da #477.
  - Verificação: `scripts/promotion/phase_c_recovery.py`, [recovery-drill.md](recovery-drill.md)
  - Evidência: aceite de Produção comentado na #474.
  - Estágio: Produção da #474.
- **SMK-26** Ensaio de rollback N+1 para N, drain e poda reais
  - Origem: Estágio Produção da #475 (Task 10), contrato de dependência da #477.
  - Verificação: [rollback.md](rollback.md), `scripts/promotion/rollback.py`
  - Evidência: aceite de Produção comentado na #475.
  - Estágio: Produção da #475.
- **SMK-27** CloudFront Free, corte e pós-promoção
  - Origem: Estágio Produção da #476 (Task 11, três partes), contrato de dependência da #477.
  - Verificação: [cloudfront-free.md](cloudfront-free.md), [demo-seed.md](demo-seed.md)
  - Evidência: aceite de Produção comentado na #476.
  - Estágio: Produção da #476.

## Schema de evidência

Um registro por item e por execução. `release_id` só existe na matriz de produção; o item tem
que pertencer à matriz do registro.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://cnesdata.com.br/schemas/operations-acceptance-evidence-v1.json",
  "title": "Evidência redigida de um item das matrizes de pré-produção e de produção",
  "oneOf": [
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["schema_version", "matrix", "item", "develop_sha", "status",
                   "observed_at", "evidence_sha256", "redacted"],
      "properties": {
        "schema_version": {"type": "integer", "const": 1},
        "matrix": {"type": "string", "const": "pre-production"},
        "item": {"type": "string", "enum": [
          "PRE-01", "PRE-02", "PRE-03", "PRE-04", "PRE-05", "PRE-06", "PRE-07", "PRE-08",
          "PRE-09", "PRE-10", "PRE-11", "PRE-12", "PRE-13", "PRE-14", "PRE-15", "PRE-16",
          "PRE-17", "PRE-18", "PRE-19", "PRE-20", "PRE-21", "PRE-22", "PRE-23", "PRE-24",
          "PRE-25"]},
        "develop_sha": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
        "status": {"type": "string", "enum": ["PASS", "FAIL", "BLOCKED"]},
        "observed_at": {"type": "string",
                        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"},
        "evidence_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        "redacted": {"type": "boolean", "const": true}
      }
    },
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["schema_version", "matrix", "item", "develop_sha", "release_id",
                   "status", "observed_at", "evidence_sha256", "redacted"],
      "properties": {
        "schema_version": {"type": "integer", "const": 1},
        "matrix": {"type": "string", "const": "production"},
        "item": {"type": "string", "enum": [
          "SMK-01", "SMK-02", "SMK-03", "SMK-04", "SMK-05", "SMK-06", "SMK-07", "SMK-08",
          "SMK-09", "SMK-10", "SMK-11", "SMK-12", "SMK-13", "SMK-14", "SMK-15", "SMK-16",
          "SMK-17", "SMK-18", "SMK-19", "SMK-20", "SMK-21", "SMK-22", "SMK-23", "SMK-24",
          "SMK-25", "SMK-26", "SMK-27"]},
        "develop_sha": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
        "release_id": {"type": "string",
                       "pattern": "^candidate-[0-9a-f]{40}-[1-9][0-9]*-[1-9][0-9]*$"},
        "status": {"type": "string", "enum": ["PASS", "FAIL", "BLOCKED"]},
        "observed_at": {"type": "string",
                        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"},
        "evidence_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        "redacted": {"type": "boolean", "const": true}
      }
    }
  ]
}
```

- `evidence_sha256` é o hash do artefato redigido guardado pelo estágio; o artefato nunca entra
  no repositório.
- `BLOCKED` registra um item que não pôde rodar, com o motivo no artefato. Só `PASS` conta para
  o aceite.
