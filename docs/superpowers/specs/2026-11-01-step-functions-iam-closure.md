# #385 — fechamento do Slice 3: API ativa, RunTask e rastreabilidade

**Status:** nota de fechamento do amendment
`2026-10-31-step-functions-iam-amendment.md` (v5), que continua sendo a autoridade de escopo.
Esta nota registra decisões tomadas na conclusão e mapeia os testes nomeados na issue.
Não executa apply, plan com provider real, deploy ou chamada AWS.

**Data:** 2026-11-01
**Base:** `develop@b2bcb698` (#438) sobre `7dd83bc5`.

## Decisões

- **Floor do OpenTofu.** `required_version = ">= 1.10.0"` permanece. O gate de floor é
  `fmt -check`, `init -backend=false` e `validate` em 1.10.0 (job `opentofu-floor` em
  `.github/workflows/infra-native.yml`). A suíte nativa `tests/production/infra` exige
  OpenTofu ≥ 1.11 (`show -json -config`, overrides com chave) e roda fixada em 1.13.2
  (job `opentofu-native`). O job é informativo e não entra no ruleset.
- **API ativa.** A policy `module.runtime_iam.aws_iam_role_policy.api[0]` mantém DDB/S3 desde
  o bootstrap e recebe SFN_M em M e SFN_E em E somente com
  `processor_active_unit_task_definition_arn != null`. Phase A com `retained` não vazio e
  `active = null` não recebe ações `states:*`.
- **RunTask.** `module.processing.aws_iam_role_policy.step_functions_ecs[0]` concede
  `ecs:RunTask` em `sort(setunion(candidate unit, retained))`. A presença do statement é
  decidida por `image_digest != null || length(retained) > 0`; no bootstrap o statement é
  omitido e o endereço permanece com DescribeTasks/StopTask em `*`.
- **Retained da promoção.** Na sequência stateful, a Phase B de route recebe
  `retained = [A, B]`. A revisão A entra como literal de manifest, porque `tofu test` só
  expõe `run.<nome>` do apply mais recente. O teste confere que o literal é igual ao A
  aplicado.
- **Produtores unknown.** Com `mock_provider`, o OpenTofu 1.13.2 preenche atributos
  computados com valores conhecidos já no plan, e a regra da #378 proíbe plan com provider
  real. Um ARN candidato unknown não é observável nativamente. O risco de count-on-unknown
  é coberto pela checagem de configuração: o `count` da policy ECS referencia somente
  `var.enable_step_functions_iam`, e a policy depende apenas dos locals de decisão e de
  recursos. A resolução após o apply mockado é verificada no estado da sequência stateful.

## Paths adicionais

- `tests/production/infra/test_step_functions_iam_inputs.py` (criado em `b5bd7698`) concentra
  os testes de transporte e validação dos inputs active/retained/runtime_grants. Ele existe
  porque `test_step_functions_iam.py` e `test_ecr_ecs.py` estão perto do limite de 500 linhas;
  não altera produto, validators ou fixtures.
- Esta nota de fechamento, em `docs/superpowers/specs/`.

## Riscos a verificar antes da ativação

- O `s3:ListBucket` de unit/recovery/API é condicionado por `s3:prefix`, como manda a matriz.
  O runtime distingue 404 de 403 em `get_object` de chave ausente
  (`cnes_infra/object_store/s3.py`). Se o S3 não considerar a permissão condicionada ao
  decidir 404 versus 403, a chave ausente retorna 403. O smoke real de #386–#389 precisa
  provar o 404 antes de ativar; mudar a condição exige novo amendment, porque
  `StringLikeIfExists` liberaria listagem sem prefixo.

## Rastreabilidade dos testes nomeados na issue

Os nomes da issue não existem literalmente; o amendment v5 redistribuiu os oráculos.

Prefixo comum dos arquivos: `tests/production/infra/`.

- `test_asl_renderizada_passa_no_validador_do_runtime`, `test_asl_passa_no_validador_de_producao`
  e `test_asl_sem_catch_distributed_map_ou_log_de_payload`:
  `test_step_functions_iam_asl.py::test_definition_production_tem_map_inline_e_retry_exato`.
  Produção é positivo de `validate_production_state_machine` e negativo esperado do validador
  genérico (polaridade do amendment); sem `Catch`, `Distributed` ou `LoggingConfiguration`.
- `test_run_task_so_nas_revisoes_exatas`: os três `test_run_task_*` de
  `test_processor_revision_contract.py` e
  `test_step_functions_iam_asl.py`, teste
  `test_run_task_acompanha_candidata_e_retained_na_sequencia_stateful`.
- `test_pass_role_exato_com_passed_to_service`:
  `test_step_functions_iam.py`, teste
  `test_role_step_functions_tem_trust_e_grants_common_exatos_no_bootstrap`.
- `test_api_sem_escrita_de_audit`:
  `test_step_functions_iam_runtime.py`, teste
  `test_runtime_e_api_recebem_grants_literais_exatos_no_bootstrap`
  (caso `api`) e
  `test_step_functions_iam_runtime.py`, teste
  `test_api_ativa_recebe_quatro_acoes_exatas_e_preserva_ddb_s3_e_role_externa`.
- `test_runtime_iam_nao_referencia_processing`:
  `test_step_functions_iam.py::test_raiz_nao_passa_saida_de_processing_para_runtime_iam` e os
  testes de runtime-iam isolado.
- `test_plan_agenda_state_machine_e_politicas`: estágios da sequência em
  `test_step_functions_iam_asl.py::test_definition_production_tem_map_inline_e_retry_exato` e
  `test_ecr_ecs.py::test_root_anexa_inventario_exato_de_policies_as_roles`.

## Fora de escopo

Apply, ativação, scheduler, deploy e chamadas AWS permanecem bloqueados pelos gates
operacionais de #386–#389.
