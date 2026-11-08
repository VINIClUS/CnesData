# Amendment de escopo #384 — provisionamento offline

Autoridade: issue #384, Task 7/8 do plano de infraestrutura e plano aprovado nesta
conversa. Base revalidada após a #436: `e9d012b635b741d03649a28c3247eae5dff82d9e`.
Este amendment organiza a integração; não dispensa critérios de aceite da issue.

## Entrega e paths

Integrar somente registry, base IAM, log group e três famílias ECS, com seus testes
nativos. Preservar a composição static-web/ACM já integrada pela #435.
Além dos paths originais da #384, autorizar os outputs e variables da raiz, helpers
`_composition_contracts.py`, `_processing_*.py`, `test_processing_*.py`, conftest e
ajustes de compatibilidade em test_composition/test_static_web_composition.
Um helper compartilhado de defaults computed AWS pode ser extraído, caso necessário,
sem substituir campos configurados ou afrouxar oráculos. Documentar esse amendment
em `docs/superpowers/specs/2026-10-31-processor-provisioning-amendment.md`.

Autorizar `_aws_mock_defaults.py`, `_fresh_process.py` e `test_fresh_process.py`
como helpers e controles de teste. O isolamento dos dois probes de bootstrap usa
um subprocesso finito por node, sem retry, mantendo as assertions originais.
O parent exige o testcase exato e um marcador emitido após todas as assertions;
JUnit verde sem execução do corpo é recusado.

Os três probes de runtime também usam processo `spawn` finito, recebendo planos
nativos já renderizados e preservando todas as assertions e guards de I/O.
O parent exige conclusão nova por modo com duas observações; timeout e falha do
child são recusados. Não redefinir a sessão global de boto3 para esconder ordem
de execução. Os imports dos helpers usam `tests.production.infra`, permitindo o
comando pytest documentado com `importlib` e somente a raiz em `PYTHONPATH`.
Um smoke `--collect-only` cobre os três módulos afetados sem fixtures ou provider.

Excluir do diff os 24 novos paths processor_container*, processor_gate*,
r4_controller* e respectivos testes: não são necessários ao contrato desta issue.
Preservar esses arquivos e seus 37 commits nas branches original e rebaseada, sem
reexecutar controllers ou descartar o trabalho histórico.

## Identidade, retenção e evidência operacional

O nome físico planejado da máquina é `cnesdata-prod`, na mesma conta do produto e
em us-east-2. A expressão determinística do ARN é identidade planejada, não prova de
existência AWS. Ela evita o ciclo máquina -> revisões ECS -> máquina. Em #385, criar
exatamente esse nome e testar a ligação entre ARN real produzido e ambiente das tasks;
não depender do output da máquina na definição da task. Não adicionar novo handoff
prévio que reintroduza esse ciclo. Nenhuma task ganha policy ou inicia trabalho em #384.

`image_digest=null` significa zero registros de task. Um digest válido só identifica
conteúdo; sua forma não prova provenance. Fixtures A/B são sintéticas e usadas somente
em provider mocks. Antes de promoção/apply, delivery deve verificar source SHA,
assinatura/checksums, repositório e bytes idênticos entre artefato e digest ECR, conforme
Phase A/activation manifest já especificados no plano de delivery e operações.

Manter lifecycle untagged sete dias e não criar expiração automática tagged que
possa remover current/prior/in-flight/revisions. O oráculo processor_retention já
integrado define cinco digests/30 dias e preserva referências protegidas. O adapter de
inventário, recusa stale/incompleto e pruning pertencem à Task 10 do plano de delivery
(scripts/promotion/prune_revisions.py); não criar outro controlador nesta issue.
A ausência desse adapter continua pendência de aceite operacional da #384.

Omitir ephemeral_storage usa o default Fargate20GiB; configuração customizada
21–200GiB está fora deste ciclo. Mounts /tmp,/logs,/data readOnly=false são evidência
de configuração. Ownership e escrita como app no Fargate exigem ensaio do digest exato;
Docker local não substitui essa prova e Dockerfile permanece fora do escopo.

## Ciclo de execução

1. Observar baseline offline do HEAD recuperado; erros de setup não são RED funcional.
2. Corrigir somente compatibilidade da mesma composição, com auditoria independente
   dos testes/helpers antes da correção; preservar assertions de segurança e produtores.
3. Demonstrar sensibilidade IAM/ECS/ECR por mutações finitas relevantes, sem framework
   genérico e sem novas chamadas AWS/Docker. Reusar testes comportamentais úteis.
4. Ruff, Pyright, toda suite infra, fmt/validate/Trivy e checks CI obrigatórios.
5. Auditoria independente do SHA final antes de merge. Rebase invalida aprovação.

#384 continua aberta até resolver retenção/inventário, promoção e escrita Fargate.
Não criar service, state machine, execução, schedule, deploy, apply ou custos neste ciclo.
