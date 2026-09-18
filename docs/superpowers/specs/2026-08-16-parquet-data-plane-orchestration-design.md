# CnesData — Data Plane Parquet e Orquestração — V2

**Status:** aprovado para planejamento

**Versão:** V2

**Data:** 2026-08-16

**Base de código:** `develop`

## 1. Objetivo

Redesenhar o CnesData como núcleo local-first com dois perfis operacionais:

- `local`: instalação single-tenant por município, sem dependência de cloud;
- `aws`: plataforma multi-tenant com serviços gerenciados da AWS.

O desenho remove PostgreSQL, MinIO, Keycloak e BigQuery da arquitetura-alvo.
Parquet versionado armazena dados raw, normalizados e reconciliados. JSON
materializado atende o dashboard. SQLite e DynamoDB implementam o mesmo contrato
de control plane nos perfis local e AWS, respectivamente.

## 2. Decisões centrais

| Tema | Decisão |
|---|---|
| Perfil local | Uma instalação e um diretório de dados por município |
| Control plane local | SQLite em `state/cnesdata.sqlite3` |
| Control plane AWS | DynamoDB multi-tenant; sem PostgreSQL/RDS |
| Modelo canônico do control plane | `Tenant`, `Membership`, `Agent`, `Job`, `Run`, `RunUnit`, `DatasetVersion`, `DatasetPointer`, `AccessRequest` |
| Primitivas internas do control plane | `IdempotencyRecord` e `OutboxEvent` |
| Object store local | Filesystem |
| Object store AWS | Amazon S3 |
| Dados canônicos | Parquet versionado e imutável por execução |
| Serving do dashboard | JSON materializado por `run_id` |
| Analytics local | DuckDB embutido, não um serviço |
| Analytics AWS | Athena opcional, fora do request path normal |
| Formato de tabela inicial | Plain Parquet com manifestos e publicação atômica |
| Iceberg | Adiado até existir necessidade de mutação concorrente |
| Semântica de execução | At-least-once com idempotência e fencing |
| TTL | Apenas garbage collection; expiração lógica é validada pela aplicação |
| Índices DynamoDB | GSIs servem descoberta/listagem; autorização e commits usam chave base + conditional write |
| SQS | Não é dependência mínima; se adotado, é transporte/wakeup, nunca fonte canônica de estado |
| Reconciliação | Executada nos processadores centrais |
| Delta no edge | Otimização de transporte, nunca única fonte recuperável |
| Autenticação local | `AUTH_MODE=local` por default; OIDC opcional |
| Autenticação AWS | OIDC genérico; Cognito é apenas um provider opcional |
| Fonte nacional | Adapter DATASUS aprovado, sem runtime ou staging no BigQuery |

### 2.1 Perfis de autenticação

No perfil `local`, `AUTH_MODE=local` mantém usuários e hashes de senha no SQLite.
`AUTH_MODE=oidc` permite SSO sem alterar o modelo de dados. O `tenant_id` é fixado
pela instalação e nunca aceito de parâmetros ou claims enviados pelo navegador.

No perfil `aws`, `AUTH_MODE=oidc` valida tokens de um issuer configurado e resolve o
tenant por mapeamento server-side. Amazon Cognito pode ser oferecido como preset de
deployment, mas não aparece nos contratos de domínio e não é uma dependência do
produto. Keycloak não integra a arquitetura-alvo.

### 2.2 Ingestão do CNES nacional

`CNES_NACIONAL` entra por um adapter central que consulta a distribuição oficial do
DATASUS, incluindo o adapter web já existente quando compatível com o source
contract. Outros adapters aprovados podem ser adicionados, mas todos devem produzir
o mesmo `RawManifest` e os mesmos objetos raw imutáveis usados pelas fontes de edge.

BigQuery não é fonte canônica, staging area nem runtime dessa ingestão. Credenciais,
datasets, jobs e configuração GCP são removidos após o cutover e a validação do novo
adapter.

### 2.3 Modelo canônico do control plane

O contrato de domínio do control plane é explícito e independe do armazenamento físico:

| Entidade | Responsabilidade |
|---|---|
| `Tenant` | configuração do município e políticas server-side |
| `Membership` | vínculo de usuário, tenant e papel; base de autorização |
| `Agent` | registro, identidade/certificado, versão, estado e `last_seen_at` |
| `Job` | trabalho destinado ao Edge Agent, incluindo lease, tentativas e fencing |
| `Run` | execução central de normalização/reconciliação/publicação |
| `RunUnit` | unidade determinística de processamento pertencente a um `Run` |
| `DatasetVersion` | metadados imutáveis de uma versão publicada |
| `DatasetPointer` | alias mutável, como `CURRENT`, que aponta para uma versão publicada |
| `AccessRequest` | solicitação e decisão de concessão de acesso |
| `IdempotencyRecord` | primitiva interna para deduplicar operações por escopo e payload |
| `OutboxEvent` | primitiva interna para entrega confiável de eventos de domínio/auditoria |

`Job`, `Run` e `RunUnit` não são sinônimos. `Job` coordena trabalho solicitado a um
Edge Agent. `Run` representa uma execução central de data processing. `RunUnit`
representa o fan-out interno de um `Run`.

`IdempotencyRecord` e `OutboxEvent` não são entidades de negócio expostas ao
frontend. São primitivas de consistência do adapter de control plane.

`DatasetVersion` nunca muda depois de publicada. A ativação de uma nova versão é
feita alterando condicionalmente o `DatasetPointer`; histórico não é sobrescrito.

## 3. Não objetivos

- Não executar regras de negócio ou reconciliação no Edge Agent.
- Não consultar Athena em cada interação do dashboard.
- Não conceder ao frontend acesso direto aos Parquets raw ou normalizados.
- Não oferecer uma instalação local multi-tenant no primeiro release.
- Não prometer exactly-once entre agente, object store e processadores.
- Não introduzir Iceberg, Spark, Redis, Kafka ou Kubernetes como dependências mínimas.
- Não manter compatibilidade runtime com MinIO, PostgreSQL ou BigQuery após o cutover.

## 4. Fronteira do Edge Agent

### 4.1 Responsabilidades permitidas

O agente executa somente transformações necessárias para ler e transportar a fonte
com fidelidade:

- descobrir caminhos e configurações das fontes locais;
- acessar Firebird e DBF próximos à origem;
- executar a extração específica de cada produto DATASUS;
- preservar o merge técnico das três consultas CNES exigido pela fonte;
- converter encoding legado para UTF-8 sem alterar significado;
- tipar valores para o source contract versionado;
- escrever Parquet comprimido e particionado;
- calcular `row_count`, tamanho, SHA-256 e manifesto;
- manter outbox local e retries;
- detectar deltas para reduzir transmissão;
- emitir diagnósticos e evidências de proveniência.

### 4.2 Responsabilidades proibidas

O agente não executa:

- precedência entre CNES local, nacional ou outras fontes;
- joins de negócio entre source types;
- regras de auditoria ou classificação de divergências;
- derivação de indicadores do dashboard;
- mascaramento condicionado por plano ou assinatura;
- publicação de datasets de serving;
- decisões de acesso, quota ou faturamento.

Transformações que alteram significado ou precisam ser reproduzidas historicamente
pertencem ao processador central.

## 5. Contrato raw e delta chain

Cada upload possui um `RawManifest` imutável com, no mínimo:

```json
{
  "manifest_version": 1,
  "tenant_id": "354130",
  "source_type": "CNES_LOCAL",
  "file_subtype": "CNES_VINCULO",
  "competencia": "2026-07",
  "agent_id": "agent-01",
  "agent_version": "1.0.0",
  "schema_version": "cnes-local-v1",
  "snapshot_mode": "FULL",
  "snapshot_id": "01J...",
  "base_snapshot_id": null,
  "sequence": 1,
  "previous_manifest_sha256": null,
  "object_sha256": "...",
  "row_count": 1000,
  "object_key": "..."
}
```

Um delta usa `snapshot_mode=DELTA`, informa `base_snapshot_id`, incrementa
`sequence` e encadeia `previous_manifest_sha256`.

O servidor rejeita o delta e solicita full snapshot quando:

- houver gap de sequência;
- o snapshot base não for conhecido ou não estiver publicado;
- o hash chain divergir;
- o source contract mudar de forma incompatível;
- a versão do agente exigir ressincronização;
- a base tiver mais de sete dias;
- a cadeia atingir trinta deltas.

Os limites iniciais são defaults de deployment e nunca podem ser desabilitados.
Uma `PlanVersion` pode exigir full snapshots mais frequentes, mas não menos seguros.
O modelo e a aplicação de planos estão definidos na especificação de
[Stripe Billing, Entitlements e Revogação](2026-08-16-stripe-billing-entitlements-design.md).

## 6. Camadas de dados

### 6.1 Layout lógico

```text
raw/<tenant>/<source>/<competencia>/<snapshot_id>/
normalized/<tenant>/<source>/<competencia>/<run_id>/
reconciliation/<tenant>/<competencia>/<run_id>/
serving/<tenant>/<run_id>/
audit/<tenant>/<yyyy>/<mm>/<dd>/
tmp/<tenant>/<run_id>/<unit_id>/<attempt>/
```

No filesystem, o prefixo parte do data directory da instalação. No S3, parte de
um bucket privado por ambiente. O layout lógico e os manifestos são idênticos.

### 6.2 Raw

- Imutável e fiel ao source contract.
- Sempre preserva proveniência e hashes.
- Pode conter full snapshots e deltas.
- Nunca é entregue diretamente ao navegador.

### 6.3 Normalized

- Padroniza chaves, datas, códigos e nullability.
- Resolve deltas contra uma base full conhecida.
- Continua separado por fonte para preservar proveniência.
- Pode ser refeito integralmente a partir de raw.

### 6.4 Reconciliation

- Executa joins, precedência e regras entre fontes.
- Publica Parquets imutáveis por `run_id`.
- Inclui resultados, divergências, estatísticas e evidências.
- Nunca sobrescreve um run anterior.

### 6.5 Serving

- Contém somente JSONs pequenos e orientados às telas.
- Exclui campos pessoais que a tela não precisa.
- Declara `run_id`, `generated_at` e schema version.
- É publicado junto ao mesmo reconciliation run.

## 7. Processadores centrais

O data processor possui três estágios independentes:

1. `NormalizeSource`: source Parquet para normalized Parquet;
2. `ReconcileCompetencia`: normalized inputs para reconciliation Parquet;
3. `MaterializeServing`: reconciliation Parquet para serving JSON.

Polars é a primeira opção para transformação determinística. DuckDB pode ser usado
para joins analíticos e consultas locais sem operar como daemon. O mesmo container
de processamento deve executar no host local e em ECS Fargate.

## 8. Ports e adapters

O domínio expõe contratos pequenos, sem SQL, expressões DynamoDB ou paths S3:

| Port | Responsabilidade |
|---|---|
| `ControlPlanePort` | tenants, memberships, agents, jobs, runs, units, leases, fences, dataset versions/pointers, access requests, idempotência e outbox |
| `ObjectStorePort` | leitura, escrita, stat e publicação de objetos |
| `ProcessorExecutorPort` | iniciar e cancelar unidades de processamento |
| `ServingAccessPort` | resolver versão ativa e autorizar serving |
| `AuditSinkPort` | registrar eventos de domínio append-only |

Adapters aprovados:

| Perfil | Control plane | Object store | Executor | Audit sink |
|---|---|---|---|---|
| local | SQLite | Filesystem | Worker pool | JSONL/Parquet local |
| aws | DynamoDB | S3 | Step Functions + ECS | S3 Object Lock |

Uma contract test suite deve ser executada contra SQLite e DynamoDB Local. O uso
de DynamoDB Local é exclusivo de teste e desenvolvimento, não de produção local.

### 8.1 Invariantes do `ControlPlanePort`

Os adapters SQLite e DynamoDB podem ter schemas físicos diferentes, mas devem
preservar as mesmas invariantes:

- autorização por membership usa lookup direto de `tenant_id + user_id`;
- registro revogado de agente nunca pode receber ou concluir `Job`;
- claim de `Job` e `RunUnit` é conditional e incrementa `fencing_token`;
- retry não cria uma nova identidade lógica para o mesmo trabalho;
- `DatasetVersion` é imutável;
- `DatasetPointer` só muda por compare-and-swap contra a versão esperada;
- uma chave de idempotência com o mesmo request hash retorna o mesmo resultado;
- a mesma chave com request hash diferente gera conflito;
- expiração lógica usa `expires_at`; TTL físico nunca decide se uma operação é válida;
- mutações auditáveis gravam um `OutboxEvent` na mesma transação do estado canônico.

SQLite usa transações (`BEGIN IMMEDIATE` quando houver disputa de claim) e constraints
únicas. DynamoDB usa `PutItem`/`UpdateItem` condicionais para invariantes de um item e
`TransactWriteItems` apenas quando a invariância envolver múltiplos itens.

### 8.2 Baseline físico do DynamoDB

O perfil AWS inicia com uma única tabela de control plane por ambiente, modelada por
access patterns, e não por uma tabela por classe. O layout exato pode evoluir sem
alterar o domínio, mas o baseline é:

```text
Tenant
  PK = TENANT#<tenant_id>
  SK = META

Membership
  PK = TENANT#<tenant_id>
  SK = MEMBER#<user_id>
  GSI1PK = USER#<user_id>
  GSI1SK = TENANT#<tenant_id>

Agent
  PK = TENANT#<tenant_id>
  SK = AGENT#<agent_id>

Job
  PK = TENANT#<tenant_id>
  SK = JOB#<job_id>

Run
  PK = TENANT#<tenant_id>
  SK = RUN#<run_id>

RunUnit
  PK = TENANT#<tenant_id>#RUN#<run_id>
  SK = UNIT#<unit_id>

DatasetVersion
  PK = TENANT#<tenant_id>#DATASET#<dataset_name>
  SK = VERSION#<version_id>

DatasetPointer
  PK = TENANT#<tenant_id>#DATASET#<dataset_name>
  SK = POINTER#CURRENT

AccessRequest
  PK = TENANT#<tenant_id>
  SK = ACCESS_REQUEST#<request_id>

IdempotencyRecord
  PK = TENANT#<tenant_id>#IDEMPOTENCY#<scope>
  SK = KEY#<key>

OutboxEvent
  PK = TENANT#<tenant_id>#OUTBOX#<shard>
  SK = <created_at>#<event_id>
```

GSIs adicionais só são introduzidos para access patterns medidos, por exemplo listar
tenants de um usuário ou descobrir jobs candidatos por agente/estado. Um resultado
obtido por GSI é apenas candidato: claim, autorização, revogação e publicação sempre
revalidam o item na tabela base com condição atômica. Nenhuma propriedade de
correção depende de leitura strongly consistent em GSI.

O heartbeat do `Agent` atualiza `last_seen_at` e metadados operacionais, mas não usa
TTL para apagar o registro. Estado `ONLINE/OFFLINE` é derivado do tempo desde o último
heartbeat; revogação é persistente.

## 9. Estado de jobs, runs e unidades

### 9.1 Job do Edge Agent

`Job` representa trabalho remoto solicitado a um agente, por exemplo extrair uma
fonte/competência e entregar seu `RawManifest`. O agente obtém jobs pela API; acesso
direto do Edge Agent ao DynamoDB não faz parte do contrato.

Estados:

```text
PENDING
LEASED
SUCCEEDED
FAILED_RETRYABLE
FAILED_FINAL
CANCEL_REQUESTED
CANCELED
```

Cada job possui, no mínimo:

- `job_id` determinístico ou associado a uma idempotency key;
- `tenant_id`;
- `agent_id` ou seletor de agente;
- `source_type`, `file_subtype` e `competencia`;
- `requested_snapshot_mode`;
- `attempt`;
- `fencing_token` monotônico;
- `lease_owner`;
- `lease_until`;
- `result_manifest_id`;
- `error_code` sanitizado.

Descoberta de jobs pode usar índice eventualmente consistente. O claim sempre é feito
por chave canônica e condição atômica sobre estado, lease e fence. Quando a validade do
claim depender do estado atual do `Agent`, o adapter usa a mesma transação para
`ConditionCheck` do agente e atualização do `Job`, ou uma representação equivalente que
preserve a mesma invariância.

SQS não é necessário para esse protocolo. Se vier a ser introduzido para wake-up ou
distribuição entre componentes server-side, uma mensagem duplicada ou fora de ordem
não pode alterar a correção: o `Job` no control plane continua sendo a fonte de verdade.

### 9.2 Run

Estados:

```text
PLANNED
WAITING_INPUTS
PROCESSING
PUBLISHING
PUBLISHED
PUBLISHED_DEGRADED
FAILED
CANCEL_REQUESTED
CANCELED
```

### 9.3 Run unit

Estados:

```text
PENDING
LEASED
SUCCEEDED
FAILED_RETRYABLE
FAILED_FINAL
CANCELED
```

Cada unit possui:

- `run_id`;
- `unit_id` determinístico;
- `input_manifest_ids`;
- `attempt`;
- `fencing_token` monotônico;
- `lease_owner`;
- `lease_until`;
- `output_manifest_id`;
- `error_code` sanitizado.

## 10. Fan-out

A unidade de paralelismo é:

```text
tenant + competencia + source_type + file_subtype ou partition
```

Não existe fan-out por linha.

No perfil local, SQLite agenda units e um worker pool limitado as executa. No AWS,
uma Step Functions Standard usa Inline Map com `MaxConcurrency` explícito e inicia
tasks ECS Fargate. Distributed Map só é habilitado quando uma medição comprovar que
Inline Map ou o histórico da execução são insuficientes.

A concorrência efetiva é:

```text
min(units pendentes, limite do deployment, quota da PlanVersion)
```

Payloads de workflow carregam somente IDs e object keys. Dados permanecem no
object store.

## 11. Fan-in

O planner cria uma lista explícita de dependências obrigatórias e opcionais. A
reconciliação começa somente depois que todas as dependências obrigatórias têm
manifestos válidos e units `SUCCEEDED`.

Falha em fonte obrigatória resulta em `FAILED`. Ausência de fonte opcional pode
resultar em `PUBLISHED_DEGRADED`, com `missing_sources` visível no manifesto e no
dashboard. O sistema nunca publica silenciosamente um resultado incompleto como
completo.

## 12. Leases, fencing e idempotência

Cada claim incrementa `fencing_token` atomicamente. SQLite usa transação imediata;
DynamoDB usa conditional update. O worker escreve somente em:

```text
tmp/<tenant>/<run_id>/<unit_id>/<attempt>/
```

O commit da unit aceita o output apenas se:

- o run não estiver cancelado;
- o unit continuar `LEASED` pelo mesmo owner;
- o `fencing_token` recebido ainda for o atual;
- o output manifest passar validação de schema e hash.

Um worker atrasado pode terminar o cálculo, mas não consegue publicar. Retries
reusam o mesmo `unit_id` e criam novo `attempt`. Writes são idempotentes por
`run_id + unit_id + attempt`.

`IdempotencyRecord` contém, no mínimo:

```text
tenant_id
scope
key
request_hash
status
resource_id
created_at
expires_at
```

A primeira operação cria o registro somente se não existir um registro lógico válido.
Repetição com o mesmo `request_hash` retorna o mesmo `resource_id`/resultado.
Reutilização da mesma chave com payload diferente é conflito. Se `expires_at <= now`,
o adapter pode substituir/reclamar o registro por condição atômica mesmo que o item
ainda exista fisicamente.

No DynamoDB, TTL é configurado apenas para garbage collection. Como a exclusão física
de itens expirados é assíncrona, nenhuma decisão de claim, idempotência, autorização ou
lease depende de o item ter desaparecido da tabela.

Para transações DynamoDB, `ClientRequestToken` pode proteger retries imediatos da mesma
chamada, mas não substitui `IdempotencyRecord`, cuja janela é definida pelo produto.

## 13. Publicação atômica

A atomicidade observável pelo produto não depende de rename atômico no object store.
Ela depende de objetos finais imutáveis já existentes e de um único compare-and-swap
no `DatasetPointer`.

O publisher:

1. valida todos os output manifests;
2. valida invariantes de reconciliação e serving;
3. promove os objetos para chaves finais imutáveis do `run_id`; no filesystem pode usar
   rename no mesmo filesystem, enquanto no S3 pode escrever diretamente a chave final
   ou copiar do prefixo temporário;
4. verifica os objetos finais e grava o `RunManifest` imutável;
5. prepara o `DatasetVersion` imutável referente ao novo `run_id`;
6. em uma transação do control plane, cria o `DatasetVersion`, muda o
   `DatasetPointer` de `old_version` para `new_version` por compare-and-swap e grava o
   `OutboxEvent` `reconciliation.published`;
7. o outbox dispatcher entrega o evento ao `AuditSinkPort` e marca a entrega.

No SQLite, o passo 6 ocorre em uma transação local. No DynamoDB, usa
`TransactWriteItems` para a versão/pointer/outbox quando esses itens precisarem mudar
juntos. Se a condição do pointer falhar porque outro publisher venceu, nenhum pointer
é sobrescrito silenciosamente.

O frontend resolve somente o `DatasetPointer` ativo. Falha antes do commit do passo 6
deixa a versão anterior ativa. Falha depois do commit não reverte o pointer; o
`OutboxEvent` pendente garante retry da auditoria. Objetos temporários e objetos finais
que nunca foram ativados podem ser removidos por lifecycle/retenção após a janela de
diagnóstico definida.

## 14. Frontend e analytics

O frontend consome serving JSON por meio da API/BFF. No AWS, o bucket continua
privado e a API emite URL ou cookie assinado de curta duração após autorização.

Athena serve análises ad hoc, investigação de auditoria e consultas que não fazem
parte da navegação normal. O frontend nunca recebe credenciais AWS para consultar
Athena ou S3 raw diretamente.

## 15. Segurança e isolamento

### 15.1 Local

- Uma instalação representa exatamente um município.
- O `tenant_id` vem da configuração do servidor, não do navegador.
- Data directory e `state/cnesdata.sqlite3` pertencem ao usuário do serviço.
- A API recusa requests com tenant divergente.

### 15.2 AWS

- Todas as chaves DynamoDB e prefixes S3 incluem `tenant_id`.
- Membership autenticada é validada por chave canônica antes de resolver o tenant; GSI não autoriza requests.
- Roles de execução usam least privilege por prefixo e operação.
- Lake Formation é opcional para acesso analítico compartilhado.
- Serving, raw e audit usam buckets ou prefixes com policies distintas.

## 16. Logs e auditoria

Logs operacionais são JSON estruturado em stdout. O perfil local pode rotacionar
arquivos. O perfil AWS envia stdout ao CloudWatch Logs.

Auditoria de domínio é separada de logs operacionais. Eventos incluem:

- manifesto recebido ou rejeitado;
- full snapshot solicitado;
- unit claimed, fenced, retried ou failed;
- run publicado, degradado, cancelado ou invalidado;
- dataset pointer alterado;
- acesso de serving concedido ou negado.

No AWS, o audit writer grava S3 versionado com Object Lock. Eventos auditáveis
críticos são persistidos primeiro como `OutboxEvent` junto da mutação canônica e depois
entregues ao sink com retry idempotente. DynamoDB Streams pode disparar ou acelerar o
dispatcher do outbox e capturar mudanças técnicas do control plane, mas não substitui
os eventos de negócio explícitos.

## 17. Recuperação e falhas

| Falha | Comportamento |
|---|---|
| Upload interrompido | Outbox retenta com mesma idempotency key |
| Delta com gap | Rejeitar e solicitar full snapshot |
| Worker perde lease | Novo claim incrementa fence; worker antigo não publica |
| Unit retryable falha | Retry limitado com backoff e jitter |
| Unit final falha | Run falha ou degrada conforme dependência |
| Publisher falha antes do pointer swap | `DatasetPointer` anterior permanece ativo |
| SQLite corrompido | Restaurar backup do control plane; somente estado derivável de datasets/runs pode ser reconstruído de manifestos. Usuários, memberships, agents e decisões de acesso exigem backup/seed próprio |
| DynamoDB indisponível | Não iniciar claims, conceder acesso novo nem publicar; preservar temporários e retentar sem assumir expiração por TTL |
| Auditoria indisponível após commit | `OutboxEvent` permanece pendente; dispatcher retenta sem repetir a mutação de domínio |
| Serving ausente | API retorna indisponibilidade sem cair para versão não autorizada |

## 18. Estratégia de testes

- Contract tests idênticos para SQLite e DynamoDB.
- Contract tests idênticos para filesystem e S3.
- Property tests para transições de estado e idempotência.
- Race tests com dois workers disputando o mesmo unit.
- Testes que comprovem que fence antigo não publica.
- Testes de gap, reorder e duplicação de deltas.
- Golden tests de reconciliação raw para normalized, reconciliation e serving.
- Shadow runs comparando a saída nova com o Gold PostgreSQL atual.
- Chaos tests interrompendo worker entre escrita e publicação.
- Security tests para cross-tenant keys, prefixes e serving access.
- Testes de idempotência com item expirado ainda fisicamente presente por TTL.
- Testes em que GSI retorna job/membership stale, comprovando revalidação na chave base.
- Race test com dois publishers tentando trocar o mesmo `DatasetPointer`.
- Crash test após pointer commit e antes do audit sink, comprovando replay via outbox.
- Backup/restore local incluindo usuários, memberships, agents e access decisions.

## 19. Migração

1. Reconciliar a divergência entre `main` e `develop` antes de mudanças funcionais.
2. Congelar source contracts e manifests existentes como fixtures de compatibilidade.
3. Introduzir ports e o modelo canônico do control plane sem alterar o comportamento atual.
4. Implementar filesystem + SQLite e S3 + DynamoDB atrás dos ports, incluindo idempotency e outbox.
5. Completar normalize, reconcile e materialize com output versionado.
6. Rodar shadow processing contra o Gold PostgreSQL por competências históricas.
7. Mudar o frontend para serving JSON e `DatasetPointer`, migrando memberships/access state necessários.
8. Cortar novos writes em PostgreSQL e MinIO.
9. Exportar o histórico necessário para Parquet versionado.
10. Remover migrations, adapters e documentação de PostgreSQL, MinIO e BigQuery.
11. Remover Keycloak do perfil local e manter OIDC como integração opcional.

O cutover exige equivalência dos principais KPIs, divergências e contagens para as
fixtures aprovadas. Diferenças devem ser explicadas por mudança de contrato ou regra,
nunca aceitas apenas por proximidade estatística.

## 20. Critérios de aceitação

- Perfil local inicia sem PostgreSQL, MinIO, Keycloak ou acesso à AWS.
- Perfil local opera um único tenant e persiste restart em SQLite/filesystem.
- Perfil AWS usa DynamoDB/S3 sem dependência de PostgreSQL/RDS.
- Edge Agent não contém reconciliação ou regras de negócio.
- Gap de delta solicita full snapshot e não corrompe o dataset ativo.
- Fan-out respeita limites de deployment e plano.
- Fence antigo não consegue publicar output.
- Falha de publicação antes do compare-and-swap preserva o `DatasetPointer` anterior.
- Frontend consome somente serving JSON autorizado.
- Raw, reconciliation e audit permanecem reprocessáveis e rastreáveis por hash.
- Claims, autorização e pointer swap não dependem de GSI eventualmente consistente.
- TTL não participa de correção de lease/idempotência; item expirado ainda presente é tratado corretamente.
- `Job`, `Run` e `RunUnit` possuem semânticas distintas e contract tests equivalentes em SQLite/DynamoDB.
- Publicação crítica grava pointer e outbox de forma atômica no control plane.
- Falha do audit sink após commit não perde o evento publicado.

## 21. Referências externas

- AWS Step Functions Map:
  <https://docs.aws.amazon.com/step-functions/latest/dg/state-map.html>
- AWS DynamoDB transactions:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html>
- AWS DynamoDB TTL:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html>
- AWS DynamoDB read consistency:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html>
- AWS DynamoDB data modeling foundations:
  <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/data-modeling-foundations.html>
- Amazon SQS at-least-once delivery:
  <https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues-at-least-once-delivery.html>
- AWS Athena ACID transactions:
  <https://docs.aws.amazon.com/athena/latest/ug/acid-transactions.html>
- AWS S3 Object Lock:
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html>
