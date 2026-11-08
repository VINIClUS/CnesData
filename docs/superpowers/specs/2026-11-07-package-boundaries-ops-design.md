# Fronteiras dos pacotes e operação

## Objetivo e decisões aprovadas

Reorganizar o monorepo em oito bibliotecas, preservando os seis apps, os perfis
local/aws e todos os contratos externos. Migrar todos os consumidores internos
no mesmo PR, sem fachada para cnes_infra. Não alterar os repositórios externos
de infraestrutura, implantar serviços ou executar Terraform.

## Donos e dependências

| Pacote | Responsabilidade | Dependências locais permitidas |
|---|---|---|
| cnes_contracts | DTOs, manifests, modelos públicos, colunas | nenhuma |
| cnes_domain | regras, entidades, ports, processamento e tenant | contracts |
| cnes_observability | logging, OTel, tracer, contador de queries | nenhuma |
| cnes_data | storage, RLS, ingestão, object stores, Alembic, helpers DynamoDB | contracts, domain, observability |
| cnes_identity | JWT/OIDC, credenciais locais, device flow, mTLS | contracts, domain, data |
| cnes_governance | eventos de auditoria e sinks local/Object Lock | domain, data |
| cnes_control_plane | persistência de controle, billing, quotas, execução | contracts, domain, data, observability |
| cnes_runtime | configuração compartilhada e composição AWS | demais capacidades |

Bibliotecas não importam apps; o grafo entre bibliotecas é acíclico. Core não lê
ambiente nem configura I/O/telemetria. Pandera permanece no Core. Ports genéricos
ficam no Core; o protocolo legado dependente de Engine fica em Data.

Billing e control plane permanecem juntos para preservar transações de quota,
criação de tenants e revogação. Sec pertence aos componentes que aplicam seus
controles: Identity, RLS em Data, billing secrets e scanners CI. Reliability fica
nas primitivas do Core e na recuperação/leases/outbox de cada componente.
Governance não inclui as regras externas de auditoria de saúde.

## Migração

Extrair todos os módulos de cnes_infra e remover o pacote ao final. O contador
compartilhado em Observability elimina o import de middleware da API pelo storage.
Preservar X-Query-Count e isolamento por request/threadpool. Alembic passa a usar
cnes_data:alembic, sem modificar revisions existentes nem sua ordem; configuração
explícita precede DB_URL. Runtime mantém nomes/defaults/lazy semantics das envs.

Atualizar consumidores, imports dinâmicos, monkeypatches, fixtures, manifests,
extras, lockfile, tipos, descoberta de testes, cobertura, Sonar, Dockerfiles e CI.
Remover referências executáveis ao app Python dump_agent, inexistente.

## Ops

| Origem | Destino |
|---|---|
| deploy/dev, deploy/prod | ops/deploy/vps/dev, ops/deploy/vps/prod |
| deploy/aws/raw | infra/aws/raw-legacy |
| charts/web-dashboard | infra/legacy/helm/web-dashboard |
| docker-compose.keycloak | ops/identity/keycloak |
| scripts/edge_audit | ops/scripts/edge_audit |
| scripts/local_backup.py e teste | ops/backup |

Compose fica na raiz; Dockerfiles nos apps; workflows em .github; ferramentas
de desenvolvimento em scripts. Runbooks continuam em docs/runbooks, indexados
por ops/README.md. Preservar bootstrap, SSH forced-command, rollback, Caddy,
volumes e segredos. Mover somente arquivos versionados, nunca state/credenciais.
Documentação ativa refletirá o runtime atual; specs históricas são preservadas.

## Alternativas consideradas

Organizar só pastas não eliminaria dependências invertidas. Criar novos serviços
distribuídos ampliaria desnecessariamente a migração. A escolha aprovada é separar
bibliotecas reais, preservando executáveis e as transações existentes.

## Aceitação e entrega

Ruff, Pyright e testes sem serviços locais em série, com baixa prioridade e uma
thread por biblioteca numérica. Contratos OpenAPI/JSON Schema sem drift. Testes
arquiteturais incluem TYPE_CHECKING, dependências proibidas e ciclos.
Gates preservados: packages 100% branch; apps 90% line. Wheels, migrações,
integrações e imagens são verificados no CI, sem deploy ou Stripe real.

Branch codex/package-boundaries-ops foi iniciada em bd6b730 e integra develop
db4ebd78 por orientação posterior do usuário; PR para develop. Implementação
nesta sessão; auditoria read-only em outro chat com contexto independente sobre
SHA congelado antes de qualquer push. Correções exigem reauditoria antes do
próximo push. Depois executar CI/Trivy na feature; abrir um PR somente com gates
verdes e auditoria válida para o SHA final. Sem merge ou deploy.

## Ajuste solicitado durante implementação

Código diretamente em packages/cnes_*, junto de tests/ e pyproject.toml, sem
níveis src/cnes_*; wheel prefix mapping mantém imports e recursos públicos.

Ops e Infra são diretórios irmãos na raiz. O OpenTofu recente permanece em
infra/opentofu; Terraform raw e Helm legados pertencem a infra, e os artefatos
operacionais de produção AWS ficam em ops/deploy/aws. Contratos e comportamento
da refatoração são comparados à develop integrada, incluindo os OIDs IANA atuais,
os ports de certificados/refresh tokens e a composição AWS adicionada nessa base.
Primitivas genéricas DynamoDB e protocolo de conexões SQLite pertencem a Data;
Identity não importa adaptadores concretos de Control Plane, mesmo para tipos.
