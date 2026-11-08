# Operação

Código operacional e configuração de deploy ficam aqui. Procedimentos canônicos
permanecem em [docs/runbooks](../docs/runbooks/); não são duplicados neste diretório.

| Área | Localização | Estado |
|---|---|---|
| Deploy VPS | `deploy/vps/dev/`, `deploy/vps/prod/` | stacks Compose existentes |
| Identidade operacional | `identity/keycloak/` | realm usado pelo bootstrap dev |
| Backup local | `backup/` | wrapper e testes; implementação em central_api.local_backup |
| Diagnóstico edge | `scripts/edge_audit/` | snapshot operacional |
| Runtime AWS | `deploy/aws/` | Compose de produção e exemplo do credential process |

Infraestrutura fica na pasta irmã [infra](../infra/): OpenTofu em
`infra/opentofu/`, Terraform raw legado em `infra/aws/raw-legacy/` e chart Helm
aposentado em `infra/legacy/helm/web-dashboard/`.

O Compose de desenvolvimento continua na raiz. Os Dockerfiles permanecem nos apps,
os workflows em `.github/workflows` e os geradores/gates em `scripts/`.

## Migração de caminhos

| Caminho antigo | Caminho atual |
|---|---|
| `deploy/dev/` | `ops/deploy/vps/dev/` |
| `deploy/prod/` | `ops/deploy/vps/prod/` |
| `deploy/aws/raw/` | `infra/aws/raw-legacy/` |
| `charts/web-dashboard/` | `infra/legacy/helm/web-dashboard/` |
| `deploy/aws/config` | `ops/deploy/aws/config` |
| `deploy/compose.production.yaml` | `ops/deploy/aws/compose.production.yaml` |
| `infra/opentofu/` | `infra/opentofu/` (preservado) |
| `docker-compose.keycloak/` | `ops/identity/keycloak/` |
| `scripts/edge_audit/` | `ops/scripts/edge_audit/` |
| `scripts/local_backup.py` | `ops/backup/local_backup.py` |

Specs e planos históricos conservam os caminhos originais. Nesta reorganização
foram movidos somente arquivos versionados; state Terraform, credenciais e
configurações privadas não são parte da migração.

## Contratos operacionais

- Bootstrap instala os mesmos scripts e nomes de forced-command no VPS.
- Deploy preserva validação de `SSH_ORIGINAL_COMMAND`, tags, `.env.image` e rollback.
- Caddy, volumes e caminhos remotos de segredos conservam sua semântica.
- Migração de código não implica executar bootstrap, deploy ou Terraform.
- Infraestrutura dos outros repositórios continua sob seus owners existentes.

## Artefatos de release

Os fontes AWS estão em ops/deploy/aws. Dentro de production-config/config.tar,
os nomes deploy/compose.production.yaml e deploy/aws/config permanecem estáveis
para os consumidores externos; os checksums cobrem os bytes dos fontes movidos.
