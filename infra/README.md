# Infraestrutura

`infra/` e [ops/](../ops/README.md) são diretórios irmãos. Este diretório mantém
as definições de infraestrutura; Ops reúne deploy, configuração de identidade,
backup e diagnóstico operacional.

| Caminho | Responsabilidade | Estado |
|---|---|---|
| `opentofu/` | Módulos, raiz prod e testes nativos AWS | atual; apply e ownership seguem os runbooks |
| `aws/raw-legacy/` | Terraform do fluxo raw anterior | legado aposentado |
| `legacy/helm/web-dashboard/` | Chart do dashboard | legado, fora do deploy ativo |

Os procedimentos permanecem em [docs/runbooks](../docs/runbooks/). Reorganizar
estes arquivos não executa provisionamento. State, credenciais e configurações
privadas ficam fora da migração; os repositórios externos mantêm seus owners.
