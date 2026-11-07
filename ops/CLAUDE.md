# Ops

Automação operacional e configurações de deploy. Índice e mapa de migração:
README.md deste diretório. Runbooks canônicos: docs/runbooks/.

- Não executar deploy, bootstrap ou Terraform durante refatorações e testes.
- Preservar os contratos SSH forced-command, rollback, volumes e Caddy.
- Nunca versionar nem mover state, credenciais ou arquivos privados de ambiente.
- Terraform raw e Helm estão explicitamente classificados como legados.
- Backup é um wrapper do CLI da API; lógica e entrypoint permanecem no app.
- Testes e validações devem respeitar a carga da máquina.
