# Package boundaries and Ops Implementation Plan

> Implementar com superpowers:executing-plans, nesta sessão, com auditoria final
> em outro chat antes de push/PR.

**Goal:** Oito pacotes com dependências verificadas e operação organizada em Ops.
**Architecture:** Monorepo modular; Core e Contracts puros, capabilities separadas,
composição externa em Runtime. Billing e control plane preservam transações.
**Tech Stack:** Python/uv, Go, React; ferramentas existentes, sem novo serviço.
**Spec:** docs/superpowers/specs/2026-11-07-package-boundaries-ops-design.md

## Global Constraints

- Preservar wire contracts, nomes/defaults de env, dados e revisions Alembic.
- Sem imports de apps por bibliotecas, ciclos ou fachada cnes_infra.
- Sem deploy, Terraform apply, Stripe real ou mudanças em repos externos.
- Checks locais seriais e limitados; integrações e imagens no CI.
- Auditoria independente do SHA final antes de cada push; PR após CI verde.

## Review Focus

- Imports dinâmicos e monkeypatches não podem resolver fontes antigas.
- Migrações devem ser encontradas no wheel e preservar a história.
- Query count deve atravessar threadpool sem vazar entre requests.
- Caminhos VPS devem preservar instalação, forced-command e rollback.
- Extras opcionais e diretórios de teste novos precisam estar cobertos no CI.

## Task 1: Baseline e fronteiras

- [x] Registrar baseline e corrigir referências ao dump_agent Python removido.
- [x] Escrever testes do grafo, imports e descoberta dos oito pacotes.
- [x] Demonstrar falha antes da migração.

## Task 2: Extrair capabilities e migrar consumidores

- [x] Extrair Contracts/Core, Data, Observability, Identity, Governance,
  Control Plane e Runtime conforme a spec; mover testes junto.
- [x] Remover reverse import query counter e dependência Alembic/Runtime.
- [x] Atualizar manifests, extras, lock, ferramentas, imagens e CI.
- [x] Remover cnes_infra; executar testes arquiteturais e checks locais.

## Task 3: Ops e documentação

- [x] Mover somente arquivos versionados para os destinos da spec.
- [x] Atualizar referências, bootstrap VPS e testes de backup.
- [x] Atualizar arquitetura, desenvolvimento, instruções e índice operacional.
- [x] Validar shell/configuração Compose sem iniciar serviços.

## Task 4: Verificação e auditoria

- [x] Ruff → Pyright → pytest sem serviços, serial, threads=1 (em blocos).
- [x] Comparar OpenAPI e JSON Schemas com baseline em diretório temporário.
- [ ] Congelar commit, abrir chat de auditoria read-only, resolver bloqueantes.
- [ ] Reauditar qualquer novo SHA antes do próximo push.

## Task 5: CI e PR

- [ ] Push somente após auditoria aprovada.
- [ ] Dispatch CI e Trivy no SHA auditado, sem workflows de deploy.
- [ ] Corrigir/reauditar se necessário e aguardar todos os gates.
- [ ] Abrir um PR para develop e entregar evidências e link da auditoria.

## Ajuste solicitado durante implementação

Código diretamente em packages/cnes_*, junto de tests/ e pyproject.toml, sem
níveis src/cnes_*; wheel prefix mapping mantém imports e recursos públicos.

Integrar develop db4ebd78 e preservar suas mudanças recentes antes de renovar
checks e auditoria. Ops e Infra ficam separados na raiz; infra/opentofu mantém
seu caminho e os destinos dos legados constam na spec atualizada.

## Task 6: Restringir Domain conforme orientação posterior

- [x] Provar RED para o inventário restrito de regras/transformações CNES/SUS.
- [x] Mover responsabilidades operacionais e seus testes aos donos da spec.
- [x] Compartilhar somente DTOs/erros necessários e usar interfaces estreitas.
- [ ] Renovar checks, contratos, wheels e auditoria do SHA final antes do push.
