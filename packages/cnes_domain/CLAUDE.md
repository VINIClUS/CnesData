# cnes_domain — Regras de negócio CNES/SUS

## Definição restrita

Este pacote contém somente competência, validações e transformações dos dados
CNES/SUS. Pureza é necessária, mas não basta para um módulo pertencer a Domain.
Um código deve representar uma regra ou invariante identificável desses dados.

## Inventário permitido

- competencia.py: períodos de referência do negócio.
- validators.py: identificação e validação dos dados CNES/SUS.
- processing/transformer.py: transformações canônicas de negócio.
- validation/dataframes.py: invariantes dos DataFrames, com Pandera.

O gate tests/architecture verifica esse inventário e exige revisão explícita
para ampliá-lo. Não usar nomes genéricos como common, utils, shared ou core
para receber responsabilidades de outras capacidades.

## Fora do escopo

- Billing, quotas, estados/execução de jobs, leases, outbox e recovery: Control Plane.
- Autenticação, tokens, certificados e onboarding: Identity.
- Ports de I/O, tenant ContextVar, circuit breaker e mapeamento para persistência: Data.
- Perfis, configuração, parsing de ambiente e release/retenção de artefatos: Runtime.
- DTOs e vocabulários compartilhados entre capacidades: Contracts.
- Telemetria, logging configurado e contadores: Observability.

## Convenções

- Funções determinísticas, sem I/O, ambiente, relógio implícito ou estado global mutável.
- Não importar apps nem outras capabilities. Não configurar logging/telemetria.
- DataFrames de entrada não podem ser mutados; usar with_columns.
- Testes verificam regras em Português, sem serviços externos.
- Aplicam-se os limites e as convenções do CLAUDE.md raiz.
