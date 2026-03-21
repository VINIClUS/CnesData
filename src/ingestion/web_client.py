"""
web_client.py — Camada de Ingestão: Cliente DATASUS/Web (STUB)

[WP-002] Módulo pendente de implementação.

Responsabilidade planejada:
  Buscar dados abertos do DATASUS via API HTTP com política de retry
  configurável (backoff exponencial) e timeout explícito.

Dependências planejadas:
  - requests (já no requirements.txt)

Regras de negócio que serão atendidas:
  - Validação de CNES via tabela nacional (cross-check com banco local).
  - Enriquecimento de dados de CBO via tabela CBO do Ministério da Saúde.
"""

# TODO [WP-002]: implementar cliente HTTP com retry policy e timeout
