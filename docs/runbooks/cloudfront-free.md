# Capability check: CloudFront Free ACTIVE

Status: documentado, nenhum executado. O plano Free do CloudFront não tem recurso no provider
AWS: a assinatura fica fora do state do OpenTofu (`docs/superpowers/specs/
2026-08-29-cnesdata-production-deployment-design.md`, seção 6.1). O gate só vê os sinais de
plano pago no plan (certificado `vip`, Origin Shield, monitoring subscription e real-time
logs, regras do `policies/cnesdata.rego`) e o bloqueio de ativação
`cloudfront_free_plan_active`, que fica pendente até esta evidência.

## Aprovação

Aprovação explícita do dono na #389, citando a distribuição e a WAF dedicada (escopo
CloudFront, `us-east-1`, não compartilhada com LimnoPulse). Nada acontece antes do cutover de
DNS; falha em qualquer passo para a ativação.

## Retenção mínima

- Nenhum recurso novo: a prova liga a distribuição e a WAF existentes ao plano Free e verifica
  `ACTIVE`.
- Sem upgrade automático nem pay-as-you-go: se o plano não ficar `ACTIVE`, reverter e parar.
- A entrada do binding é a saída `free_subscription_binding_input` do módulo static-web
  (distribuição e WAF exatas).

## Procedimento

1. Conferir a elegibilidade e a cota Free da conta antes do cutover.
2. Chamada manual e idempotente da API do Pricing Plan Manager em `us-east-1`, ligando a
   distribuição e a WAF exatas ao plano FREE.
3. Ler o estado da assinatura e exigir `ACTIVE`; repetir a chamada e exigir o mesmo resultado
   (idempotência).
4. Conferir que a distribuição não tem Origin Shield, certificado `vip`, monitoring
   subscription nem real-time logs, e que `tofu plan` continua sem diff.

## Evidência redigida

Registrar só: plano (`FREE`), estado (`ACTIVE`), resultado da repetição e a confirmação de que o
plan não tem diff. Trocar conta, ID da distribuição e ARN da WAF por `<conta>`, `<dist>`,
`<waf>`. Anexar a evidência assinada de deploy; nunca credenciais.

## Limpeza

- Nada a apagar. Se o plano não ficou `ACTIVE`, desfazer a ligação, manter a ativação bloqueada
  e abrir issue.
- Só depois da evidência, um humano remove `cloudfront_free_plan_active` de
  `activation_blocking_preconditions` em PR próprio.
