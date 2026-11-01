# Athena do operador

O Athena é só de operador: nenhuma role de aplicação ou serviço consulta. O operador roda
queries em série, no workgroup `cnesdata-prod-operator`.

## Limites

- Corte por query: 5 GB, forçado pelo workgroup.
- Alarme de 1 dia: ao somar 2 GB de bytes processados (DML) na janela deslizante, o alarme
  aciona o SNS de cutoff e a Lambda desabilita o workgroup.
- k = 4 é a premissa de queries em voo no disable, mais 1 que cruza o limiar. A AWS não limita a
  concorrência por workgroup sem capacity reservation (paga e proibida). O teto do mês é
  31 x 2 GB + 5 x 5 GB = 87 GB, abaixo de 100 GB, só enquanto o operador respeita a premissa.

## Reativação

A reativação é manual, só no mês seguinte e com revisão de quem leu o alarme e as queries:

```bash
aws athena update-work-group --work-group cnesdata-prod-operator --state ENABLED
```

Nenhum apply reativa o workgroup (`ignore_changes = [state]`) e nenhuma role de aplicação tem
`athena:UpdateWorkGroup`.

## Ativação da release

A precondição `cloudfront_free_plan_active` bloqueia a ativação. Confirmar que o plano CloudFront
Free está ACTIVE na conta é decisão PENDENTE DO USUÁRIO; sem ela, a web ACL custa USD 7 por mês e
estoura o envelope de 8 USD.
