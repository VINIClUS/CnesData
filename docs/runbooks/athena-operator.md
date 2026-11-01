# Athena do operador

O Athena é só de operador: nenhuma role de aplicação ou serviço consulta. O operador roda
queries em série, no workgroup `cnesdata-prod-operator`.

## Limites

- Corte por query: 5 GB, forçado pelo workgroup.
- Alarme de 1 dia: ao somar 2 GB de bytes processados (DML) na janela deslizante, o alarme
  aciona o SNS de cutoff e a Lambda desabilita o workgroup.
- k = 4 é a premissa de queries em voo no disable, mais 1 que cruza o limiar. A AWS não limita a
  concorrência por workgroup sem capacity reservation (paga e proibida).
- 1000 queries cobradas por mês é a segunda premissa. A AWS cobra cada query arredondada para
  cima ao megabyte, com mínimo de 10 MB, e `ProcessedBytes` mede só o que foi lido: cada query
  cobrada (`SUCCEEDED` ou `CANCELED`; `FAILED` não é cobrada) soma até 10 MiB além do que leu.
- Teto do mês: 31 x 2 GB + 5 x 5 GB + 1000 x 10 MiB = 97,49 GB, abaixo de 100 GB, só enquanto
  o operador respeita as duas premissas.

## Falha no caminho do cutoff

O alarme só aciona o SNS quando muda de estado. Se `cnesdata-prod-cutoff-lambda-errors` ou
`cnesdata-prod-cutoff-sns-delivery-failures` disparar, o alarme de cutoff já está em `ALARM` e
não aciona de novo enquanto continuar nele. Desabilitar o workgroup à mão:

```bash
aws athena update-work-group --work-group cnesdata-prod-operator --state DISABLED
```

## Reativação

A reativação é manual, só no mês seguinte e com revisão de quem leu o alarme e as queries. O
alarme de cutoff precisa estar em `OK`: reativar com ele em `ALARM` deixa o workgroup sem corte,
porque nenhuma query nova muda o estado. Com o workgroup desabilitado, a janela de 1 dia esvazia
em até 24 horas.

```bash
aws cloudwatch describe-alarms --alarm-names cnesdata-prod-athena-cutoff \
  --query 'MetricAlarms[0].StateValue' --output text
```

Só seguir se a saída for `OK`:

```bash
aws athena update-work-group --work-group cnesdata-prod-operator --state ENABLED
```

Nenhum apply reativa o workgroup (`ignore_changes = [state]`) e nenhuma role de aplicação tem
`athena:UpdateWorkGroup`.

## Ativação da release

A precondição `cloudfront_free_plan_active` bloqueia a ativação. Confirmar que o plano CloudFront
Free está ACTIVE na conta é decisão PENDENTE DO USUÁRIO; sem ela, a web ACL custa USD 7 por mês e
estoura o envelope de 8 USD.
