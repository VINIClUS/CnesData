# Capability check: Object Lock COMPLIANCE do audit

Status: documentado, nenhum executado. O bucket `audit` nasce com Object Lock COMPLIANCE de 365
dias (`test_audit_tem_object_lock_compliance_365_na_criacao`). Esta prova cria objetos que
ninguém consegue apagar antes do vencimento, nem a conta root. GOVERNANCE já foi exercitado pela
#398 (`docs/runbooks/floci-and-aws-capability.md`); não repetir aqui.

## Aprovação

Pedir aprovação explícita do dono na #389 antes de qualquer chamada, citando o bucket de teste,
a retenção e a data de vencimento. Sem aprovação registrada, não executar. Nunca usar o bucket
`audit` de produção: a prova usa um bucket efêmero `cnesdata-cap-<uuid8>`.

## Retenção mínima

- Bucket novo com Object Lock habilitado na criação, em `us-east-2`, com tags
  `Project=CnesData`, `Purpose=aws-capability`, `Run=<uuid8>`.
- COMPLIANCE com a menor retenção que a API aceita (1 dia). O objeto e o bucket só podem ser
  apagados depois do vencimento; planejar a limpeza para o dia seguinte.
- Um único objeto de poucos bytes, com conteúdo sintético.
- Orçamento: dentro do envelope de USD 8 do produto; sem versões extras.

## Procedimento

1. Sessão temporária de role assumida. Nunca root, nunca CI.
2. Criar o bucket com `--object-lock-enabled-for-bucket` e o bloqueio público de conta ativo.
3. `put-object` com `--object-lock-mode COMPLIANCE` e `--object-lock-retain-until-date` em
   hoje + 1 dia.
4. Provar que a retenção vale: `delete-object --version-id` e `put-object-retention` encurtando
   a data devem falhar com `AccessDenied`, inclusive com
   `--bypass-governance-retention`.
5. Provar que a role de runtime do audit não tem `s3:BypassGovernanceRetention` nem
   `s3:PutObjectRetention` (matriz IAM do `test_audit_sem_politica_de_bypass_ou_delete`).

## Evidência redigida

Registrar só: modo, dias de retenção, código de erro de cada negação (`AccessDenied`), contagem
de versões e a data de limpeza prevista. Trocar conta, bucket e version id por `<conta>`,
`<bucket>`, `<versao>`. Nunca colar ARN, URL assinada nem credencial.

## Limpeza

- No dia seguinte ao vencimento: apagar a versão, o bucket e conferir o inventário final vazio
  (`cnesdata-cap-*` em S3).
- Se a limpeza falhar, abrir issue com a data de vencimento; o bucket é cobrado até lá.
- Registrar na #389 a data e o resultado da limpeza.
