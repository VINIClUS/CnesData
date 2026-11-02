# Capability check: aceite e revogação do Roles Anywhere

Status: documentado, nenhum executado. A infraestrutura de produto consome o trust anchor, o
profile e a role da API como saídas compartilhadas versionadas (`shared_outputs`,
`schema_version = 2`); o trust anchor usa uma CA externa e offline, e AWS Private CA é proibida
pelo contrato de custo. Este check prova na AWS real que um certificado válido assume a role e
que um certificado revogado por CRL deixa de assumir.

## Aprovação

Aprovação explícita do dono na #389, citando o trust anchor, o profile e o certificado folha de
teste. A emissão e a revogação são operacionais e ficam fora do repositório e do state: a chave
privada da CA e as chaves folha nunca entram no repositório, em OpenTofu ou em log.

## Retenção mínima

- Um certificado folha de teste de curta duração (até 1 dia), emitido pela CA externa só para
  este check, com nome `cnesdata-cap-<uuid8>`.
- A CRL publicada contém só esse certificado de teste; reaproveitar a CRL de produção não é
  permitido.
- Nenhum recurso novo na AWS além da importação da CRL no trust anchor existente, revertida na
  limpeza.

## Procedimento

1. Aceite: `aws_signing_helper credential-process` com o certificado folha, o trust anchor, o
   profile e a role de teste; confirmar com `aws sts get-caller-identity` que a identidade é a
   role esperada e que `SourceIdentity` tem o CN do certificado.
2. Revogação: revogar o certificado na CA, regenerar a CRL, importar no trust anchor e
   habilitar a CRL.
3. Repetir o passo 1 com o mesmo certificado: deve falhar com `AccessDenied`. Anotar o tempo
   até a negação passar a valer.
4. Conferir que a CRL de produção em `shared_outputs.roles_anywhere.crl` (`enabled`,
   `next_update`, `sha256`) não mudou.

## Evidência redigida

Registrar só: aceite (sim/não), revogado (sim/não), código de erro da negação, tempo até a
negação e o `next_update` da CRL de teste. Trocar conta, ARNs, CN e serial por `<conta>`,
`<arn>`, `<cn>`, `<serial>`. Nunca registrar certificado, chave, token de sessão ou a CRL.

## Limpeza

- Remover a CRL de teste do trust anchor e restaurar a CRL de produção, conferindo o `sha256`.
- Destruir a chave e o certificado folha de teste fora do repositório.
- Inventário final: nenhum recurso `cnesdata-cap-*` em IAM Roles Anywhere; registrar na #389.
