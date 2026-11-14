# Incidente e drill do Roles Anywhere (#477)

Procedimento de incidente de credencial do API/VPS e o drill que o prova. Complementa o
capability check de aceite e revogação em
[roles-anywhere-evidence.md](roles-anywhere-evidence.md), sem repetir o procedimento dele, e
cobre o item SMK-04 de [production-promotion.md](production-promotion.md).

Este runbook não faz deploy nem muda recurso. Trust anchor, profile, CRL e a role da API são
`shared_outputs` de produtor externo (`infra/opentofu/env/prod/variables.tf`). Cada mudança
abaixo é feita no repositório dono, por plan exato auditado e aprovado pelo dono. A CA externa e
offline é operada pelo dono de segurança, e AWS Private CA é proibida.

## Valores de produção

Fonte: `docs/superpowers/specs/2026-08-29-cnesdata-production-operations-design.md` (§6).

- Sessão máxima: profile `durationSeconds=3600`, helper `--session-duration 3600` e role
  `MaxSessionDuration=3600`.
- Revogação só por CRL do trust anchor; nunca OCSP nem CDP.
- Leaf único do API/VPS: `CA:false`, `Digital Signature` e SHA-256 ou mais forte.

## 1. Desligar

1. Profile e trust anchor desabilitados, no repositório dono.
2. CRL do trust anchor atualizada com o leaf comprometido, pelo dono de segurança.
3. Sessões anteriores revogadas na role da API: `AWSRevokeOlderSessions` ou um Deny explícito
   com `aws:TokenIssueTime` anterior ao instante do incidente.
4. O dono de segurança recebe o alerta. O instante do Deny entra na evidência.

## 2. Provar a negação

Com credenciais emitidas antes do incidente (no drill, um leaf de teste como o
`cnesdata-cap-<uuid8>` de [roles-anywhere-evidence.md](roles-anywhere-evidence.md)):

- DynamoDB: leitura da tabela do control plane negada;
- S3: leitura de `serving/` no bucket de dados negada;
- Step Functions: describe da máquina de produção negado.

Os três precisam voltar `AccessDenied` antes de declarar fail-closed. O repositório não tem
essas três sondas: o smoke de produção (`scripts/promotion/smoke_production.py`) só confere a
CRL e a recusa do leaf revogado ao abrir sessão, não sessões já emitidas. A forma exata das
três leituras entra no plan do drill aprovado pelo dono. A identidade usada é conferida só
por leitura:

```bash
aws sts get-caller-identity --query Arn --output text
```

## 3. Manter o deny

O Deny fica no mínimo pela sessão máxima (3600 s) mais a propagação. A propagação é o tempo até
a negação medido no passo 3 de [roles-anywhere-evidence.md](roles-anywhere-evidence.md), nunca
uma estimativa. Remover o Deny antes disso reabre as sessões anteriores.

## 4. Rotacionar e religar

1. O dono de segurança emite um leaf novo na CA externa. A chave fica fora do container,
   legível só pelo UID fixo, e nunca entra em repositório, state ou log.
2. O host gate do infra-ansible instala o leaf novo.
3. A CRL nova chega ao CnesData só como mudança de variável em
   `shared_outputs.roles_anywhere.crl` (`enabled`, `next_update`, `sha256`), sem diff de
   recurso, validada pelo gate de [infrastructure-plan.md](infrastructure-plan.md).
4. Profile e trust anchor religados, no repositório dono.
5. Uma sessão nova com o leaf novo assume a role; o leaf revogado continua negado.

## Evidência

Formato de [roles-anywhere-evidence.md](roles-anywhere-evidence.md), mais:

- negação por serviço (três booleanos);
- duração efetiva do Deny e a propagação medida;
- sessão nova aceita (sim/não) e leaf revogado negado (sim/não).

O registro segue o schema de [production-promotion.md](production-promotion.md#schema-de-evidência)
com o item `SMK-04`. Nunca registrar certificado, chave, token de sessão, CRL, conta, ARN, CN ou
serial.
