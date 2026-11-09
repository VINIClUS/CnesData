# Seed demo e smoke de produção (#476)

Status: Código; nenhuma operação AWS executada. O aceite de Produção permanece aberto.
Consumir `demo_seed_iam_contract` de #481 e o handoff privado aprovado do controller.
O seed não emite usuários Cognito, certificados ou credenciais permanentes.

## Inputs e fronteira

Obter os nove campos de [demo-seed-iam.md](demo-seed-iam.md) do output revisado,
sem editar IAM ou publicar valores privados. Confirmar readback da role, trust e inline
policy contra `policy_name`/`policy_sha256` do contrato; esse hash não é prova AWS sozinho.
A conta, tabela, PK e role são exatas; não usar a role `canary_seeder`.

O arquivo privado `seed-spec.json` contém **somente** `tenant_id`, `user_id` (subject
Cognito do usuário demo criado fora deste script) e `oidc_issuer`. O tenant deve derivar
exatamente a PK do contrato. Armazenar inputs em diretório privado fora Git/state/logs,
com permissões 0700/0600; não passar tokens em argv, não usar shell tracing/debug SDK.

O script constrói quatro itens, nunca aceita um payload DynamoDB arbitrário:

| Entidade | Conteúdo fixado |
|---|---|
| TENANT | nome `Demonstração sintética`, chave canônica do tenant |
| MEMBERSHIP | usuário demo, issuer exato, role `viewer`, GSI canônico por usuário |
| DATASETVERSION | dataset `cnes`, run/version `demo-seed-v1`, competência `2026-01` |
| DATASETPOINTER | pointer `active` para essa versão |

Timestamp determinístico do fixture: `2026-01-01T00:00:00Z`. O manifesto esperado é
`reconciliation/<tenant>/2026-01/demo-seed-v1/run-manifest.json`. Isso **não** publica
objetos ou um Run aprovado: #473/#474 continuam responsáveis pela publicação sintética
real e pelo smoke posterior. Não promover uma referência sem os objetos/manifestos
verificados; o serving falha enquanto essa publicação não existir. O seed não substitui
um pointer já publicado, nem tenta migrar dados ou preservar versões anteriores.

Cada item recebe `demo_seed_owner` determinístico. GetItem forte em todas as quatro
chaves precede qualquer write; item existente precisa ser byte-a-byte idêntico,
inclusive marca de propriedade, índices e payload. Filho sem tenant é recusado.
PutItem exige `attribute_not_exists(pk) AND attribute_not_exists(sk)`. Uma corrida
condicional só é replay após releitura idêntica; erro incerto interrompe sem retry cego.
Se houve interrupção depois do tenant, repetir os mesmos inputs completa os itens
faltantes da mesma propriedade. Não há rollback/delete, overwrite, Query, Scan,
Update, batch, semáforo ou concessão para outra PK/tabela.

A proteção cobre essas chaves conhecidas. IAM limita PK/tabela, não SK/payload; não
alegar uma busca por itens desconhecidos sem Scan/Query. Reservar o tenant sintético
exclusivamente ao seed e interromper caso o inventário privado indique outra utilização.

## Execução futura autorizada

Depois dos gates #476 P3 e do apply dedicado #481, usar sessão temporária do principal
exato do trust. O consumidor chama STS `AssumeRole` com `RoleSessionName=demo-seed` e
`DurationSeconds=900`; verifica identidade e expiração 900s, tolerância de cinco segundos.
O `max_session_duration_seconds=3600` é o mínimo configurável da role AWS, não um cap
IAM de 900. Uma latência/deriva maior falha fechado; renovar a sessão de forma consciente.
As credenciais assumidas ficam somente na memória do processo.

```bash
uv run --frozen python -m scripts.promotion.seed_demo \
  --contract /private/demo-seed-iam.json --spec /private/seed-spec.json \
  --release-hash "$RELEASE_SHA256"
```

Executar duas vezes com os mesmos inputs. Ambas devem sair 0; a segunda não escreve.
A saída só contém `assertion` (`demo_seed:pass`/`demo_seed:fail`), `timestamp` e
`release_hash`. Não anexar inputs, itens, IDs, emails, tokens ou logs SDK à issue.
Uma falha não autoriza deletar/recriar tenant: interromper e revisar a colisão em privado.

## Smoke posterior

O entrypoint `python -m scripts.promotion.smoke_production` usa probes reais HTTP/SDK,
com timeout, redirects desabilitados e sem log de exceção/corpo. Reusa `challenge_of`,
`claims_of`, `aud_label` e `scope_has_api` de `aws_capability`. Não executar os probes
inteiros `_trust_probe.py`/`_s3_retention_probe.py`: alteram trust/policies de laboratório
ou demonstram GOVERNANCE, não RA/CRL/COMPLIANCE produtivos.

Preparar um arquivo privado de inputs do smoke, a partir do handoff da release:

- Raiz: `demo_user`, `demo_tenant`, `other_tenant` (diferente e sem membership).
- `cognito`: `domain` AWS-managed prefix, `issuer`, `client_id`, `redirect_uri`,
  `logout_uri` exatos de prod. Grants `primary`, `without_resource`, `wrong_verifier`,
  cada um com `authorize_url`, `code`, `verifier`, `callback_state`.
- `serving`: `bucket`, `version_id`, `body_sha256` da publicação sintética aprovada.
- `sfn`: `state_machine_arn`, `role_arn`, `definition`, `role_policies` (mapa
  nome→documento), `execution_arn`, `task_definition_arn`, `task_parameters` governados.
- `object_lock`: `audit_bucket`; fixture aprovado: `approved_synthetic_fixture=true`,
  `bucket`, `key`, `version_id`.
- `roles_anywhere`: `crl_id`, `crl_sha256`, `trust_anchor_arn`, `profile_arn`, `role_arn`,
  `revoked_serial`, `expected_session_arn`, `helper`, `aws_config`, `valid_profile`,
  `revoked_profile`, `valid_certificate`, `revoked_certificate`, `valid_key`, `revoked_key`.

Preparar três logins browser com códigos novos e únicos. A URL deve usar
`response_type=code`, o client/callback de prod, `state` criptográfico,
`code_challenge_method=S256` e `code_challenge=challenge_of(verifier)`; incluir escopos
OIDC e `https://api.cnesdata.com.br/api.access`. Em `primary` e `wrong_verifier`, incluir
`resource=https://api.cnesdata.com.br`; em `without_resource`, omitir `resource`.
Capturar callback code/state somente no arquivo privado temporário; não usar callback
localhost do laboratório no client de prod. O runner compara state/challenge/callback,
troca os códigos, exige negação com verifier incorreto, ausência de aud sem resource,
verifica RS256/issuer/exp/aud do token principal e do refresh e faz logout no endpoint padrão.
Não reutilizar códigos depois de uma execução: preparar três códigos novos para novo smoke.

`memberships_isolation` exige somente o tenant demo, usuário esperado, negação 403
com outro tenant e 401 anônimo. CORS exige origem pública correta e nega outra origem,
TRACE e header não permitido. Serving exige metadados privados/versionados, envelope
200 com três campos exatos e TTL limitado; a segunda requisição S3 não herda bearer,
cookies, tenant ou referrer, recusa redirect e valida hash/tenant/run do corpo sintético.

SFN lê trust exato account/state-machine, todas as inline policies e nenhuma managed
policy extra, definição e histórico da execução SUCCEEDED de até uma hora. Cada task
agendada deve usar os parâmetros e revisão aprovados. Isso não substitui as negações
reais RunTask/PassRole de D5, descritas em [cloudfront-free.md](cloudfront-free.md).

COMPLIANCE: lê configuração audit de 365 dias e usa **somente** versão sintética já
aprovada em bucket `cnesdata-cap-*`, distinto do audit, com retenção COMPLIANCE futura.
O runner tenta delete da versão e encurtamento com bypass, exige AccessDenied e
retenção intacta. Esses pedidos só são autorizados após aprovação explícita do fixture;
a retenção já foi fixada fora do runner. Não cria bucket/objeto, não apaga versão de
produção. Readback COMPLIANCE mais negações não prova privilégios root ou toda a matriz
IAM; os gates IAM e a limpeza pós-expiração do capability check continuam obrigatórios.

RA/CRL: emissão/revogação/importação da CRL são do dono EXT-4, fora deste runner.
Usar folhas de teste autorizadas, uma válida e outra revogada na CRL habilitada exata;
`revoked_serial` é hex privado. O hash/anchor/nextUpdate e serial da CRL são conferidos.
Os profiles só podem conter `region=us-east-2` e o comando exato:
```text
<helper> credential-process --certificate <cert> --private-key <key>
  --trust-anchor-arn <anchor> --profile-arn <profile> --role-arn <role>
```

O comando do profile deve ser uma única linha, nessa ordem de argumentos.
`AWS_CONFIG_FILE` deve ser `aws_config` e `AWS_SHARED_CREDENTIALS_FILE=/dev/null`.
O helper deve estar instalado/pinado e revisado; as chaves nunca entram em Git/state.
O profile válido precisa retornar a sessão exata; o revogado precisa falhar no processo
com `AccessDenied`. Falta de arquivo, certificado expirado ou outro erro não passa.
Não rodar o runbook antigo de laboratório contra a CRL produtiva nem restaurar/importar
CRLs automaticamente; custódia, alertas e readback posterior continuam no handoff EXT-4.

```bash
uv run --frozen python -m scripts.promotion.smoke_production \
  --inputs /private/smoke-inputs.json --release-hash "$RELEASE_SHA256" \
  > /private/smoke-evidence.json
```

Saída: array de oito objetos com **somente** `assertion`, `timestamp`, `release_hash`.
O resultado está no nome fechado `nome:pass`/`nome:fail`. Falta de input/probe/readback,
exceção, corpo HTTP inesperado ou negação ausente gera fail e exit 1; nunca converter
um item não executado em sucesso. Descartar inputs de login/credenciais ao terminar.
Assinar/anexar somente essa evidência redigida à #476, sob a release exata do handoff.
Ela não é autoatestação de Ready/Produção nem substitui o pacote assinado do controller.
