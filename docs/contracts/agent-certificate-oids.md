# OIDs dos certificados de agente (PEN IANA 67386)

Contrato das extensões X.509 que a CA do `central_api` grava no certificado folha do
`dump_agent_go` e que o `central_api` lê para ligar a requisição mTLS a um tenant e a um
agente. A fonte única no código é `packages/cnes_identity/cert_extensions.py`,
usada pela emissão (`auth/ca.py`) e pela leitura (`auth/peer_cert.py`). Origem: issue #60.

## Atribuição

| Campo | Valor |
| --- | --- |
| Private Enterprise Number | `67386` |
| Titular registrado na IANA | Vinicius A. Santana |
| Arco | `1.3.6.1.4.1.67386` (`iso.org.dod.internet.private.enterprise.67386`) |
| Registro | <https://www.iana.org/assignments/enterprise-numbers/> |

O PEN pertence ao titular e pode servir a outros projetos. O CnesData usa só o sub-arco
`.1`. Um novo OID do CnesData recebe o próximo número livre sob `.1`. Números já
publicados nunca são reaproveitados nem mudam de significado.

## Alocação do sub-arco CnesData

| OID | Nome | Valor | Crítica |
| --- | --- | --- | --- |
| `1.3.6.1.4.1.67386.1` | CnesData | arco, sem extensão própria | n/a |
| `1.3.6.1.4.1.67386.1.1` | `tenant_id` | tenant ao qual o agente foi provisionado | não |
| `1.3.6.1.4.1.67386.1.2` | `agent_id` | identificador do agente emitido no provisionamento | não |

As duas extensões são não críticas. Um verificador que não as conhece, como o Caddy no
`client_auth verify_if_given`, aceita a cadeia sem rejeitar o certificado. Quem decide
tenant e agente é o `central_api`.

## Codificação

O `extnValue` de cada extensão contém o DER de um `UTF8String` (tag `0x0C`):

- comprimento definido na forma mínima: curta até 127 bytes, longa com até 4 octetos e
  sem zero à esquerda;
- nenhum byte depois do valor;
- conteúdo UTF-8 válido e não vazio.

Exemplo: o valor `354130` vira `0C 06 33 35 34 31 33 30`.

A leitura é estrita. Tag diferente, comprimento indefinido ou não mínimo, bytes
sobrando, truncamento, UTF-8 inválido ou valor vazio resultam em `401 invalid_token`,
nunca em 500. A emissão também recusa valor vazio.
O mesmo vale para extensão ausente.

## Legado `1.3.6.1.4.1.99999`

Até a #60, os certificados de desenvolvimento usavam o placeholder não registrado
`1.3.6.1.4.1.99999.1.{1,2}`, com bytes UTF-8 crus no `extnValue`. O corte é seco: o
`central_api` não lê mais esses OIDs nem esse formato. Um certificado legado recebe
`401 invalid_token` em `require_agent_cert` e em `edge_identity_from_cert`, inclusive
em `POST /provision/cert/rotate`.

Por isso a rotação automática não recupera um certificado legado. O rotator do agente
trata o 4xx como terminal (`rotate_terminal_stop`). Procedimento para cada agente
afetado, depois do deploy do `central_api` com este contrato:

1. Parar o serviço do agente. Com o certificado legado, toda chamada mTLS dele recebe 401.
2. Rodar `dumpagent register --force --tenant-id <tenant> --base-url <url>` e aprovar o
   `user_code` na página de ativação do dashboard. Nos ambientes com CA privada, repetir o
   `--ca-pin`: `--force` sem ele remove o pin persistido.
3. Conferir `mtls_ok status=200` no stderr do `register`, que faz o smoke
   `GET /api/v1/agents/whoami` (sem `--no-smoke`). O smoke só avisa: o comando sai com 0
   mesmo com `mtls_check_failed`. Depois subir o serviço.

Conferência manual de um certificado emitido:

```bash
openssl x509 -in cert.pem -noout -text | grep -A1 "1.3.6.1.4.1.67386.1"
openssl asn1parse -in cert.pem | grep -A2 "1.3.6.1.4.1.67386.1"
```

## Fora deste contrato

A atribuição do PEN remove só o bloqueio da #60. Emitir certificados de agente em
produção continua dependendo da promoção AWS e do plano delivery (`docs/roadmap.md`).
