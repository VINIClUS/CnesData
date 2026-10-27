# Persistência de refresh DynamoDB — primeira fatia

`DynamoDBRefreshTokenStore` oferece somente `create`, `import_hashed` e
`validate`. Recebe um cliente de baixo nível já composto, o nome da tabela e
uma fábrica de segredo. O cliente pertence ao chamador: o adapter não cria
sessões, configura retentativas ou fecha o cliente. Não há wiring, migração,
CAS, consulta de agentes ativos ou conformidade completa com `RefreshTokenStore`.

## Duas projeções BASE

Defina `H(s) = s.encode("utf-8").hex()`. Tenant e agente usam os UUIDs canônicos
em minúsculas; o digest é SHA-256 hexadecimal em minúsculas. Cada registro
ocupa dois endereços distintos na mesma tabela, com chaves string `pk` e `sk`:

| Projeção | `pk` | `sk` |
| --- | --- | --- |
| DIGEST | `TENANT#{H(tenant_id)}` | `REFRESH_TOKEN#{H(token_sha256)}` |
| AGENT | `TENANT#{H(tenant_id)}#AGENT#{H(agent_id)}` | `REFRESH_TOKEN#{H(token_sha256)}` |

Ambos os itens contêm o registro completo. DIGEST determina a ocupação por
(tenant, digest), inclusive entre agentes diferentes. O mesmo digest em outro
tenant é independente. Não há GSI, Query ou Scan nesta fatia.

## Schema1

Cada escrita contém exatamente os atributos abaixo. Leitores ignoram atributos
adicionais, mas exigem todos os campos conhecidos, seus tipos individuais e a
concordância entre identidade embutida, chaves e discriminadores.

| Atributo | AttributeValue |
| --- | --- |
| `pk`, `sk` | `S`, endereço da projeção |
| `entity` | `S`, `REFRESH_TOKEN` |
| `schema_version` | `N`, `1` |
| `projection` | `S`, `DIGEST` ou `AGENT` |
| `token_sha256`, `tenant_id`, `agent_id`, `machine_fingerprint` | `S`, valores do domínio |
| `issued_at`, `token_expires_at` | `S`, ISO8601 UTC com seis casas e `+00:00` |
| `last_used_at`, `revoked_at` | `S` no mesmo formato, ou `NULL: true` explícito |
| `version` | `N`, inteiro decimal de 0 a 9223372036854775807 |
| `expires_at` | `N`, teto inteiro dos segundos epoch de `token_expires_at` |

O cálculo de TTL usa dias, segundos e microssegundos inteiros. Um prazo com
fração não é arredondado para o segundo anterior; um prazo em segundo exato
mantém esse segundo. Datas anteriores ao epoch são representáveis, sem promessa
de coleta pelo serviço. `expires_at` é o atributo TTL existente da tabela;
`token_expires_at` preserva o prazo preciso do domínio.

Leitura aceita as grafias UTC compatíveis com `datetime.fromisoformat`, incluindo
`Z` e separador espaço, sem reescrever itens. A igualdade entre projeções compara
os nove valores decodificados, não a grafia dos timestamps. `last_used_at` deve
estar entre emissão e expiração, inclusive; `revoked_at` deve ser igual ou
posterior à emissão e pode ultrapassar a expiração.

Entrada inválida mantém `RefreshTokenInvalid`. Uma versão não negativa válida
no domínio, mas superior ao limite do storage, produz `RefreshTokenUnavailable`
antes do SDK. Versões persistidas fora do limite também são indisponibilidade.

## Criação e importação

A validação do domínio precede o SDK e a fábrica. `create` chama a fábrica uma
vez, calcula o digest localmente e grava versão zero e timestamps opcionais
nulos. `import_hashed` preserva os nove valores, inclusive registros inativos,
sem chamar a fábrica. Plaintext não integra atributos nem requisições ao SDK.

Uma chamada `TransactWriteItems` grava dois `Put`, na ordem DIGEST e AGENT.
Cada um usa `attribute_not_exists(pk) AND attribute_not_exists(sk)`. Um item
corrompido existente também impede sobrescrita. Cada invocação pública recebe
um `ClientRequestToken` UUID novo; eventuais retentativas internas pertencem ao
cliente emprestado e preservam a mesma requisição. Não há retentativa no adapter,
compensação, reparo ou geração de outro segredo após incerteza.

Reimportar uma identidade ocupada não é sucesso idempotente. Somente
`TransactionCanceledException` com dois motivos ordenados, limitados a `None`
e `ConditionalCheckFailed`, com pelo menos uma falha condicional, permite
investigar colisão. O adapter descobre DIGEST com leitura forte, inspeciona o
endereço AGENT solicitado também com leitura forte e confirma o par real do
agente descoberto por snapshot atômico. Só a ocupação coerente permite
`RefreshTokenConflict`; órfãos, corrupção, leituras falhas, motivos ambíguos ou
mistos e falhas de serviço continuam `RefreshTokenUnavailable`.

Respostas precisam ser mapas com `ResponseMetadata`, HTTPStatusCode inteiro
200 e RequestId string não vazia. Uma resposta perdida ou malformada pode
ocorrer após o commit do par; permanece indisponibilidade. Uma instância nova
pode posteriormente observar esse par, sem recuperar o segredo pelo storage.

## Validação e snapshot

`validate` valida o contexto, calcula o digest e busca DIGEST com
`ConsistentRead=True`. Ausência é somente a omissão de `Item` em envelope de
sucesso válido. Se presente, o registro completo deve ser válido e corresponder
à chave solicitada antes de derivar o endereço do agente.

Em seguida, `TransactGetItems` lê DIGEST e AGENT, nessa ordem, em um único
snapshot BASE. O resultado precisa ter duas posições. Posições nulas ou vazias
após descoberta presente, par parcial, campos inválidos, identidade alterada ou
registros discordantes produzem indisponibilidade. Mesmo o desaparecimento de
ambos os itens nessa janela não é tratado como ausência. A decisão usa o
snapshot; o registro preliminar não autoriza acesso.

Só um par íntegro, não revogado e com `issued_at <= now < token_expires_at`
retorna o registro. Os demais pares íntegros retornam `None`. Corrupção precede
inatividade, inclusive para registros expirados, revogados ou de emissão futura.
Não há limpeza na leitura. Uma revogação pode ocorrer depois do snapshot:
validar não concede um lease.

## Integridade, privacidade e limites

TTL é coleta assíncrona e não determina autorização. A exclusão de AGENT antes
de DIGEST causa indisponibilidade; sem DIGEST, a descoberta retorna `None`.
Duas projeções sujeitas a TTL não permitem descobrir um órfão histórico em um
endereço de outro agente desconhecido. O adapter evita criar pares parciais
pela transação e falha fechado para inconsistências encontradas; não garante
ausência universal de órfãos nem unicidade de digest por toda a vida do sistema.

Erros públicos usam somente `operation=insert_refresh`,
`operation=validate_refresh` ou `operation=create_secret`, com encadeamento
suprimido. Plaintext, fingerprint e conteúdo de registros não aparecem nos
erros públicos, repr, traceback formatado ou diagnósticos emitidos pelo produto,
inclusive chamadas ao logger raiz e encaminhamento de erros do SDK. O adapter
não emite logs. Fingerprint e campos de domínio são obrigatórios nos payloads
SDK dos dois itens; plaintext permanece ausente de requisições e persistência.

A garantia cobre as superfícies controladas pelo produto. O cliente emprestado
pode emitir seus próprios dumps de protocolo e payload quando logging DEBUG do
SDK estiver habilitado. Esta fatia não garante segredo em todos os logs do
processo e não altera cliente, handlers, filtros, níveis ou eventos globais.
Uma política de diagnóstico em runtime exige desenho e verificação separados.

Moto oferece evidência em memória dos itens e formatos de requisição. Não
certifica ordenação ou conflitos reais da AWS, coleta TTL, durabilidade,
reinício, isolamento de serviço ou reutilização de segredos em produção.
Esta fatia não autoriza cutover ou migração de dados ativos; #280 e #60
permanecem abertos.
