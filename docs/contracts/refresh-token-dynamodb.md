# Persistência de refresh DynamoDB — criação, validação e CAS

`DynamoDBRefreshTokenStore` oferece `create`, `import_hashed`, `validate`,
`mark_used`, `revoke` e `has_active_for_agent`, com conformidade completa com
`RefreshTokenStore`. Recebe um cliente de baixo nível já composto, o nome da tabela e
uma fábrica de segredo. O cliente pertence ao chamador: o adapter não cria
sessões, configura retentativas ou fecha o cliente. Não há wiring, migração,
cutover ou migração de dados ativos.

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
tenant é independente. A descoberta por agente usa Query BASE; não há GSI ou Scan.

## Schema1

Criação e importação contêm exatamente os atributos abaixo. Leitores ignoram atributos
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

## Existência de refresh ativo por agente

`has_active_for_agent` valida agente e contexto antes do SDK. Uma Query forte consulta
exatamente a partição AGENT do tenant/agente solicitado com prefixo `REFRESH_TOKEN#`,
sem filtro, projeção parcial ou contagem como autoridade. Cada linha completa precisa
ser válida e corresponder à identidade solicitada antes do mesmo snapshot atômico
DIGEST/AGENT, sem GetItem preliminar. Mesmo candidatos inativos exigem par íntegro.

A atividade vem exclusivamente do snapshot: `revoked_at is None` e
`issued_at <= context.now < token_expires_at`. Uso anterior não consome o token.
Todas as linhas e páginas observadas são validadas antes do retorno; corrupção ou falha
posterior vence um ativo anterior. Query vazia retorna `False` quando esgotada.

Somente ausência de `LastEvaluatedKey` ou mapa vazio encerra a paginação. Um cursor
não vazio precisa conter exatamente pk/sk, ambos AttributeValues unitários S não vazios,
na partição solicitada e com prefixo `REFRESH_TOKEN#` seguido de sufixo. Sua sort key
avança estritamente em bytes UTF-8; repetição/regressão falha fechado. O próximo pedido
reutiliza exatamente o cursor recebido. Items vazio não encerra quando há cursor.
O cursor é posição, sem exigir que corresponda a registro válido ou a linha retornada.
Não há limite arbitrário de páginas. Envelope exige Items list e metadata de sucesso.

O custo é uma Query forte por página e um TransactGetItems de dois itens por candidato,
inclusive inativos. A atomicidade cobre cada par; não há snapshot global da enumeração.
Mudanças coerentes entre Query e snapshot são aceitas e decididas pelo snapshot atual.
Uma criação após a posição percorrida pode não aparecer, e uma revogação posterior ao
snapshot não invalida retroativamente a observação. A consulta não concede lease.

Um DIGEST órfão sem AGENT é indescoberto por esta Query e não impede `False` quando
não há candidatos. AGENT descoberto sem DIGEST, par parcial ou ambos desaparecidos após
descoberta produzem indisponibilidade. Não há reparo, escrita, limpeza, fábrica de
segredo, retentativa ou alteração do cliente durante a consulta.

## Uso e revogação por CAS

`mark_used` e `revoke` validam segredo e `RefreshTokenMutation` antes do SDK, sem
chamar a fábrica. O segredo é opaco, inclusive whitespace. A descoberta DIGEST
é forte e precede o mesmo snapshot atômico completo de DIGEST e AGENT usado na
validação. DIGEST ausente retorna `False`; descoberta presente seguida de par
parcial ou inválido produz indisponibilidade, inclusive para versão stale ou
registro inativo. Não há busca de um endereço AGENT desconhecido nem limpeza.

Só um par íntegro com versão igual a `expected_version`, não revogado e com
`issued_at <= mutation.now < token_expires_at` é elegível. Uso também exige
`last_used_at` nulo ou `mutation.now >= last_used_at`. Revogação pode anteceder
um uso anterior dentro do intervalo de validade. A comparação usa datetimes e
precisão de microssegundos, sem relógio do sistema. Pares íntegros inelegíveis
retornam `False`, inclusive quando a versão do chamador excede o máximo do
storage. Uma versão persistida máxima elegível produz indisponibilidade antes
da escrita; versões persistidas inválidas falham antes das guardas.

Uma única transação contém dois `Update`, DIGEST e AGENT nessa ordem. Cada
condição compara todos os atributos conhecidos não chave com os AttributeValues
brutos daquela projeção: discriminadores, identidade, fingerprint, timestamps,
versão e TTL, incluindo `NULL` explícito. Isso é igualdade tipada do DynamoDB;
não é uma garantia de representação física contra escritores externos. Somente
o timestamp escolhido (`last_used_at` ou `revoked_at`) e `version + 1` mudam.
O novo timestamp usa UTC com seis casas e `+00:00`. Grafias dos demais timestamps,
TTL e atributos desconhecidos são preservados. Não há substituição integral,
reparo, compensação, fallback ou retentativa no adapter.

Somente envelope HTTP200 válido com RequestId não vazio confirma `True`. Apenas
um `TransactionCanceledException` com dois motivos ordenados limitados a `None`
e `ConditionalCheckFailed`, com pelo menos uma falha, permite nova leitura
atômica do par nos endereços originais. Ela permite `False` somente para par
válido e coerente, valores imutáveis idênticos e versão estritamente maior que
a versão esperada. Os imutáveis são digest, tenant, agente, fingerprint, emissão
e expiração decodificados. Isso classifica o estado observado, sem identificar
o escritor ou provar uma cadeia histórica completa de mutações.

Versão igual ou menor, imutáveis alterados, par parcial, corrupção ou releitura
falha continuam indisponibilidade. Conflitos de serviço, capacidade e motivos
mistos ou malformados também são indisponibilidade, sem releitura classificadora.
Uma resposta perdida ou malformada após commit permanece indisponibilidade,
mesmo que o par resultante exista. Uma chamada pública independente posterior
com versão antiga pode retornar `False` por seu próprio snapshot válido; isso
não atribui o commit anterior. Cada escrita usa um novo UUID ClientRequestToken,
sem reutilização entre chamadas ou geração de outro segredo após incerteza.

## Integridade, privacidade e limites

TTL é coleta assíncrona e não determina autorização. A exclusão de AGENT antes
de DIGEST causa indisponibilidade; sem DIGEST, a descoberta retorna `None`.
Duas projeções sujeitas a TTL não permitem descobrir um órfão histórico em um
endereço de outro agente desconhecido. O adapter evita criar pares parciais
pela transação e falha fechado para inconsistências encontradas; não garante
ausência universal de órfãos nem unicidade de digest por toda a vida do sistema.

Erros públicos usam somente `operation=insert_refresh`,
`operation=validate_refresh`, `operation=has_active_refresh`, `operation=mutate_refresh` ou
`operation=create_secret`, com encadeamento
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

Moto oferece evidência em memória dos itens, condições e interleavings determinísticos
do CAS. Não certifica ordenação ou conflitos reais da AWS, coleta TTL, durabilidade,
reinício, isolamento de serviço ou reutilização de segredos em produção.
Faltam wiring e migração; esta fatia não autoriza cutover
ou migração de dados ativos. #280 e #60 permanecem abertos.
