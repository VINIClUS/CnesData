# Contrato normativo de ingestão raw da Phase 3

Status: ratificado por CND-029 em 2026-09-06.

Este documento é a autoridade para CND-030–034. A Phase 3 ingere vínculos
profissionais do CNES nacional e dados do Edge Agent até a camada raw. Normalização e
reconciliação estão fora deste contrato, assim como publicação de dataset, coleta de órfãos e
agendamento automático de reposição.

## 1. Identidade e escopo da fonte nacional

A única fonte nacional da Phase 3 é o arquivo mensal de profissionais:

`ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/PF/PF{UF}{YYMM}.dbc`

- `{UF}` é derivado dos dois primeiros dígitos do IBGE6 do tenant.
- `{YYMM}` é derivado exclusivamente da competência solicitada em `YYYY-MM`.
- O subtipo canônico é `CNES_VINCULO` e a fonte é `CNES_NACIONAL`.
- A identidade interna é `agent_id=system-datasus`.
- Cada extração nacional é `FULL`, sequência 1, sem base ou hash anterior.

A resolução IBGE6 para UF é total e fechada:

| Prefixo | UF | Prefixo | UF | Prefixo | UF |
|---|---|---|---|---|---|
| 11 | RO | 12 | AC | 13 | AM |
| 14 | RR | 15 | PA | 16 | AP |
| 17 | TO | 21 | MA | 22 | PI |
| 23 | CE | 24 | RN | 25 | PB |
| 26 | PE | 27 | AL | 28 | SE |
| 29 | BA | 31 | MG | 32 | ES |
| 33 | RJ | 35 | SP | 41 | PR |
| 42 | SC | 43 | RS | 50 | MS |
| 51 | MT | 52 | GO | 53 | DF |

IBGE6 ausente, não numérico, de tamanho diferente de seis ou com prefixo fora do mapa é erro
final de entrada. O transporte não consulta arquivo ST, estabelecimento, BigQuery nem outra fonte.
FTP `550` ou ausência do PF solicitado significa `source_not_published`, retryable, sem procurar
outra competência. Fallback de competência é proibido.

## 2. Transferência FTP e validação

O transporte usa FTP anônimo e executa, nesta ordem:

1. Consulta `SIZE` e `MDTM` imediatamente antes da transferência.
2. Baixa o DBC contando os bytes e calculando SHA-256 local incrementalmente.
3. Consulta novamente `SIZE` e `MDTM` imediatamente depois da transferência.
4. Exige metadados pré/pós idênticos e contagem local igual a `SIZE`.
5. Valida o DBC completo e o converte de arquivo para arquivo em DBF temporário.
6. Abre o DBF incrementalmente, valida o layout de 40 campos e os valores necessários.
7. Confirma que a competência do conteúdo é `COMPETEN=<YYYYMM>` solicitado.
8. Somente então retém as linhas com `CODUFMUN=<IBGE6>`.

Falha de rede, FTP temporária, indisponibilidade de `SIZE`/`MDTM` ou mudança dos metadados durante
a transferência é retryable. DBC inválido, DBF inválido, schema divergente, valor de campo
inválido ou arquivo PF cuja competência de conteúdo não corresponde à solicitada é falha final
de dados. A divergência de competência precede o filtro municipal e não é
`source_not_published`. Somente após confirmar a competência do arquivo, a ausência de linhas
para o IBGE6 solicitado é `source_not_published`, retryable. Recursos FTP, arquivos e diretórios
temporários são fechados ou removidos em `finally`, inclusive em falha.

FTP simples não autentica o publicador e o DATASUS não fornece checksum nesse endpoint. As duas
leituras de metadados, a contagem de bytes e o SHA-256 local demonstram integridade depois do
download, mas não demonstram autenticidade de origem.

O layout, os campos consumidos e o mapeamento estão congelados em
`docs/data-dictionary-datasus-pf.md`.

## 3. Parquet determinístico

O objeto de dados é um Parquet simples com estas opções obrigatórias:

- compressão interna Zstandard nível 3; em Go, `SpeedDefault`;
- estatísticas de colunas habilitadas;
- row groups com no máximo 64.000 linhas;
- metadado `CreatedBy` exatamente `Polars`;
- nenhuma compressão gzip externa e nenhuma extensão `.parquet.gz`.

As 14 colunas, tipos e ordem seguem o dicionário PF e o fixture
`docs/fixtures/data-plane/cnes-nacional-v1.parquet`. Entradas idênticas devem produzir bytes e
SHA-256 idênticos.

## 4. Chaves e imutabilidade

Dados e manifesto são objetos irmãos:

```text
raw/<tenant>/<source>/<competencia>/<snapshot_id>/data.parquet
raw/<tenant>/<source>/<competencia>/<snapshot_id>/manifest.json
```

O primeiro PUT de uma chave é imutável. Repetir a chave com bytes idênticos é sucesso
idempotente. Repeti-la com bytes diferentes é conflito e nunca sobrescreve o objeto anterior.
Uploads aceitos antes de uma rejeição permanecem órfãos imutáveis. A Phase 3 não os remove.

O JSON canônico usa a ordem dos campos de `RawManifest`, UTF-8 compacto, `null` explícito e
timestamp UTC RFC 3339. A serialização Python de referência é
`model_dump_json(exclude_none=False, by_alias=False)`. O servidor devolve seu
`manifest_sha256` canônico, que se torna a cabeça usada pelo próximo DELTA. O golden
cross-language é `docs/fixtures/data-plane/raw-manifest-v1.json`.

## 5. Identidade mTLS, lease e privacidade

Tenant, agente e fingerprint vêm somente do certificado mTLS verificado. JSON e headers de
aplicação não concedem identidade. Certificado ausente ou não verificável recebe `401`; agente
ausente ou revogado, tenant divergente ou fingerprint divergente recebe `403`, antes de acessar
job ou objeto.

`EDGE_JOB_LEASE_SECONDS` é 300. Claim, heartbeat, upload e manifesto sempre revalidam job, owner,
lease e fencing token quando o job ainda está vivo. O PUT usa streaming Starlette, spool com
limiar de 8 MiB em memória e limite inicial de 1 GiB. O corpo inteiro nunca é retido em memória.

Logs são eventos estruturados `key=value`. Bytes de payload, CPF, CNS e nome de profissional são
proibidos em qualquer nível de log.

## 6. Contrato HTTP futuro

Todas as respostas JSON usam `application/json`. JSON malformado ou incompatível com o schema
recebe `422`. Os erros usam `{"detail":"<codigo_estavel>"}`, salvo o `409` tipado de resync.

### 6.1 Claim do próximo job

`GET /api/v1/edge/jobs/next` não recebe body nem identidade em query/header de aplicação.

Sucesso `200`:

```json
{
  "job_id": "<id>",
  "source_type": "CNES_LOCAL",
  "file_subtype": "CNES_VINCULO",
  "competencia": "2026-01",
  "requested_snapshot_mode": "FULL",
  "fencing_token": 1,
  "lease_until": "2026-02-01T00:05:00Z",
  "raw_upload_path": "/api/v1/edge/jobs/<id>/raw-object"
}
```

Sem candidato elegível retorna `204` sem body. Falhas de identidade mTLS retornam `401` ou `403`.
O `200` representa um claim forte e canônico, nunca apenas descoberta posicional de ID.

### 6.2 Heartbeat

`POST /api/v1/edge/jobs/{job_id}/heartbeat`

Request:

```json
{"fencing_token":1}
```

Sucesso `200`:

```json
{
  "job_id": "<id>",
  "fencing_token": 1,
  "lease_until": "2026-02-01T00:10:00Z"
}
```

Job desconhecido retorna `404`; estado, owner, lease ou fence incompatível retorna `409`; falha
de identidade retorna `401` ou `403`. O lease renovado dura 300 segundos.

### 6.3 Upload do objeto raw

`PUT /api/v1/edge/jobs/{job_id}/raw-object` recebe bytes em streaming e exige:

- `X-Fencing-Token: <inteiro>`;
- `X-Object-Key: raw/<tenant>/<source>/<competencia>/<snapshot_id>/data.parquet`;
- `Content-Type: application/octet-stream`.

Primeira escrita ou replay byte a byte idêntico retorna `200`:

```json
{
  "object_key": "raw/<tenant>/<source>/<competencia>/<snapshot_id>/data.parquet",
  "object_sha256": "<sha256-lowercase>",
  "size_bytes": 5123
}
```

Job desconhecido retorna `404`. Identidade, estado, owner, lease, fence ou chave incompatível,
assim como replay divergente, retorna `409` sem publicar ou sobrescrever. Limite excedido retorna
`413`; media type diferente de `application/octet-stream` retorna `415`. Falhas mTLS retornam
`401` ou `403`. Job/fence são revalidados antes de ler e imediatamente antes de publicar.

### 6.4 Registro do manifesto

`POST /api/v1/edge/raw-manifests`

Request:

```json
{
  "job_id": "<id>",
  "fencing_token": 1,
  "manifest": {"manifest_version": 1}
}
```

`manifest` é o `RawManifest` completo. Aceite e replay terminal idêntico retornam `200`:

```json
{
  "accepted": true,
  "manifest_id": "<id>",
  "manifest_sha256": "<sha256-lowercase>",
  "full_resync_required": false,
  "reason": null
}
```

Rejeição válida de DELTA e seu replay terminal idêntico retornam `409`:

```json
{
  "accepted": false,
  "manifest_id": "<id>",
  "manifest_sha256": "<sha256-lowercase>",
  "full_resync_required": true,
  "reason": "SEQUENCE_GAP"
}
```

Job desconhecido retorna `404`. Falhas mTLS retornam `401` ou `403`. Demais conflitos de
identidade, modo, canonicalidade, fence ou objeto retornam `409` com código estável e sem
mutação.

## 7. Política de resync

DELTA é avaliado nesta ordem exata e interrompe na primeira razão aplicável:

1. `AGENT_RESYNC_REQUIRED`
2. `BASE_UNKNOWN`
3. `SEQUENCE_GAP`
4. `HASH_CHAIN_MISMATCH`
5. `SCHEMA_INCOMPATIBLE`
6. `BASE_TOO_OLD`
7. `CHAIN_TOO_LONG`

`RawResyncState` é o marcador durável do servidor, isolado pela tupla exata
`(tenant_id, agent_id, source_type, file_subtype, competencia)`. O marcador inexiste antes da
primeira rejeição de política. `AGENT_RESYNC_REQUIRED` aplica-se se, e somente se, o marcador já
existir quando um DELTA começar a avaliação. Qualquer rejeição válida de DELTA cria ou mantém o
marcador na mesma transação do job e do evento. Um FULL aceito remove o marcador na mesma
transação do aceite; falha de FULL, aceite de DELTA e qualquer erro anterior à política não o
removem. O marcador não cria job nem agenda trabalho.

FULL requer sequência 1 e base/hash anterior nulos. DELTA requer base conhecida, sequência
anterior mais um, hash canônico anterior, mesmo schema, idade da base de no máximo sete dias e
menos de 30 deltas já aceitos. Configuração pode reduzir, mas nunca ampliar ou desabilitar esses
limites.

Uma rejeição válida da política DELTA conclui atomicamente o job em `FAILED_FINAL`, grava
`RAW_RESYNC_<REASON>`, persiste no job o `rejected_manifest_sha256` dos bytes canônicos recebidos,
cria ou mantém `RawResyncState` e emite um único `raw.manifest.resync_required`; ela não cria
sidecar nem altera dataset pointer. O ID, payload e timestamp do evento são determinísticos.

Falha de autenticação, identidade, modo do job, owner, lease, fence, bytes não canônicos ou
objeto ausente/divergente não altera job, índice, outbox ou dataset pointer. O servidor não
agenda um FULL substituto. Após `409` tipado, o Edge persiste `force_full` somente para a source
key e descarta somente seus fingerprints pendentes. Cabeça e fingerprints já confirmados são
preservados. `force_full` só é consumido quando um job posterior explicitamente solicitar FULL.

Envelopes raw são imutáveis e duráveis, contendo job ID, fencing token, source key, JSON
canônico e hash do manifesto antes do envio. Novo fence cria novo envelope. Rede ou `5xx` retém o
envelope; somente `2xx` ou `409` tipado o encerra. Replay terminal autenticado de aceite ou
rejeição acontece antes de exigir lease/owner/fence ainda vivos e não duplica índice ou evento.
O replay de rejeição exige igualdade com o `rejected_manifest_sha256` guardado no job; hash
ausente ou divergente é conflito sem mutação.
