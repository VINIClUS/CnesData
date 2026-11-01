# Amendment #441 — chave ausente no S3 sob ListBucket condicionado

Status: aceito, sem mudança de runtime nem de matriz. Data: 2026-11-01.
Base: `develop@463b1449`. Autoridade: issue #441 e a nota de fechamento
`2026-11-01-step-functions-iam-closure.md` (seção "Riscos a verificar antes da ativação").

## Contexto

A #441 supôs que, sob a matriz v5, `get_object` de chave ausente devolveria 403, porque o
`s3:ListBucket` de unit, recovery e API é condicionado por `StringLike s3:prefix`. Então
`S3ObjectStore._is_missing` (aceita só `404`, `NoSuchKey`, `NotFound`) relançaria o erro e
`stat()` falharia na primeira tentativa.

## Evidência da sonda AWS real

Fonte: https://github.com/VINIClUS/CnesData/issues/441#issuecomment-6559506298 (redigida, sem
conta nem ARN). Bucket efêmero, role reader na forma da matriz (`GetObject` e `ListBucket` com
`StringLike s3:prefix` em `normalized/*` e `serving/*`), controle positivo por prefixo.

| Chamada da reader | `normalized/` | `serving/` |
|---|---|---|
| `GetObject`, chave presente | 200 | 200 |
| `GetObject`, chave ausente | 404 `NoSuchKey` | 404 `NoSuchKey` |
| `HeadObject`, chave ausente | 404 | 404 |
| `ListObjectsV2 Prefix=<ausente> MaxKeys=1` | 200, `KeyCount=0` | 200, `KeyCount=0` |
| `ListObjectsV2 Prefix=<presente> MaxKeys=1` | 200, `KeyCount=1` | 200, `KeyCount=1` |

Outras observações:
- `ListObjectsV2` sem prefixo: 403 (a condição está ativa).
- `GetObject` de chave ausente em `raw/`, fora dos prefixos de `ListBucket`: 403.
- Role controle, `ListBucket` sem condição: 404 em todas as chamadas, inclusive em `raw/`, onde
  não tem `GetObject`; listagem sem prefixo: 200.

## Inferência

Para objeto inexistente, o S3 decide entre 404 e 403 pelo `s3:ListBucket`, avaliado com a chave
como `s3:prefix`. É inferência dos controles, não documentação explícita da AWS: o controle
devolveu 404 até onde não tinha `GetObject`, e a reader devolveu 403 onde a chave não casa com o
`StringLike`.

## Decisão

- Os caminhos 1 (fallback `ListObjectsV2` no runtime) e 2 (relaxar a condição de `ListBucket`)
  da #441 são desnecessários.
- A matriz v5 e o runtime ficam inalterados.
- A regra "Corrigir grants, não o runtime" (v5, seção "Proveniência das correções de IAM")
  vale sem exceção.
- O risco da nota de fechamento fica resolvido pela sonda; resta o critério 3 abaixo.

## Invariante normativo

Para unit, recovery e API, no data bucket: todo prefixo de `s3:GetObject` deve estar coberto
pelos valores do `s3:ListBucket` com `StringLike s3:prefix` da mesma role.

- Cobertura: o valor `v` cobre o prefixo `p` se `v == p`, ou se `v` termina em um único `*` e
  `p` começa com `v` sem o `*`.
- Não conta como cobertura: outro operador (`StringLikeIfExists`), chaves extras na condição,
  `ListBucket` sem condição ou em outro bucket.
- Wildcard fora de um único `*` final falha fechado (`invariant unsupported_pattern=<valor>`).
- Se quebrar, a chave ausente sob o prefixo descoberto passa a devolver 403 e `stat()` levanta
  `ClientError` em vez de devolver `None`.

Testes que sustentam o invariante:
- `tests/production/infra/test_s3_missing_object_invariant.py::`
  `test_get_object_de_cada_role_fica_coberto_pelo_list_bucket_condicionado` (policies
  renderizadas) e os `test_oraculo_*` do mesmo arquivo (o oráculo de cobertura).
- `packages/cnes_infra/tests/object_store/test_s3_missing_object.py::`
  `test_stat_de_chave_ausente_devolve_none` (`NoSuchKey`, `404` e `NotFound` viram `None`).

## Impacto

- Nenhuma mudança para MinIO, local ou legado: não há alteração de código.
- Audit fica fora do invariante: não tem `ListBucket` e usa outro bucket. Seu `stat` só roda
  em chave ausente no replay de conflito condicional, caminho inalterado.

## Pendente

O critério 3 da #441 (prova com cada role real no gate de #389) continua aberto: a sonda usou
uma role na forma da matriz, não as três roles reais.
