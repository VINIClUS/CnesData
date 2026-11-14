# Restore trimestral (#477)

Exercício trimestral de recuperação: reconstrói o API e um pointer e objeto de serving
sintéticos sem mudar produção. Este runbook não faz deploy, não restaura tabela nem objeto em
produção e não troca rota.

Fonte: `docs/superpowers/specs/2026-08-29-cnesdata-production-operations-design.md` (§6).

## Backups de produção

- Control plane: PITR de 35 dias (`infra/opentofu/modules/control-plane/main.tf`).
- Buckets de dados e de audit: versionamento ligado (`infra/opentofu/modules/data-buckets/main.tf`).
- Entrypoints estáticos: rollback versionado, conforme [rollback.md](rollback.md).
- Sem réplica entre regiões na fase 1.

Restore ou export destrutivo do PITR exige uma execução aprovada à parte, fora deste exercício.

## Pré-condições

- Aprovação do dono, registrada no tracker #93.
- Digest GHCR do API da última release aceita, lido da evidência de promoção.
- Pointer `active` e versão do tenant demo, conforme [demo-seed.md](demo-seed.md). Só o tenant
  demo é usado: nenhum dado de tenant real sai de produção.

## 1. Conferir os backups

Só leitura em produção:

```bash
aws dynamodb describe-continuous-backups --table-name "$CONTROL_PLANE_TABLE" --output json
aws s3api get-bucket-versioning --bucket "$DATA_BUCKET" --output json
```

Esperado: PITR `ENABLED` com a janela de 35 dias e versionamento `Enabled`.

## 2. Reconstruir o API

O API sobe pelo digest GHCR exato da release, sem rebuild, contra os emuladores do gate de
[aws-runtime-validation.md](aws-runtime-validation.md#gate-em-emuladores), num projeto compose
próprio (por exemplo `cnesdata-restore`) para não tocar em outras stacks.

## 3. Reconstruir o pointer e o objeto de serving

1. Em produção, só leitura: o item `DATASETPOINTER` `active` do tenant demo, com leitura
   consistente, e a versão do objeto de serving que ele aponta, lida pelo `versionId`.
2. Nos emuladores: gravar o pointer e o objeto lidos. Nenhuma escrita vai para produção.

## 4. Provar

- O API reconstruído responde `200` com o envelope de serving para o pointer reconstruído, como
  no item SMK-10 de [production-promotion.md](production-promotion.md).
- O hash do objeto servido é igual ao hash da versão lida em produção.
- O digest do API em execução é igual ao da release.

## 5. Limpeza

Derrubar só o projeto do exercício:

```bash
docker compose -p cnesdata-restore --profile aws-test down -v
```

## Evidência

O exercício não é item das matrizes. O artefato redigido segue o formato de
[production-promotion.md](production-promotion.md#como-ler-as-matrizes) e guarda:

- status do PITR e do versionamento;
- igualdade do digest do API, da versão do pointer e do hash do objeto;
- o `200` do envelope.

O hash do artefato vai para o tracker #93. Nunca registrar conta, ARN, object key, URL assinada,
corpo do objeto ou ID de tenant.
