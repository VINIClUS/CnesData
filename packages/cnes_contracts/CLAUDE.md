# cnes_contracts — Canonical contracts package

## Executive Summary

Single source of truth for domain contracts: pydantic v2 models for every
Gold dim/fato, `landing.extractions`, job status enum, e DTOs públicos. Ports pertencem à capacidade que os consome; interfaces de I/O pertencem
a Data. Membership, OutboxEvent e códigos de erro compartilhados são contratos.
Políticas de billing, autorização, execução e retenção ficam fora deste pacote. JSON Schema exported to
`docs/contracts/schemas/` for consumption by non-Python agents
(oapi-codegen Go). Zero infra dependency — pure types.

## Module Map

| File | Contents |
|---|---|
| `dims.py` | `Profissional`, `Estabelecimento`, `ProcedimentoSUS`, `CBO`, `CID10`, `Municipio`, `Competencia` |
| `fatos.py` | `VinculoCNES`, `ProducaoAmbulatorial`, `Internacao`, `ProcedimentoAIH` |
| `landing.py` | `Extraction`, `ExtractionRegisterPayload`, `FileManifest`, `UploadUrlRequest`, `UploadUrlResponse`, `ExtractionFailPayload` |
| `jobs.py` | `JobStatus` enum, `JobTransitionEvent` |
| `cnes_data.ingestion.ports` / `cnes_data.storage.protocols` | `DimLookupPort`, `RowMapperPort`, `ExtractionRepoPort`, `ExtractorPort` |
| `export.py` | `export_all(target_dir)` JSON Schema generator |

## Conventions

- Preservar `ConfigDict` dos modelos existentes. DTOs HTTP migrados mantêm
  suas validações e mutabilidade; não aplicar frozen/strict por padronização.
- Field constraints mirror SQL CHECK constraints (`pattern=`, `ge=`, `le=`, `Literal`).
- Surrogate key fields: `sk_*: int = Field(gt=0)`.
- Monetary: `BIGINT` cents via `int = Field(ge=0)`, name suffix `_cents`.

## Gotchas

- `strict=True` rejects `1.0` for `int` fields — always pass `int`.
- `frozen=True` means `.model_copy(update=...)` is required for mutations.

Código diretamente nesta pasta, ao lado de tests/. Empacotamento wheel mantém
o namespace público. Fronteiras atuais: docs/architecture.md.

## Contratos compartilhados entre capacidades

- membership.py e audit.py preservam DTOs, cópia defensiva e serialização dos eventos.
- public_leads.py contém DTOs/validação estrutural dos contatos públicos.
- serving.py contém os envelopes Request/Grant e suas restrições de formato.
- errors.py e modes.py preservam vocabulários externos e de configuração.
- Ports consumidores pertencem a Data ou Control Plane; não colocá-los aqui.
- Não implementar autorização, transições, billing, execução ou retenção neste pacote.
