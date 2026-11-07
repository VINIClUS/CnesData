# cnes_contracts — Canonical contracts package

## Executive Summary

Single source of truth for domain contracts: pydantic v2 models for every
Gold dim/fato, `landing.extractions`, job status enum, e DTOs públicos. Ports pertencem ao Core; o protocolo SQL legado pertence
a Data. JSON Schema exported to
`docs/contracts/schemas/` for consumption by non-Python agents
(oapi-codegen Go). Zero infra dependency — pure types.

## Module Map

| File | Contents |
|---|---|
| `dims.py` | `Profissional`, `Estabelecimento`, `ProcedimentoSUS`, `CBO`, `CID10`, `Municipio`, `Competencia` |
| `fatos.py` | `VinculoCNES`, `ProducaoAmbulatorial`, `Internacao`, `ProcedimentoAIH` |
| `landing.py` | `Extraction`, `ExtractionRegisterPayload`, `FileManifest`, `UploadUrlRequest`, `UploadUrlResponse`, `ExtractionFailPayload` |
| `jobs.py` | `JobStatus` enum, `JobTransitionEvent` |
| `cnes_domain.ports.extraction` / `cnes_data.storage.protocols` | `DimLookupPort`, `RowMapperPort`, `ExtractionRepoPort`, `ExtractorPort` |
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
