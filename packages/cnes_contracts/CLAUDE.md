# cnes_contracts — Canonical contracts package

## Executive Summary

Single source of truth for domain contracts: pydantic v2 models for every
Gold dim/fato, `landing.extractions`, job status enum, and PEP 544
Protocols for storage/mapper ports. JSON Schema exported to
`docs/contracts/schemas/` for consumption by non-Python agents
(oapi-codegen Go). Zero infra dependency — pure types.

## Module Map

| File | Contents |
|---|---|
| `dims.py` | `Profissional`, `Estabelecimento`, `ProcedimentoSUS`, `CBO`, `CID10`, `Municipio`, `Competencia` |
| `fatos.py` | `VinculoCNES`, `ProducaoAmbulatorial`, `Internacao`, `ProcedimentoAIH` |
| `landing.py` | `Extraction`, `ExtractionRegisterPayload`, `FileManifest` |
| `jobs.py` | `JobStatus` enum, `JobTransitionEvent` |
| `protocols.py` | `DimLookupPort`, `RowMapperPort`, `ExtractionRepoPort`, `ExtractorPort` |
| `export.py` | `export_all(target_dir)` JSON Schema generator |

## Conventions

- All pydantic models use `ConfigDict(frozen=True, strict=True)`.
- Field constraints mirror SQL CHECK constraints (`pattern=`, `ge=`, `le=`, `Literal`).
- Surrogate key fields: `sk_*: int = Field(gt=0)`.
- Monetary: `BIGINT` cents via `int = Field(ge=0)`, name suffix `_cents`.

## Gotchas

- `strict=True` rejects `1.0` for `int` fields — always pass `int`.
- `frozen=True` means `.model_copy(update=...)` is required for mutations.
