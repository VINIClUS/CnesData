# data_processor — Changelog

Histórico de fases de implementação. Não autoritativo para o estado atual
— isso vive em `CLAUDE.md`. Não carregado automaticamente pelo worker.

## BPA + SIA adapters (T12/T13, 2026-04-23)

- `adapters/bpa_adapter.py` — `map_bpa_c_to_fato`, `map_bpa_i_to_fato`. BPA_C uses sentinel `_SK_PROFISSIONAL_AGREGADO=1` (seed dim_profissional row 1 required).
- `adapters/sia_adapter.py` — `map_apa_to_fato`, `map_bpi_to_fato` (historico flag toggles SIA_BPI vs SIA_BPIHST).
- `adapters/sia_dim_sync.py` — `sync_dim_procedimento` (S_CDN), `sync_dim_municipio` (CADMUN with ibge7 check-digit).
- `producao_ambulatorial_repo.gravar` upserts idempotent; `fontes_reportadas` JSONB merged via `||`.
- Migration 012 added natural-key unique index on `fato_producao_ambulatorial` to support ON CONFLICT upsert.
- Migration 013 extended `chk_fonte_amb` CHECK to allow SIA_BPIHST.

## CDC delta mode (P3, 2026-05-03)

- Delta is the only inbound shape (no flag, no legacy snapshot path).
- `cdc_merger.merge_delta(df, conn, source, intent, apply_iu_fn=None)` branches Parquet rows on `_op ∈ {I,U,D}`. D applied inline via `text("DELETE FROM gold.X WHERE pk = :pk")` per (source, intent) PK template aligned with edge agent's `delta/profiles.go`. I/U applied via `apply_iu_fn(df_iu) -> int` callback (existing upsert path).
- `processor.route_delta(df, conn, source, intent, apply_iu_fn=None)` raises `ValueError("missing_op_column")` if `_op` absent.
- DELETE idempotency: `delete_no_op` INFO log when rowcount=0 (already-deleted row).

## P2 integrity (2026-05-04)

`integrity_check.verify_parquet(path, expected_sha256)` recomputes SHA-256 over downloaded Parquet (1MB chunks); raises `IntegrityError` on mismatch; skips when `expected_sha256 None`. `processor.verify_and_route_delta(parquet_path, expected_sha256, conn, source, intent, apply_iu_fn=None)` calls verify_parquet → pl.read_parquet → route_delta. Mismatch propagates `IntegrityError` to caller (caller fails the job). landing.extractions gains nullable `sha256 char(64)` column (Alembic 018).
