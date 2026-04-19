"""Macro: CnesExtractor end-to-end — Firebird cursor → parquet."""
from pathlib import Path

import fdb
import pytest

from cnes_domain.models.extraction import (
    ExtractionIntent,
    ExtractionParams,
)
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.io_guard import SpoolGuard

pytestmark = pytest.mark.perf_macro

_COD_MUN_FIXTURE = "355030"


def _fb_connect(dsn: str) -> fdb.Connection:
    host, rest = dsn.split("/", 1)
    port, db = rest.split(":", 1)
    return fdb.connect(
        host=host, port=int(port), database=db,
        user="SYSDBA", password="masterkey",  # noqa: S106
    )


def test_extrair_profissionais_100k(
    benchmark, fb_perf_dsn, tmp_path: Path,
) -> None:
    con = _fb_connect(fb_perf_dsn)
    try:
        params = ExtractionParams(
            intent=ExtractionIntent.PROFISSIONAIS,
            cod_municipio=_COD_MUN_FIXTURE,
        )
        guard = SpoolGuard(max_bytes=2 * 1024 * 1024 * 1024)
        extractor = CnesExtractor()

        def _run() -> None:
            out = extractor.extract(
                params=params, con=con, tmp_dir=tmp_path, guard=guard,
                batch_size=10_000,
            )
            assert out.exists()
            assert out.stat().st_size > 0
            out.unlink()

        benchmark.group = "dump_e2e"
        benchmark.pedantic(_run, iterations=1, rounds=3)
    finally:
        con.close()
