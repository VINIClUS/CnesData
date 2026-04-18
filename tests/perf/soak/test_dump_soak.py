"""Soak: 30 min de extrações em loop via CnesExtractor; estabilidade de RSS/FDs."""
import shutil
from pathlib import Path

import fdb
import pytest

from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.io_guard import SpoolGuard
from tests.perf.soak._harness import run_soak

pytestmark = pytest.mark.soak

_DURACAO_S = 1800.0
_RPS = 0.2
_COD_MUN_FIXTURE = "355030"


def _fb_connect(dsn: str) -> fdb.Connection:
    host, rest = dsn.split("/", 1)
    port, db = rest.split(":", 1)
    return fdb.connect(
        host=host, port=int(port), database=db,
        user="SYSDBA", password="masterkey",  # noqa: S106
    )


def test_dump_soak_30min(fb_perf_dsn: str, tmp_path: Path) -> None:
    con = _fb_connect(fb_perf_dsn)
    try:
        contador = iter(range(10**8))
        params = ExtractionParams(
            intent=ExtractionIntent.PROFISSIONAIS,
            cod_municipio=_COD_MUN_FIXTURE,
        )
        extractor = CnesExtractor()

        def _dump_um() -> None:
            i = next(contador)
            sub = tmp_path / f"dump_{i}"
            sub.mkdir(exist_ok=True)
            out = extractor.extract(
                params=params, con=con, tmp_dir=sub,
                guard=SpoolGuard(max_bytes=2 * 1024 * 1024 * 1024),
                batch_size=1000,
            )
            out.unlink(missing_ok=True)

        report = run_soak(_dump_um, duracao_s=_DURACAO_S, rps_alvo=_RPS)
        print(
            f"dump_soak rss_slope={report.rss_slope_mb_por_min:.2f}MB/min "
            f"fd_delta={report.fd_delta}"
        )
        assert report.rss_slope_mb_por_min < 1.0
        assert report.fd_delta < 100
    finally:
        con.close()
        shutil.rmtree(tmp_path, ignore_errors=True)
