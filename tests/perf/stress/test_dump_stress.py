"""Stress: N workers paralelos extraindo do Firebird via CnesExtractor."""
import concurrent.futures
import time
from pathlib import Path

import fdb
import pytest

from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.io_guard import SpoolGuard

pytestmark = pytest.mark.stress

_COD_MUN_FIXTURE = "355030"


def _fb_connect(dsn: str) -> fdb.Connection:
    host, rest = dsn.split("/", 1)
    port, db = rest.split(":", 1)
    return fdb.connect(
        host=host, port=int(port), database=db,
        user="SYSDBA", password="masterkey",  # noqa: S106
    )


@pytest.mark.parametrize("n_workers", [1, 4, 8, 16])
def test_dump_paralelismo(
    fb_perf_dsn: str, tmp_path: Path, n_workers: int,
) -> None:
    def _worker(i: int) -> float:
        con = _fb_connect(fb_perf_dsn)
        try:
            t0 = time.monotonic()
            CnesExtractor().extract(
                params=ExtractionParams(
                    intent=ExtractionIntent.PROFISSIONAIS,
                    cod_municipio=_COD_MUN_FIXTURE,
                ),
                con=con,
                tmp_dir=tmp_path / f"worker_{i}",
                guard=SpoolGuard(max_bytes=2 * 1024 * 1024 * 1024),
                batch_size=1000,
            )
            return time.monotonic() - t0
        finally:
            con.close()

    for i in range(n_workers):
        (tmp_path / f"worker_{i}").mkdir(exist_ok=True, parents=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
        durations = list(pool.map(_worker, range(n_workers)))

    max_duracao = max(durations)
    avg_duracao = sum(durations) / len(durations)
    print(
        f"n_workers={n_workers} max={max_duracao:.2f}s avg={avg_duracao:.2f}s",
    )
    assert max_duracao < 120.0, "single worker deve terminar em <2min"
