"""Spike: dump com burst 10x de workers concorrentes via CnesExtractor."""
import concurrent.futures
import time
from pathlib import Path

import fdb
import pytest

from cnes_domain.models.extraction import ExtractionIntent, ExtractionParams
from dump_agent.extractors.cnes_extractor import CnesExtractor
from dump_agent.io_guard import SpoolGuard

pytestmark = pytest.mark.spike

_COD_MUN_FIXTURE = "355030"


def _fb_connect(dsn: str) -> fdb.Connection:
    host, rest = dsn.split("/", 1)
    port, db = rest.split(":", 1)
    return fdb.connect(
        host=host, port=int(port), database=db,
        user="SYSDBA", password="masterkey",  # noqa: S106
    )


def _um_dump(fb_perf_dsn: str, out_dir: Path, i: int) -> float:
    sub = out_dir / f"dump_{i}"
    sub.mkdir(exist_ok=True, parents=True)
    con = _fb_connect(fb_perf_dsn)
    try:
        t0 = time.monotonic()
        out = CnesExtractor().extract(
            params=ExtractionParams(
                intent=ExtractionIntent.PROFISSIONAIS,
                cod_municipio=_COD_MUN_FIXTURE,
            ),
            con=con,
            tmp_dir=sub,
            guard=SpoolGuard(max_bytes=2 * 1024 * 1024 * 1024),
            batch_size=500,
        )
        out.unlink(missing_ok=True)
        return time.monotonic() - t0
    finally:
        con.close()


def test_dump_spike_recovery(fb_perf_dsn: str, tmp_path: Path) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        pre = list(pool.map(
            lambda i: _um_dump(fb_perf_dsn, tmp_path, i),
            range(10),
        ))

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        burst = list(pool.map(
            lambda i: _um_dump(fb_perf_dsn, tmp_path, 100 + i),
            range(20),
        ))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        rec = list(pool.map(
            lambda i: _um_dump(fb_perf_dsn, tmp_path, 200 + i),
            range(10),
        ))

    pre_max = max(pre)
    rec_max = max(rec)
    print(
        f"pre_max={pre_max:.2f}s burst_max={max(burst):.2f}s "
        f"rec_max={rec_max:.2f}s",
    )
    assert rec_max <= pre_max * 1.2
