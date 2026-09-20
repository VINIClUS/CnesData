"""Compara o vertical slice CNES completo contra o golden congelado (CND-002)."""

from __future__ import annotations

import json
import sys
from contextlib import nullcontext
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.compare_cnes_data_plane import (  # noqa: E402
    APPROVED_RULES,
    GOLD_FILE,
    SERVING_FILE,
    UnexplainedDifference,
    compare_outputs,
    materialize_actual_directory,
    materialize_expected_directory,
    require_explained,
    run_vertical_slice,
)

if TYPE_CHECKING:
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO

    from cnes_domain.ports.object_store import ObjectStat

_FIXTURES_ROOT = _REPO_ROOT / "docs" / "fixtures" / "data-plane"
_RUN_ID = "run-golden-cnd054"


class _FakeObjectStore:
    """Store em memória — mesma forma de test_reconcile_cnes._FakeObjectStore."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        from cnes_domain.ports.object_store import ObjectStat as _ObjectStat

        payload = body.read()
        assert sha256(payload).hexdigest() == expected_sha256
        self.objects[key] = payload
        return _ObjectStat(key=key, size_bytes=len(payload), sha256=expected_sha256)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        from cnes_domain.ports.object_store import ObjectStat as _ObjectStat

        payload = self.objects.get(key)
        if payload is None:
            return None
        return _ObjectStat(key=key, size_bytes=len(payload), sha256=sha256(payload).hexdigest())

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        raise NotImplementedError


def _run_and_materialize(tmp_path: Path) -> tuple[Path, Path]:
    store = _FakeObjectStore()
    artifacts = run_vertical_slice(_FIXTURES_ROOT, store, _RUN_ID)
    expected_dir, actual_dir = tmp_path / "expected", tmp_path / "actual"
    materialize_expected_directory(_FIXTURES_ROOT, expected_dir)
    materialize_actual_directory(store, artifacts, actual_dir)
    return expected_dir, actual_dir


def test_vertical_completo_bate_com_golden_cnes002(tmp_path: Path) -> None:
    expected_dir, actual_dir = _run_and_materialize(tmp_path)

    differences = compare_outputs(expected_dir, actual_dir)
    require_explained(differences, APPROVED_RULES)

    expected_gold = pl.read_parquet(expected_dir / GOLD_FILE)
    actual_gold = pl.read_parquet(actual_dir / GOLD_FILE)
    expected_rows = expected_gold.drop("_source_manifest_ids").to_dicts()
    actual_rows = actual_gold.drop("_source_manifest_ids").to_dicts()
    assert actual_rows == expected_rows
    assert actual_gold.height == 7

    expected_payload = json.loads((expected_dir / SERVING_FILE).read_bytes())
    actual_payload = json.loads((actual_dir / SERVING_FILE).read_bytes())
    for field in ("competencia", "kpis", "divergence_counts", "missing_sources"):
        assert actual_payload[field] == expected_payload[field]


def test_perturbacao_de_valor_gold_produz_diferenca_nao_aprovada(tmp_path: Path) -> None:
    expected_dir, actual_dir = _run_and_materialize(tmp_path)

    perturbed = pl.read_parquet(actual_dir / GOLD_FILE).with_columns(
        (pl.col("CH_TOTAL") + 1).alias("CH_TOTAL")
    )
    perturbed.write_parquet(actual_dir / GOLD_FILE)

    differences = compare_outputs(expected_dir, actual_dir)

    with pytest.raises(UnexplainedDifference, match="field=CH_TOTAL"):
        require_explained(differences, APPROVED_RULES)
