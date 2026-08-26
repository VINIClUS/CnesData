"""Testes do verificador das fixtures golden do data plane."""

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from scripts.verify_data_plane_fixtures import FixtureError, verify_fixture_set

VERSIONED_ROOT = Path(__file__).parents[1] / "docs" / "fixtures" / "data-plane"


def _load_manifest(root: Path) -> dict[str, Any]:
    return json.loads((root / "fixture-manifest.json").read_text(encoding="utf-8"))


def _copy_fixture_set(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    root = tmp_path / "data-plane"
    shutil.copytree(VERSIONED_ROOT, root)
    return root, _load_manifest(root)


def _write_manifest(root: Path, manifest: dict[str, Any]) -> None:
    (root / "fixture-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _update_hash(root: Path, manifest: dict[str, Any], filename: str) -> None:
    content = (root / filename).read_bytes()
    manifest["files"][filename]["sha256"] = hashlib.sha256(content).hexdigest()


def _rewrite_parquet(
    root: Path,
    manifest: dict[str, Any],
    filename: str,
    frame: pl.DataFrame,
) -> None:
    frame.write_parquet(
        root / filename,
        compression="zstd",
        compression_level=3,
        statistics=True,
        row_group_size=5,
    )
    _update_hash(root, manifest, filename)


def test_rejeita_fixture_com_hash_divergente(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    manifest["files"]["cnes-local-v1.parquet"]["sha256"] = "0" * 64

    with pytest.raises(FixtureError, match="field=sha256"):
        verify_fixture_set(root, manifest)


def test_rejeita_fixture_com_arquivo_ausente(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    (root / "cnes-nacional-v1.parquet").unlink()

    with pytest.raises(FixtureError, match="status=missing"):
        verify_fixture_set(root, manifest)


def test_rejeita_fixture_com_contagem_de_linhas_divergente(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    filename = "cnes-local-v1.parquet"
    _rewrite_parquet(root, manifest, filename, pl.read_parquet(root / filename).head(4))

    with pytest.raises(FixtureError, match="field=row_count"):
        verify_fixture_set(root, manifest)


def test_rejeita_fixture_com_schema_divergente(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    filename = "cnes-local-v1.parquet"
    frame = pl.read_parquet(root / filename).with_columns(pl.col("CH_TOTAL").cast(pl.String))
    _rewrite_parquet(root, manifest, filename, frame)

    with pytest.raises(FixtureError, match="field=schema"):
        verify_fixture_set(root, manifest)


def test_rejeita_fixture_com_versao_divergente(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    manifest["fixture_version"] = 2

    with pytest.raises(FixtureError, match="field=fixture_version"):
        verify_fixture_set(root, manifest)


def test_rejeita_campo_nao_permitido_no_serving(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    filename = "cnes-serving-v1.json"
    serving = json.loads((root / filename).read_text(encoding="utf-8"))
    serving["campo_futuro"] = True
    (root / filename).write_text(json.dumps(serving), encoding="utf-8")
    _update_hash(root, manifest, filename)

    with pytest.raises(FixtureError, match="field=serving_fields"):
        verify_fixture_set(root, manifest)


def test_rejeita_identificador_pessoal_fora_do_padrao_sintetico(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    filename = "cnes-nacional-v1.parquet"
    frame = pl.read_parquet(root / filename).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("12345678901"))
        .otherwise(pl.col("CPF"))
        .alias("CPF")
    )
    _rewrite_parquet(root, manifest, filename, frame)

    with pytest.raises(FixtureError, match="field=CPF"):
        verify_fixture_set(root, manifest)


def test_rejeita_kpi_divergente(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    manifest["kpis"]["conflict_count"] = 2

    with pytest.raises(FixtureError, match="field=kpis"):
        verify_fixture_set(root, manifest)


def test_rejeita_precedencia_divergente(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    manifest["precedence"]["order"] = ["NACIONAL", "LOCAL"]

    with pytest.raises(FixtureError, match="field=precedence"):
        verify_fixture_set(root, manifest)


def test_rejeita_gold_sem_preenchimento_nacional_de_nulo_local(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    filename = "cnes-gold-v2.parquet"
    frame = pl.read_parquet(root / filename).with_columns(
        pl.when(pl.col("CNS") == "999000000000003")
        .then(pl.lit(None, dtype=pl.Int64))
        .otherwise(pl.col("CH_OUTRAS"))
        .alias("CH_OUTRAS")
    )
    _rewrite_parquet(root, manifest, filename, frame)

    with pytest.raises(FixtureError, match="field=gold"):
        verify_fixture_set(root, manifest)


def test_rejeita_divergencia_sem_valores_e_manifestos(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    del manifest["divergences"][0]["national_value"]

    with pytest.raises(FixtureError, match="field=divergences"):
        verify_fixture_set(root, manifest)


def test_cli_retorna_nao_zero_para_fixture_invalida(tmp_path: Path) -> None:
    root, manifest = _copy_fixture_set(tmp_path)
    manifest["tenant_id"] = "000000"
    _write_manifest(root, manifest)

    completed = subprocess.run(
        [sys.executable, "scripts/verify_data_plane_fixtures.py", str(root)],
        cwd=Path(__file__).parents[1],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 1
    assert "status=invalid" in completed.stderr


def test_valida_conjunto_versionado() -> None:
    verify_fixture_set(VERSIONED_ROOT, _load_manifest(VERSIONED_ROOT))
