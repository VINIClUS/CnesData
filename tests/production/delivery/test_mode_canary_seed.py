"""Fixtures do canary de modo: bytes pequenos, sintéticos e descritos pelo manifesto."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import polars as pl
import pytest

_ROOT = Path(__file__).resolve().parents[3]
_FIXTURES = _ROOT / "tests/production/fixtures/mode_canary"
_DOCS = _ROOT / "docs/fixtures/data-plane"
_FILES = ("cnes-local-v1.parquet", "cnes-nacional-v1.parquet")
_SYNTHETIC = {
    "CPF": re.compile(r"9000000000[0-9]"),
    "CNS": re.compile(r"999000000000[0-9]{3}"),
    "NOME_PROFISSIONAL": re.compile(r"PROFISSIONAL TESTE [0-9]{3}"),
    "CNES": re.compile(r"1234567"),
}


def _manifest() -> dict[str, Any]:
    return json.loads((_FIXTURES / "fixture-manifest.json").read_text(encoding="utf-8"))


def test_manifesto_tem_campos_fechados_e_sem_tenant_municipal() -> None:
    manifest = _manifest()

    assert set(manifest) == {"schema", "competencia", "file_subtype", "files", "expectations",
                             "synthetic_proof"}
    assert manifest["schema"] == "cnesdata.mode-canary-fixtures/v1"
    assert (manifest["competencia"], manifest["file_subtype"]) == ("2026-01", "CNES_VINCULO")
    assert sorted(manifest["files"]) == sorted(_FILES)
    assert "354130" not in json.dumps(manifest)
    assert manifest["expectations"] == {"gold_row_count": 7}


@pytest.mark.parametrize("name", _FILES)
def test_fixture_confere_sha_tamanho_linhas_e_origem_nos_docs(name: str) -> None:
    body = (_FIXTURES / name).read_bytes()
    entry = _manifest()["files"][name]

    assert set(entry) == {"sha256", "size", "schema_version", "row_count", "source_type",
                          "manifest_id", "origin"}
    assert entry["sha256"] == hashlib.sha256(body).hexdigest()
    assert entry["size"] == len(body)
    assert entry["row_count"] == pl.read_parquet(_FIXTURES / name).height
    assert entry["schema_version"] == "cnes-profissional-v1"
    assert entry["source_type"] in {"CNES_LOCAL", "CNES_NACIONAL"}
    assert body == (_DOCS / name).read_bytes()
    assert entry["origin"] == f"docs/fixtures/data-plane/{name}"


@pytest.mark.parametrize("name", _FILES)
def test_fixture_so_contem_identidades_sinteticas(name: str) -> None:
    frame = pl.read_parquet(_FIXTURES / name)

    for column, pattern in _SYNTHETIC.items():
        values = [value for value in frame[column].to_list() if value is not None]
        assert values
        assert all(pattern.fullmatch(value) for value in values), column
    cells = [str(value) for row in frame.rows() for value in row]
    assert not any("354130" in cell for cell in cells)
    proof = _manifest()["synthetic_proof"]
    assert proof == {column: pattern.pattern for column, pattern in _SYNTHETIC.items()}
