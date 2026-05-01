"""Contract JSON Schema export tests."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

from cnes_contracts.export import MODELS, export_all

if TYPE_CHECKING:
    from pathlib import Path


def test_export_produz_todos_arquivos(tmp_path: Path):
    paths = export_all(tmp_path)
    assert len(paths) == len(MODELS)
    for p in paths:
        assert p.suffix == ".json"
        assert p.stat().st_size > 0


def test_export_json_valido(tmp_path: Path):
    export_all(tmp_path)
    for f in tmp_path.glob("*.json"):
        schema = json.loads(f.read_text())
        # Pydantic v2 schemas have either "properties" (object) or "type"
        assert "properties" in schema or "type" in schema or "$ref" in schema


def test_export_cria_dir_se_ausente(tmp_path: Path):
    target = tmp_path / "nested" / "schemas"
    assert not target.exists()
    paths = export_all(target)
    assert target.exists()
    assert len(paths) > 0


def test_export_sorted_keys_no_properties(tmp_path: Path):
    """Exported schemas have alphabetically sorted property keys."""
    export_all(tmp_path)
    sample = tmp_path / "profissional.json"
    if sample.exists():
        schema = json.loads(sample.read_text())
        props = list(schema.get("properties", {}).keys())
        assert props == sorted(props), f"properties not sorted: {props}"
