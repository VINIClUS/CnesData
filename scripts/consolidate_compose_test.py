"""Teste do consolidate_compose."""
from pathlib import Path

import yaml

from scripts.consolidate_compose import consolidate


def test_consolidate_merge_services_com_profiles(tmp_path: Path) -> None:
    a = tmp_path / "a.yml"
    b = tmp_path / "b.yml"
    a.write_text(yaml.safe_dump({"services": {"postgres": {"image": "pg"}}}))
    b.write_text(yaml.safe_dump({"services": {"minio": {"image": "minio"}}}))

    out = tmp_path / "out.yml"
    consolidate([a, b], out, profiles={a.name: "dev", b.name: "perf"})

    merged = yaml.safe_load(out.read_text())
    assert "postgres" in merged["services"]
    assert "minio" in merged["services"]
    assert merged["services"]["postgres"]["profiles"] == ["dev"]
    assert merged["services"]["minio"]["profiles"] == ["perf"]
