"""Teste do gen_openapi."""
import json
from pathlib import Path

from scripts.gen_openapi import generate


def test_generate_escreve_json_valido(tmp_path: Path) -> None:
    output = tmp_path / "openapi.json"
    rc = generate(output)
    assert rc == 0
    assert output.exists()

    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["openapi"].startswith("3.")
    assert "info" in data
    assert "paths" in data


def test_generate_inclui_rotas_jobs(tmp_path: Path) -> None:
    output = tmp_path / "openapi.json"
    generate(output)
    data = json.loads(output.read_text(encoding="utf-8"))
    paths = data["paths"]

    assert "/api/v1/jobs/acquire" in paths
    assert "/api/v1/jobs/{job_id}/heartbeat" in paths
    assert "/api/v1/jobs/{job_id}/complete-upload" in paths


def test_generate_deterministico(tmp_path: Path) -> None:
    a = tmp_path / "a.json"
    b = tmp_path / "b.json"
    generate(a)
    generate(b)
    assert a.read_bytes() == b.read_bytes(), "output must be byte-identical between runs"
