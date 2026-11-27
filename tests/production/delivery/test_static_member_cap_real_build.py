"""Build real do dashboard sob o teto por arquivo do transporte por digest do gate."""

from __future__ import annotations

import io
import os
import tarfile
from pathlib import Path

import pytest
import yaml

from scripts.promotion.publish_dashboard import read_candidate
from tests.production.delivery import _ia_static_digest_consumer as ia

ROOT = Path(__file__).resolve().parents[3]
THIS = "tests/production/delivery/test_static_member_cap_real_build.py"


def test_dist_cada_chave_do_build_real_cabe_no_teto_por_arquivo_do_gate() -> None:
    dist = os.environ.get("PRODUCTION_DIST")
    if not dist:
        pytest.skip("production_dist=not_requested")
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as archive:
        archive.add((ROOT / dist).resolve(), arcname=".")
    objects = read_candidate(raw.getvalue()).objects
    assert {"index.html", "theme-init.js"} <= {item.key for item in objects}
    assert [(item.key, item.size) for item in objects if item.size > ia.STATIC_MEMBER_LIMIT] == []


def test_job_do_dashboard_roda_o_teto_por_arquivo_sobre_o_build_real() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/verify-production.yml").read_text())
    job = workflow["jobs"]["dashboard"]
    commands = [step.get("run", "") for step in job["steps"]]
    assert any(THIS in command and "-k test_dist_" in command for command in commands)
    assert job["env"]["PRODUCTION_DIST"] == "apps/web_dashboard/dist"
