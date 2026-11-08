"""Compatibilidade dos paths e bytes do artefato de configuração de produção."""

import hashlib
import os
import shutil
import tarfile
from pathlib import Path

from tests.production.delivery.test_ci_workflows import ROOT, _shell, _workflow


def test_config_empacota_fontes_existentes_e_checksums_reais(tmp_path: Path) -> None:
    paths = (
        "ops/deploy/aws/compose.production.yaml",
        "ops/deploy/aws/config",
        "uv.lock",
        "apps/web_dashboard/bun.lock",
        "infra/opentofu/env/prod/.terraform.lock.hcl",
        "infra/opentofu/modules/observability/cost-manifest.json",
        "policies",
        "schemas/cost-manifest.schema.json",
    )
    for relative in paths:
        source, target = ROOT / relative, tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            shutil.copyfile(source, target)
    job = _workflow()["jobs"]["opentofu-policy-cost"]
    body = next(
        s["run"]
        for s in job["steps"]
        if s.get("name") == "Package validated configuration and locks"
    )
    environment = dict(os.environ, GITHUB_SHA="a" * 40, GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="1")
    result = _shell(tmp_path, environment, body)
    assert result.returncode == 0, result.stderr
    aliases = {
        "ops/deploy/aws/compose.production.yaml": "deploy/compose.production.yaml",
        "ops/deploy/aws/config": "deploy/aws/config",
    }
    with tarfile.open(tmp_path / "config-artifact/config.tar") as archive:
        assert {aliases.get(path, path) for path in paths} <= set(archive.getnames())
        assert not set(aliases).intersection(archive.getnames())
        for source, target in aliases.items():
            payload = archive.extractfile(target)
            assert payload is not None
            assert payload.read() == (ROOT / source).read_bytes()
    sums = (tmp_path / "config-artifact/SHA256SUMS").read_text().splitlines()
    for line in sums:
        digest, name = line.split()
        assert (
            hashlib.sha256((tmp_path / "config-artifact" / name).read_bytes()).hexdigest() == digest
        )
