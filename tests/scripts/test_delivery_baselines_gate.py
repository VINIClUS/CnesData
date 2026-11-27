"""Oráculo opcional: validadores reais do gate no infra-ansible contra os baselines (P23)."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.promotion import delivery_baselines as db
from tests.production.delivery._delivery_baselines_fixtures import (
    CONFTEST_BIN,
    CUSTO,
    POLITICA,
    TOFU_BIN,
    criar_binario,
    criar_espelho,
    criar_produto,
    lock_texto,
)

IA = os.environ.get("CNESDATA_IA_CHECKOUT", "")
pytestmark = pytest.mark.skipif(not IA, reason="oraculo_ia=ausente env=CNESDATA_IA_CHECKOUT")
CONTRATO_SHA256 = "781ac403daefde27b4a4af723171b4babb73ab63be809a69f06d98a284a6d097"
ORACULO = """
import json, sys
sys.path.insert(0, sys.argv[1])
from tools.cnesdata_delivery import contract as c
modo, caminho = sys.argv[2], sys.argv[3]
raw = open(caminho, "rb").read()
try:
    value = c.loads(raw, canonical=True, limit=262144)
    c.public_safe(value)
    if modo == "toolchain":
        c.closed(value, "opentofu providers lock_sha256")
        c.closed(value["opentofu"], "version binary_sha256")
        assert value["opentofu"]["version"] == "1.13.2"
        c.digest(value["opentofu"]["binary_sha256"]); c.digest(value["lock_sha256"])
        assert type(value["providers"]) is list and len(value["providers"]) >= 1
        for provider in value["providers"]:
            c.closed(provider, "source version package_sha256")
            c.digest(provider["package_sha256"])
    elif modo == "policy":
        c.closed(value, "policy_sha256 conftest_version conftest_sha256")
        c.digest(value["policy_sha256"]); c.digest(value["conftest_sha256"])
    else:
        from tests.fixtures.cnesdata_cost import fixture
        cost, _, evidence, manifest = fixture(active=False)
        stage = value
        evidence["source_sha"] = stage["source_sha"]
        manifest["source_sha"] = stage["source_sha"]
        evidence["measurement"]["checkpoint"] = stage
        cost["manifest_sha256"] = c.sha(c.canonical(manifest))
        evidence["measurement"]["manifest_sha256"] = cost["manifest_sha256"]
        evidence["measurement"]["cost"] = {k: v for k, v in cost.items() if k != "evidence_sha256"}
        cost["evidence_sha256"] = c.sha(c.canonical(evidence))
        c.validate_cost(cost, purpose="bootstrap", stage=stage, evidence=evidence,
                        manifest=manifest, now=c.instant("2026-11-07T12:30:00Z"),
                        source_manifest_sha256=evidence["measurement"]["source_manifest_sha256"])
except (ValueError, AssertionError, KeyError) as error:
    print("recusado=" + str(error))
    raise SystemExit(3)
print("aceito")
"""


def _oraculo(modo: str, dado: bytes, raiz: Path) -> str:
    caminho = raiz / f"{modo}.json"
    caminho.write_bytes(dado)
    saida = subprocess.run(  # noqa: S603 -- Fixed interpreter running the pinned IA validators.
        [sys.executable, "-I", "-c", ORACULO, IA, modo, str(caminho)],
        cwd=IA, capture_output=True, text=True, timeout=120, check=False,
    )
    return saida.stdout.strip() or saida.stderr.strip()


def test_oraculo_esta_no_contrato_pinado_do_gate() -> None:
    contrato = Path(IA) / "tools/cnesdata_delivery/contract.py"

    assert hashlib.sha256(contrato.read_bytes()).hexdigest() == CONTRATO_SHA256


def test_gate_real_aceita_o_baseline_toolchain(tmp_path: Path) -> None:
    espelho = criar_espelho(tmp_path)
    tofu = criar_binario(tmp_path, "tofu", TOFU_BIN)

    assert _oraculo("toolchain", db.build_toolchain(lock_texto(), espelho, tofu), tmp_path) == (
        "aceito")


def test_gate_real_recusa_toolchain_com_chave_extra(tmp_path: Path) -> None:
    espelho = criar_espelho(tmp_path)
    tofu = criar_binario(tmp_path, "tofu", TOFU_BIN)
    valor = db.canonical_bytes({**db.decode_json(db.build_toolchain(lock_texto(), espelho, tofu)),
                                "extra": 1})

    assert _oraculo("toolchain", valor, tmp_path) == "recusado=closed object fields"


def test_gate_real_aceita_o_baseline_policy(tmp_path: Path) -> None:
    conftest = criar_binario(tmp_path, "conftest", CONFTEST_BIN)

    assert _oraculo("policy", db.build_policy(POLITICA, conftest), tmp_path) == "aceito"


def test_gate_real_valida_cost_com_o_contexto_do_produtor(tmp_path: Path) -> None:
    repo, source = criar_produto(tmp_path)
    contexto = db.decode_json(db.build_cost_context(repo, source, CUSTO))
    contexto["source_sha"] = "a" * 40

    assert _oraculo("cost", db.canonical_bytes(contexto), tmp_path) == "aceito"


def test_gate_real_recusa_documento_nao_canonico(tmp_path: Path) -> None:
    conftest = criar_binario(tmp_path, "conftest", CONFTEST_BIN)
    bruto = db.build_policy(POLITICA, conftest)
    espacado = bruto.replace(b",", b", ")

    assert _oraculo("policy", espacado, tmp_path) == "recusado=noncanonical JSON bytes"
