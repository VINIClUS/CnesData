"""Oráculo opcional: authenticate_adapter_closure real do infra-ansible contra o produtor (P09)."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from shutil import which
from typing import TYPE_CHECKING

import pytest

from scripts.promotion import product_source_manifest as psm
from scripts.promotion._canonical import canonical_bytes
from tests.production.delivery._product_source_fixtures import (
    ARQUIVOS,
    ambiente_git,
    commit,
    criar_repositorio,
    escrever,
    git,
)

if TYPE_CHECKING:
    from pathlib import Path

IA = os.environ.get("CNESDATA_IA_CHECKOUT", "")
pytestmark = pytest.mark.skipif(not IA, reason="oraculo_ia=ausente env=CNESDATA_IA_CHECKOUT")
ORACULO = """
import hashlib, json, sys
sys.path.insert(0, sys.argv[1])
from tools.cnesdata_delivery.application_gate import authenticate_adapter_closure
raw = open(sys.argv[4], "rb").read()
try:
    result = authenticate_adapter_closure(sys.argv[2], sys.argv[3], json.loads(raw),
                                          hashlib.sha256(raw).hexdigest())
except ValueError as error:
    print("recusado=" + str(error))
    raise SystemExit(3)
print("aceito=" + str(len(result)))
"""
ACEITO = f"aceito={len(ARQUIVOS)}"


def _oraculo(produto: Path, fonte: str, manifesto: bytes) -> str:
    caminho = produto.parent / "manifesto-oraculo.json"
    caminho.write_bytes(manifesto)
    saida = subprocess.run(  # noqa: S603 -- Fixed interpreter running the pinned IA consumer.
        [sys.executable, "-I", "-c", ORACULO, IA, str(produto), fonte, str(caminho)],
        cwd=IA,
        env=ambiente_git(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    return saida.stdout.strip() or saida.stderr.strip()


def _com_arquivos(fonte: str, arquivos: dict[str, str]) -> bytes:
    return canonical_bytes({"version": 1, "source_sha": fonte, "files": arquivos}, 4 * 1024**2)


def _arquivos(repo: Path, fonte: str) -> dict[str, str]:
    return json.loads(psm.build_manifest(repo, fonte))["files"]


def test_oraculo_esta_no_pin_do_gate() -> None:
    head = subprocess.run(  # noqa: S603 -- Fixed git argv over the IA checkout.
        [which("git") or "git", "rev-parse", "HEAD"], cwd=IA, capture_output=True, text=True,
        check=True,
    ).stdout.strip()

    assert head == "f6cab21eb88d5b5cb87958b4fac44af8e10157c9"


def test_gate_real_aceita_os_bytes_do_gerador_contra_o_checkout(tmp_path: Path) -> None:
    repo, fonte = criar_repositorio(tmp_path)

    assert _oraculo(repo, fonte, psm.build_manifest(repo, fonte)) == ACEITO


def test_gate_real_recusa_arquivo_extra_nao_rastreado_no_checkout(tmp_path: Path) -> None:
    repo, fonte = criar_repositorio(tmp_path)
    manifesto = psm.build_manifest(repo, fonte)
    escrever(repo, "extra.txt", b"nao rastreado\n")

    assert _oraculo(repo, fonte, manifesto) == "recusado=dirty or shadowed adapter closure"


def test_gate_real_recusa_arquivo_ausente_do_checkout(tmp_path: Path) -> None:
    repo, fonte = criar_repositorio(tmp_path)
    manifesto = psm.build_manifest(repo, fonte)
    (repo / "README.md").unlink()

    assert _oraculo(repo, fonte, manifesto) == "recusado=dirty or shadowed adapter closure"


def test_gate_real_recusa_manifesto_com_arquivo_extra(tmp_path: Path) -> None:
    repo, fonte = criar_repositorio(tmp_path)
    arquivos = {**_arquivos(repo, fonte), "extra.txt": "0" * 64}

    assert _oraculo(repo, fonte, _com_arquivos(fonte, arquivos)) == (
        "recusado=complete adapter source closure")


def test_gate_real_recusa_manifesto_sem_um_arquivo(tmp_path: Path) -> None:
    repo, fonte = criar_repositorio(tmp_path)
    arquivos = _arquivos(repo, fonte)
    del arquivos["README.md"]

    assert _oraculo(repo, fonte, _com_arquivos(fonte, arquivos)) == (
        "recusado=adapter source mode/path/inventory")


def test_gate_real_recusa_mudanca_de_modo_no_checkout(tmp_path: Path) -> None:
    repo, fonte = criar_repositorio(tmp_path)
    manifesto = psm.build_manifest(repo, fonte)
    (repo / "README.md").chmod(0o755)

    assert _oraculo(repo, fonte, manifesto) == "recusado=dirty or shadowed adapter closure"


def test_gate_real_recusa_manifesto_anterior_a_mudanca_de_modo_commitada(tmp_path: Path) -> None:
    repo, inicial = criar_repositorio(tmp_path)
    anterior = psm.build_manifest(repo, inicial)
    (repo / "README.md").chmod(0o755)
    fonte = commit(repo, "modo")
    atual = psm.build_manifest(repo, fonte)

    assert json.loads(atual)["files"] == json.loads(anterior)["files"]
    assert _oraculo(repo, fonte, anterior) == "recusado=original adapter source manifest"
    assert _oraculo(repo, fonte, atual) == ACEITO


def test_gate_real_recusa_symlink_que_o_gerador_recusa(tmp_path: Path) -> None:
    repo, inicial = criar_repositorio(tmp_path)
    arquivos = _arquivos(repo, inicial)
    (repo / "link").symlink_to("README.md")
    fonte = commit(repo, "symlink")
    arquivos["link"] = hashlib.sha256(b"README.md").hexdigest()

    with pytest.raises(ValueError, match="product_source=entry path=link mode=120000"):
        psm.build_manifest(repo, fonte)
    assert _oraculo(repo, fonte, _com_arquivos(fonte, arquivos)) == (
        "recusado=adapter source mode/path/inventory")


def test_gate_real_recusa_submodulo_que_o_gerador_recusa(tmp_path: Path) -> None:
    repo, inicial = criar_repositorio(tmp_path)
    arquivos = {**_arquivos(repo, inicial), "vendor/sub": "0" * 64}
    git(repo, "update-index", "--add", "--cacheinfo", f"160000,{inicial},vendor/sub")
    fonte = commit(repo, "submodulo", adicionar=False)
    (repo / "vendor/sub").mkdir(parents=True)

    with pytest.raises(ValueError, match="product_source=entry path=vendor/sub mode=160000"):
        psm.build_manifest(repo, fonte)
    assert _oraculo(repo, fonte, _com_arquivos(fonte, arquivos)) == (
        "recusado=adapter source mode/path/inventory")


def test_gate_real_recusa_arquivo_acima_de_8_mib_que_o_gerador_recusa(tmp_path: Path) -> None:
    grande = b"a" * (8 * 1024**2 + 1)
    repo, inicial = criar_repositorio(tmp_path)
    arquivos = {**_arquivos(repo, inicial), "big.bin": hashlib.sha256(grande).hexdigest()}
    escrever(repo, "big.bin", grande)
    fonte = commit(repo, "grande")

    with pytest.raises(ValueError, match=re.escape("product_source=file_size path=big.bin")):
        psm.build_manifest(repo, fonte)
    assert _oraculo(repo, fonte, _com_arquivos(fonte, arquivos)) == (
        "recusado=adapter real bounded source file")
