"""Produtor do manifesto de fonte do produto: bytes aceitos pelo gate de aplicação (P09)."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import subprocess
from pathlib import Path
from shutil import which
from typing import Any

import pytest

from scripts.promotion import product_source_manifest as psm
from tests.production.delivery._product_source_fixtures import (
    ADAPTER,
    ARQUIVOS,
    commit,
    criar_repositorio,
    escrever,
    git,
)

ROOT = Path(__file__).resolve().parents[3]
VETOR = Path(__file__).with_name("product_source_manifest_vector.json")
GATE_PIN = "f6cab21eb88d5b5cb87958b4fac44af8e10157c9"
VETOR_SOURCE = "cb84957c19593b03485531cd777a0c452f5bfe32"
VETOR_SHA256 = "2b7fb8481410fd7e9d0e6f68a0bd4601aeea79117a42039e207e119df3cd8389"


def _manifesto(repo: Path, source: str) -> dict[str, Any]:
    return json.loads(psm.build_manifest(repo, source))


def test_limites_e_membro_espelham_o_consumidor_no_pin() -> None:
    assert len(GATE_PIN) == 40
    assert psm.MEMBER == "product-source-manifest.json"
    assert psm.ADAPTER == ADAPTER
    assert (psm.FILE_LIMIT, psm.AGGREGATE_LIMIT) == (8 * 1024**2, 128 * 1024**2)
    assert (psm.TREE_LIMIT, psm.MANIFEST_LIMIT, psm.MAX_FILES) == (4 * 1024**2, 4 * 1024**2, 16384)


def test_gerador_reproduz_os_bytes_aceitos_pelo_gate_no_pin(tmp_path: Path) -> None:
    repo, source = criar_repositorio(tmp_path)
    esperado = VETOR.read_bytes()

    gerado = psm.build_manifest(repo, source)

    assert source == VETOR_SOURCE
    assert hashlib.sha256(esperado).hexdigest() == VETOR_SHA256
    assert gerado == esperado


def test_manifesto_e_canonico_com_sha256_de_cada_blob_em_s(tmp_path: Path) -> None:
    repo, source = criar_repositorio(tmp_path)

    gerado = psm.build_manifest(repo, source)
    valor = json.loads(gerado)

    assert gerado.endswith(b"}\n")
    assert b" " not in gerado
    assert gerado.count(b"\n") == 1
    assert list(valor) == ["files", "source_sha", "version"]
    assert valor["version"] == 1
    assert valor["source_sha"] == source
    assert valor["files"] == {nome: hashlib.sha256(dado).hexdigest()
                              for nome, dado in ARQUIVOS.items()}


def test_arvore_real_do_produto_gera_manifesto_completo() -> None:
    git_bin = which("git") or "git"
    head = subprocess.run(  # noqa: S603 -- Fixed git argv.
        [git_bin, "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, check=True, text=True
    ).stdout.strip()
    nomes = subprocess.run(  # noqa: S603 -- Fixed git argv.
        [git_bin, "ls-tree", "-r", "-z", "--name-only", head], cwd=ROOT, capture_output=True,
        check=True,
    ).stdout.split(b"\0")[:-1]

    valor = _manifesto(ROOT, head)

    assert valor["source_sha"] == head
    assert sorted(valor["files"]) == sorted(nome.decode() for nome in nomes)
    assert ADAPTER in valor["files"]


def test_aceita_arquivo_de_exatamente_8_mib(tmp_path: Path) -> None:
    dado = b"a" * (8 * 1024**2)
    repo, source = criar_repositorio(tmp_path, {**ARQUIVOS, "limite.bin": dado})

    assert _manifesto(repo, source)["files"]["limite.bin"] == hashlib.sha256(dado).hexdigest()


def test_recusa_arquivo_acima_de_8_mib(tmp_path: Path) -> None:
    repo, source = criar_repositorio(tmp_path, {**ARQUIVOS, "big.bin": b"a" * (8 * 1024**2 + 1)})

    with pytest.raises(ValueError, match=re.escape("product_source=file_size path=big.bin")):
        psm.build_manifest(repo, source)


def test_recusa_symlink_rastreado(tmp_path: Path) -> None:
    repo, _ = criar_repositorio(tmp_path)
    (repo / "link").symlink_to("README.md")
    source = commit(repo, "symlink")

    with pytest.raises(ValueError, match="product_source=entry path=link mode=120000"):
        psm.build_manifest(repo, source)


def test_recusa_submodulo_rastreado(tmp_path: Path) -> None:
    repo, inicial = criar_repositorio(tmp_path)
    git(repo, "update-index", "--add", "--cacheinfo", f"160000,{inicial},vendor/sub")
    source = commit(repo, "submodulo", adicionar=False)

    with pytest.raises(ValueError, match="product_source=entry path=vendor/sub mode=160000"):
        psm.build_manifest(repo, source)


def test_mudanca_de_modo_gera_novo_s_com_os_mesmos_arquivos(tmp_path: Path) -> None:
    repo, inicial = criar_repositorio(tmp_path)
    antes = _manifesto(repo, inicial)
    (repo / "README.md").chmod(0o755)
    depois_sha = commit(repo, "modo")

    depois = _manifesto(repo, depois_sha)

    assert depois_sha != inicial
    assert git(repo, "ls-tree", depois_sha, "README.md").startswith("100755 blob ")
    assert depois["files"] == antes["files"]
    assert depois["source_sha"] == depois_sha


def test_arquivo_extra_ou_alterado_fora_de_s_nao_entra_no_manifesto(tmp_path: Path) -> None:
    repo, source = criar_repositorio(tmp_path)
    original = psm.build_manifest(repo, source)
    escrever(repo, "extra.txt", b"nao rastreado\n")
    escrever(repo, "README.md", b"alterado no checkout\n")
    (repo / "bin/run.sh").unlink()

    assert psm.build_manifest(repo, source) == original == VETOR.read_bytes()


def test_recusa_nome_fora_do_alfabeto_do_gate(tmp_path: Path) -> None:
    repo, source = criar_repositorio(tmp_path, {**ARQUIVOS, "com espaco.txt": b"x\n"})

    with pytest.raises(ValueError, match=re.escape("product_source=path path='com espaco.txt'")):
        psm.build_manifest(repo, source)


def test_recusa_arvore_sem_o_adapter_fixo(tmp_path: Path) -> None:
    arquivos = {nome: dado for nome, dado in ARQUIVOS.items() if nome != ADAPTER}
    repo, source = criar_repositorio(tmp_path, arquivos)

    with pytest.raises(ValueError, match="product_source=adapter_missing"):
        psm.build_manifest(repo, source)


def test_recusa_checkout_que_nao_esta_em_s(tmp_path: Path) -> None:
    repo, inicial = criar_repositorio(tmp_path)
    escrever(repo, "novo.txt", b"novo\n")
    commit(repo, "seguinte")

    with pytest.raises(ValueError, match="product_source=checkout_head"):
        psm.build_manifest(repo, inicial)


@pytest.mark.parametrize("source", ["HEAD", "A" * 40, "a" * 39, "main"])
def test_recusa_source_sha_que_nao_e_commit_completo(tmp_path: Path, source: str) -> None:
    repo, _ = criar_repositorio(tmp_path)

    with pytest.raises(ValueError, match="product_source=source_sha"):
        psm.build_manifest(repo, source)


@pytest.mark.parametrize(
    ("limite", "valor", "motivo"),
    [
        ("AGGREGATE_LIMIT", 30, "product_source=aggregate_size"),
        ("MAX_FILES", 3, "product_source=file_count"),
        ("TREE_LIMIT", 64, "product_source=tree_size"),
        ("MANIFEST_LIMIT", 256, "json=output_size"),
    ],
)
def test_recusa_inventario_acima_dos_limites_do_consumidor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limite: str, valor: int, motivo: str
) -> None:
    repo, source = criar_repositorio(tmp_path)
    monkeypatch.setattr(psm, limite, valor)

    with pytest.raises(ValueError, match=motivo):
        psm.build_manifest(repo, source)


def test_cli_grava_so_o_membro_e_registra_o_sha256(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    repo, source = criar_repositorio(tmp_path)
    saida = tmp_path / "artefato" / psm.MEMBER
    caplog.set_level(logging.INFO)

    codigo = psm.main(["--checkout", str(repo), "--source-sha", source, "--output", str(saida)])

    assert codigo == 0
    assert saida.read_bytes() == VETOR.read_bytes()
    assert [p.name for p in saida.parent.iterdir()] == [psm.MEMBER]
    assert f"product_source_manifest=written sha256={VETOR_SHA256} files=4" in caplog.text


@pytest.mark.parametrize("nome", ["manifest.json", "product-source-manifest.json.tmp"])
def test_cli_recusa_nome_de_membro_diferente(tmp_path: Path, nome: str) -> None:
    repo, source = criar_repositorio(tmp_path)
    saida = tmp_path / nome

    codigo = psm.main(["--checkout", str(repo), "--source-sha", source, "--output", str(saida)])

    assert codigo == 2
    assert not saida.exists()


def test_cli_recusa_saida_existente(tmp_path: Path) -> None:
    repo, source = criar_repositorio(tmp_path)
    saida = tmp_path / psm.MEMBER
    saida.write_bytes(b"anterior\n")

    codigo = psm.main(["--checkout", str(repo), "--source-sha", source, "--output", str(saida)])

    assert codigo == 2
    assert saida.read_bytes() == b"anterior\n"


def test_cli_retorna_2_sem_arquivo_quando_a_arvore_e_recusada(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    repo, source = criar_repositorio(tmp_path, {**ARQUIVOS, "big.bin": b"a" * (8 * 1024**2 + 1)})
    saida = tmp_path / psm.MEMBER

    codigo = psm.main(["--checkout", str(repo), "--source-sha", source, "--output", str(saida)])

    assert codigo == 2
    assert not saida.exists()
    assert "product_source_manifest=failed reason=product_source=file_size" in caplog.text


def test_cli_retorna_2_quando_o_checkout_nao_e_repositorio(tmp_path: Path) -> None:
    saida = tmp_path / "saida" / psm.MEMBER

    codigo = psm.main(["--checkout", str(tmp_path), "--source-sha", "a" * 40, "--output",
                       str(saida)])

    assert codigo == 2
    assert not saida.exists()
