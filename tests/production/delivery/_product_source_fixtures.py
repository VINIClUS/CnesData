"""Repositórios git sintéticos e determinísticos para o manifesto de fonte do produto."""

from __future__ import annotations

import os
import subprocess
from shutil import which
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

DATA = "2026-11-20T12:00:00+00:00"
ADAPTER = "scripts/promotion/promote_api.py"
ARQUIVOS: dict[str, bytes] = {
    ADAPTER: b"ADAPTER = 1\n",
    "README.md": "produto sintético\n".encode(),
    "@scope/x-y_z.v1.txt": b"alfabeto do gate\n",
    "bin/run.sh": b"#!/bin/sh\ntrue\n",
}
EXECUTAVEIS = frozenset({"bin/run.sh"})


def ambiente_git() -> dict[str, str]:
    identidade = {"NAME": "Produto", "EMAIL": "produto@example.invalid", "DATE": DATA}
    return {
        **os.environ,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_LFS_SKIP_SMUDGE": "1",
        **{f"GIT_{papel}_{campo}": valor
           for papel in ("AUTHOR", "COMMITTER") for campo, valor in identidade.items()},
    }


def git(repo: Path, *args: str) -> str:
    return subprocess.run(  # noqa: S603 -- Fixed git argv over a synthetic repository.
        [which("git") or "git", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null",
         *args],
        cwd=repo,
        env=ambiente_git(),
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()


def escrever(repo: Path, nome: str, conteudo: bytes, executavel: bool = False) -> None:
    caminho = repo / nome
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_bytes(conteudo)
    caminho.chmod(0o755 if executavel else 0o644)


def commit(repo: Path, mensagem: str, adicionar: bool = True) -> str:
    if adicionar:
        git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", mensagem)
    return git(repo, "rev-parse", "HEAD")


def criar_repositorio(raiz: Path, arquivos: dict[str, bytes] | None = None) -> tuple[Path, str]:
    repo = raiz / "produto"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    for nome, conteudo in (ARQUIVOS if arquivos is None else arquivos).items():
        escrever(repo, nome, conteudo, nome in EXECUTAVEIS)
    return repo, commit(repo, "inicial")
