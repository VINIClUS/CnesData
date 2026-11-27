"""Árvore, lock, espelho de provedores e binários sintéticos para os baselines (P23)."""

from __future__ import annotations

import hashlib
import textwrap
from typing import TYPE_CHECKING

from tests.production.delivery._product_source_fixtures import commit, criar_repositorio, escrever

if TYPE_CHECKING:
    from pathlib import Path

LOCK = "infra/opentofu/env/prod/.terraform.lock.hcl"
POLICY = "policies/cnesdata.rego"
COST = "infra/opentofu/modules/observability/cost-manifest.json"
REGISTRY = "registry.opentofu.org/hashicorp"
PROVEDORES = {"archive": ("2.8.1", b"zip-archive\n"), "aws": ("6.54.0", b"zip-aws\n")}
POLITICA = b"package cnesdata\n"
CUSTO = b'{"schema_version": 1, "lines": []}\n'
TOFU_BIN = b"#!/bin/sh\necho '{\"terraform_version\":\"1.13.2\"}'\n"
CONFTEST_BIN = b"#!/bin/sh\necho 'Conftest: 0.71.1'\necho 'OPA: 1.21.1'\n"


def sha(dado: bytes) -> str:
    return hashlib.sha256(dado).hexdigest()


def _bloco(nome: str, versao: str, zip_: bytes, extra: tuple[str, ...]) -> str:
    hashes = sorted((f"h1:{nome}ABC=", f"zh:{sha(zip_)}", *extra))
    linhas = "".join(f'    "{item}",\n' for item in hashes)
    return (f'provider "{REGISTRY}/{nome}" {{\n  version     = "{versao}"\n'
            f'  constraints = "~> 1.0"\n  hashes = [\n{linhas}  ]\n}}\n')


def lock_texto(provedores: dict[str, tuple[str, bytes]] | None = None) -> bytes:
    itens = PROVEDORES if provedores is None else provedores
    cabecalho = textwrap.dedent("""\
        # This file is maintained automatically by "tofu init".
        # Manual edits may be lost in future updates.

        """)
    corpo = "\n".join(_bloco(n, v, z, ("zh:" + "0" * 64,)) for n, (v, z) in itens.items())
    return (cabecalho + corpo).encode()


def criar_espelho(raiz: Path, provedores: dict[str, tuple[str, bytes]] | None = None) -> Path:
    espelho = raiz / "espelho"
    for nome, (versao, zip_) in (PROVEDORES if provedores is None else provedores).items():
        pasta = espelho / REGISTRY / nome
        pasta.mkdir(parents=True)
        (pasta / f"terraform-provider-{nome}_{versao}_linux_amd64.zip").write_bytes(zip_)
        (pasta / "index.json").write_bytes(b"{}")
    return espelho


def criar_binario(raiz: Path, nome: str, conteudo: bytes) -> Path:
    caminho = raiz / "bin" / nome
    caminho.parent.mkdir(exist_ok=True)
    caminho.write_bytes(conteudo)
    caminho.chmod(0o755)
    return caminho


def criar_produto(raiz: Path) -> tuple[Path, str]:
    repo, _ = criar_repositorio(raiz)
    escrever(repo, LOCK, lock_texto())
    escrever(repo, POLICY, POLITICA)
    escrever(repo, COST, CUSTO)
    escrever(repo, "infra/opentofu/env/prod/main.tf", b"# main\n")
    return repo, commit(repo, "baselines")
