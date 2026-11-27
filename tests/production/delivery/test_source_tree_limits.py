"""Contrato da arvore rastreada: limites lidos pelo gate de aplicacao da entrega."""

from __future__ import annotations

import re
import subprocess
from shutil import which

LIMITE_ARQUIVO = 8 * 1024**2
NOME_PERMITIDO = re.compile(r"[A-Za-z0-9_.@/-]+")


def _arvore_rastreada() -> list[tuple[str, int]]:
    saida = subprocess.run(  # noqa: S603 -- Fixed git ls-tree argv.
        [which("git") or "git", "ls-tree", "-r", "-l", "-z", "HEAD"],
        check=True,
        capture_output=True,
    ).stdout
    entradas: list[tuple[str, int]] = []
    for registro in saida.split(b"\0")[:-1]:
        metadados, nome = registro.split(b"\t", 1)
        tamanho = metadados.split()[3]
        entradas.append((nome.decode("utf-8"), int(tamanho)))
    return entradas


def test_nenhum_arquivo_rastreado_passa_de_8_mib() -> None:
    acima = [
        f"{nome} size={tamanho}"
        for nome, tamanho in _arvore_rastreada()
        if tamanho > LIMITE_ARQUIVO
    ]

    assert acima == []


def test_todo_nome_rastreado_respeita_o_alfabeto_do_gate() -> None:
    invalidos = [nome for nome, _ in _arvore_rastreada() if not NOME_PERMITIDO.fullmatch(nome)]

    assert invalidos == []
