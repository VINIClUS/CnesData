"""Testes de integridade da evidencia MIG-010: fonte identificada e relatorios imutaveis."""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from data_processor.migration.publication import ShadowRunError
from scripts.run_historical_shadow import main, write_report

_ROOT = Path(__file__).resolve().parents[1]
_TENANT = "354130"


@dataclass(frozen=True)
class _Checkout:
    root: Path
    commit: str


def _git(root: Path, *args: str) -> str:
    git = shutil.which("git")
    assert git is not None
    command = [git, "-C", str(root), *args]
    return subprocess.run(command, capture_output=True, text=True, check=True).stdout.strip()


@pytest.fixture
def checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Checkout:
    root = tmp_path / "fonte"
    root.mkdir()
    _git(root, "init", "--quiet")
    (root / "fonte.py").write_text("VERSAO = 1\n", encoding="utf-8")
    _git(root, "add", "fonte.py")
    _git(
        root, "-c", "user.name=mig010", "-c", "user.email=mig010@example.invalid",
        "-c", "commit.gpgsign=false", "commit", "--quiet", "--no-verify", "--message", "fonte",
    )
    monkeypatch.setattr("scripts.run_historical_shadow._ROOT", root)
    return _Checkout(root, _git(root, "rev-parse", "HEAD"))


def _argv(work: Path) -> list[str]:
    return [
        "--tenant", _TENANT, "--source", "sihd", "--from-competencia", "2026-01",
        "--to-competencia", "2026-01", "--legacy-root", str(_ROOT),
        "--candidate-root", str(work / "candidate"), "--report-root", str(work / "reports"),
    ]


def _assert_nada_gravado(work: Path) -> None:
    assert not (work / "candidate").exists()
    assert not (work / "reports").exists()


def test_agregado_registra_o_commit_do_checkout_limpo(tmp_path: Path, checkout: _Checkout) -> None:
    assert main(_argv(tmp_path)) == 0

    aggregate = json.loads((tmp_path / "reports" / _TENANT / "aggregate.json").read_bytes())
    assert aggregate["accepted"] is True
    assert aggregate["git_commit"] == checkout.commit


@pytest.mark.parametrize("name", ["fonte.py", "nao_rastreado.py"])
def test_recusa_checkout_com_alteracao_nao_commitada(
    tmp_path: Path, checkout: _Checkout, caplog: pytest.LogCaptureFixture, name: str
) -> None:
    (checkout.root / name).write_text("VERSAO = 2\n", encoding="utf-8")

    assert main(_argv(tmp_path)) == 1

    assert "source_tree_dirty entries=1" in caplog.text
    _assert_nada_gravado(tmp_path)


def test_recusa_fonte_fora_de_repositorio_git(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("scripts.run_historical_shadow._ROOT", tmp_path)

    assert main(_argv(tmp_path)) == 1

    assert "source_unidentified reason=git_failed command=rev-parse" in caplog.text
    _assert_nada_gravado(tmp_path)


def test_recusa_execucao_sem_git_disponivel(
    tmp_path: Path, checkout: _Checkout, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("PATH", str(tmp_path / "sem-git"))

    assert main(_argv(tmp_path)) == 1

    assert "source_unidentified reason=git_missing" in caplog.text
    _assert_nada_gravado(tmp_path)


def test_write_report_cria_somente_leitura_e_recusa_sobrescrever(tmp_path: Path) -> None:
    target = tmp_path / "nivel" / "relatorio.json"

    write_report(target, b"{}\n")

    assert target.read_bytes() == b"{}\n"
    assert target.stat().st_mode & 0o222 == 0
    with pytest.raises(ShadowRunError, match=r"report_exists report=relatorio\.json"):
        write_report(target, b"outro")
    assert target.read_bytes() == b"{}\n"
