"""Baseline estrutural dos documentos reconciliados."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.negative

REPOSITORY_ROOT = Path(__file__).parents[2]
RECONCILED_PATHS = (
    ".env.example",
    "README.md",
    "docs/architecture.md",
    "docs/project-context.md",
    "docs/roadmap.md",
)


def _has_conflict_marker(content: str) -> bool:
    return any(
        line == "=======" or line.startswith(("<<<<<<<", ">>>>>>>"))
        for line in content.splitlines()
    )


def test_mantem_documentacao_de_desenvolvimento() -> None:
    assert (REPOSITORY_ROOT / "docs/development.md").is_file()


def test_mantem_ca_local_como_configuracao_opcional() -> None:
    content = (REPOSITORY_ROOT / ".env.example").read_text(encoding="utf-8")
    active_settings = {
        line.split("=", maxsplit=1)[0]
        for line in content.splitlines()
        if line and not line.startswith("#")
    }

    assert active_settings.isdisjoint({"AUTH_CA_CERT_PATH", "AUTH_CA_KEY_PATH"})


@pytest.mark.parametrize("relative_path", RECONCILED_PATHS)
def test_remove_marcadores_de_conflito(relative_path: str) -> None:
    content = (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")

    assert not _has_conflict_marker(content)


@pytest.mark.parametrize(
    "expected_setting",
    [
        "COD_MUN_IBGE=354130",
        "ID_MUNICIPIO_IBGE7=3541307",
        "CNPJ_MANTENEDORA=55293427000117",
    ],
)
def test_preserva_identificadores_do_municipio(expected_setting: str) -> None:
    content = (REPOSITORY_ROOT / ".env.example").read_text(encoding="utf-8")

    assert expected_setting in content.splitlines()
