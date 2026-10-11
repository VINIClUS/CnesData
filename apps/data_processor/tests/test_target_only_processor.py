"""Write-fence MIG-012: o processor só compõe os perfis local e aws e falha fechado."""
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import logging
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cnes_infra.storage import extractions_repo
from cnes_infra.storage.s3_presigned import S3PresignedStorage
from data_processor.main import main

if TYPE_CHECKING:
    from pathlib import Path

_TIMEOUT_SECONDS = 5


def _tripwire(label: str) -> MagicMock:
    return MagicMock(name=label, side_effect=AssertionError(f"legacy_write label={label}"))


def _repo_functions() -> list[str]:
    return [
        name
        for name, function in inspect.getmembers(extractions_repo, inspect.isfunction)
        if not name.startswith("_") and function.__module__ == extractions_repo.__name__
    ]


def _storage_methods() -> list[str]:
    public = [name for name, _ in inspect.getmembers(S3PresignedStorage, inspect.isfunction)]
    return ["__init__", *(name for name in public if not name.startswith("_"))]


@pytest.fixture
def spies(monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
    named = {"sqlalchemy.create_engine": _tripwire("sqlalchemy.create_engine")}
    monkeypatch.setattr("sqlalchemy.create_engine", named["sqlalchemy.create_engine"])
    for name in _repo_functions():
        named[f"extractions_repo.{name}"] = _tripwire(f"extractions_repo.{name}")
        monkeypatch.setattr(extractions_repo, name, named[f"extractions_repo.{name}"])
    for name in _storage_methods():
        named[f"S3PresignedStorage.{name}"] = _tripwire(f"S3PresignedStorage.{name}")
        monkeypatch.setattr(S3PresignedStorage, name, named[f"S3PresignedStorage.{name}"])
    return named


def _fired(spies: dict[str, MagicMock]) -> dict[str, int]:
    return {label: spy.call_count for label, spy in spies.items() if spy.call_count}


def _set_profile(monkeypatch: pytest.MonkeyPatch, profile: str | None) -> None:
    if profile is None:
        monkeypatch.delenv("PROFILE", raising=False)
    else:
        monkeypatch.setenv("PROFILE", profile)


@pytest.mark.parametrize("profile", [None, "legacy", "vps"], ids=["ausente", "legacy", "vps"])
async def test_rejeita_perfil_fora_de_local_e_aws_sem_tocar_legado(
    spies: dict[str, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    profile: str | None,
) -> None:
    _set_profile(monkeypatch, profile)
    caplog.set_level(logging.ERROR, logger="data_processor.main")

    with (
        patch("data_processor.main._setup_logging"),
        patch("data_processor.main.init_telemetry"),
    ):
        rc = await asyncio.wait_for(main(), timeout=_TIMEOUT_SECONDS)

    assert rc != 0
    assert [r.getMessage() for r in caplog.records if r.name == "data_processor.main"] == [
        f"profile_required profile={profile or ''}",
    ]
    assert _fired(spies) == {}


async def test_perfil_local_continua_compondo_sem_tocar_legado(
    spies: dict[str, MagicMock], monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("PROFILE", "local")
    monkeypatch.setenv("TENANT_ID", "354130")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    with (
        patch("data_processor.main._setup_logging"),
        patch("data_processor.main.init_telemetry"),
        patch("data_processor.main._poll_until_shutdown", new_callable=AsyncMock) as poll,
    ):
        rc = await asyncio.wait_for(main(), timeout=_TIMEOUT_SECONDS)

    assert rc == 0
    poll.assert_awaited_once()
    assert _fired(spies) == {}


@pytest.mark.parametrize("module", ["data_processor.poll", "data_processor.consumer"])
def test_modulos_da_fila_legada_nao_existem_mais(module: str) -> None:
    assert importlib.util.find_spec(module) is None
