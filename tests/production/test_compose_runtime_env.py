"""Configuração de runtime do compose de produção: só o arquivo renderizado em /run."""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

import pytest
import yaml

from tests.production.test_compose_contract import (
    API_SERVICE,
    COMPOSE_PATH,
    RUNTIME_ENV_FILE,
    _violations,
)

if TYPE_CHECKING:
    from collections.abc import Callable

Document = dict[str, Any]
RENDERED_ENV = "/run/cnesdata-production/api.env"


@pytest.fixture
def document() -> Document:
    loaded = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _api(document: Document) -> dict[str, Any]:
    return document["services"][API_SERVICE]


def _relative_env_file(document: Document) -> None:
    _api(document)["env_file"] = [".env"]


def _optional_env_file(document: Document) -> None:
    _api(document)["env_file"] = [{"path": RENDERED_ENV, "required": False}]


def _second_env_file(document: Document) -> None:
    _api(document)["env_file"].append({"path": "/etc/cnesdata-production/api.env"})


def _inline_environment(document: Document) -> None:
    _api(document)["environment"] = {"PROFILE": "legacy"}


def test_compose_le_a_configuracao_de_runtime_so_do_arquivo_renderizado_em_run(
    document: Document,
) -> None:
    api = _api(document)

    assert api["env_file"] == RUNTIME_ENV_FILE == [{"path": RENDERED_ENV, "required": True}]
    assert "environment" not in api
    assert _violations(document) == set()


@pytest.mark.parametrize(
    ("mutate", "violation"),
    [
        pytest.param(_relative_env_file, "runtime_env_file_mismatch", id="env-relativo"),
        pytest.param(_optional_env_file, "runtime_env_file_mismatch", id="env-opcional"),
        pytest.param(_second_env_file, "runtime_env_file_mismatch", id="segundo-env"),
        pytest.param(_inline_environment, "unexpected_service_key", id="environment-inline"),
    ],
)
def test_cada_desvio_da_configuracao_de_runtime_morde(
    document: Document, mutate: Callable[[Document], None], violation: str,
) -> None:
    mutated = copy.deepcopy(document)

    mutate(mutated)

    assert violation in _violations(mutated)
