"""TDD do bootstrap de primeiro usuário do profile local."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from central_api.bootstrap import _resolve_password, bootstrap_user, main
from cnes_domain.profiles import parse_profile
from cnes_infra.auth.local_credentials import (
    CredentialRejected,
    LocalCredentialStore,
    hash_password,
)
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

if TYPE_CHECKING:
    from pathlib import Path

_EMAIL = "gestor@epitacio.sp.gov.br"
_PASSWORD = "correct-horse-battery"  # noqa: S105
_TENANT = "354130"


def _settings(tmp_path: Path):
    return parse_profile({"TENANT_ID": _TENANT, "DATA_DIR": str(tmp_path)})


def test_cria_usuario_e_membership_no_data_dir_novo(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    bootstrap_user(settings, _EMAIL, "user-1", "gestor", _PASSWORD)

    credentials = LocalCredentialStore(settings.state_db)
    user = credentials.find_user_by_email(_EMAIL)
    assert user is not None
    assert user.user_id == "user-1"
    assert user.email == _EMAIL
    assert hash_password(_PASSWORD, user.salt) == user.password_hash

    control_plane = SQLiteControlPlane(settings.state_db, lambda: user.created_at)
    membership = control_plane.get_membership(_TENANT, "user-1")
    assert membership is not None
    assert membership.tenant_id == _TENANT
    assert membership.user_id == "user-1"
    assert membership.role == "gestor"
    assert membership.oidc_issuer is None


def test_rejeita_senha_curta(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    with pytest.raises(CredentialRejected, match="password_length"):
        bootstrap_user(settings, _EMAIL, "user-1", "gestor", "curta")


def test_reexecucao_e_idempotente(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    bootstrap_user(settings, _EMAIL, "user-1", "gestor", _PASSWORD)
    bootstrap_user(settings, _EMAIL, "user-1", "gestor", _PASSWORD)

    control_plane = SQLiteControlPlane(settings.state_db, lambda: datetime.now(UTC))
    assert control_plane.get_membership(_TENANT, "user-1") is not None


def test_resolve_password_prefere_variavel_de_ambiente() -> None:
    assert _resolve_password({"LOCAL_BOOTSTRAP_PASSWORD": "from-env"}) == "from-env"


def test_resolve_password_cai_para_prompt_quando_variavel_ausente(monkeypatch) -> None:
    monkeypatch.setattr("central_api.bootstrap.getpass", lambda prompt: "from-prompt")

    assert _resolve_password({}) == "from-prompt"


def test_main_cria_usuario_via_env_e_argv(tmp_path: Path) -> None:
    env = {
        "TENANT_ID": _TENANT, "DATA_DIR": str(tmp_path), "LOCAL_BOOTSTRAP_PASSWORD": _PASSWORD,
    }

    exit_code = main(["--email", _EMAIL], env=env)

    assert exit_code == 0
    settings = _settings(tmp_path)
    credentials = LocalCredentialStore(settings.state_db)
    assert credentials.find_user_by_email(_EMAIL) is not None


def test_main_exige_email(tmp_path: Path) -> None:
    env = {"TENANT_ID": _TENANT, "DATA_DIR": str(tmp_path)}

    with pytest.raises(SystemExit):
        main([], env=env)
