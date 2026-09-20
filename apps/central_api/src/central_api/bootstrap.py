"""Cria o primeiro usuário + membership de um `data_dir` local novo.

Um `data_dir` recém-criado só recebe a linha `Tenant` (via `_seed_tenant` na
composition root, chamada a cada boot). Sem usuário + membership, nenhum login
é possível no profile local — este módulo fecha essa lacuna.

CLI: `python -m central_api.bootstrap --email ...` — roda dentro do container
`central-api-local`, como o usuário `app`, contra o `data_dir` montado em `/data`.
Senha nunca em argv: lê de `LOCAL_BOOTSTRAP_PASSWORD` ou prompt interativo.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, datetime
from getpass import getpass
from typing import TYPE_CHECKING

from cnes_domain.control_plane.entities import Membership
from cnes_domain.profiles import ProfileSettings, parse_profile
from cnes_infra.auth.local_credentials import LocalCredentialStore, build_user
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)

_DEFAULT_ROLE = "gestor"


def _resolve_password(env: Mapping[str, str]) -> str:
    password = env.get("LOCAL_BOOTSTRAP_PASSWORD")
    if password:
        return password
    return getpass("Senha do primeiro usuário: ")


def bootstrap_user(
    settings: ProfileSettings, email: str, user_id: str, role: str, password: str,
) -> None:
    """Args: settings, email, user_id, role, password.
    Raises: CredentialRejected: Quando a senha está fora de 12-128 caracteres.
    """
    now = datetime.now(UTC)
    credentials = LocalCredentialStore(settings.state_db)
    credentials.initialize()
    credentials.put_user(build_user(user_id, email, password, now))
    control_plane = SQLiteControlPlane(settings.state_db, lambda: now)
    control_plane.initialize()
    control_plane.put_membership(Membership(
        tenant_id=settings.tenant_id, user_id=user_id, role=role, created_at=now,
        oidc_issuer=None,
    ))
    logger.info(
        "local_bootstrap_user_created user_id=%s email=%s tenant_id=%s role=%s",
        user_id, email, settings.tenant_id, role,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m central_api.bootstrap")
    parser.add_argument("--email", required=True)
    parser.add_argument("--user-id", default="user-1")
    parser.add_argument("--role", default=_DEFAULT_ROLE)
    return parser


def main(argv: list[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    resolved_env = env if env is not None else os.environ
    settings = parse_profile(dict(resolved_env))
    password = _resolve_password(resolved_env)
    bootstrap_user(settings, args.email, args.user_id, args.role, password)
    return 0


__all__ = ["bootstrap_user", "main"]


if __name__ == "__main__":
    sys.exit(main())
