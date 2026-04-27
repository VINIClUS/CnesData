"""Dependências compartilhadas da API (engine, minio wrapper, reaper)."""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException
from sqlalchemy import create_engine
from starlette.requests import Request  # noqa: TC002 - needed at runtime by FastAPI

from central_api.middleware import AuthenticatedUser
from cnes_infra import config
from cnes_infra.storage import extractions_repo
from cnes_infra.storage.query_counter import install_query_counter
from cnes_infra.storage.rls import install_rls_listener
from cnes_infra.telemetry import instrument_engine

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

_engine: Engine | None = None
_REAPER_INTERVAL = 60


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(config.DB_URL)
    return _engine


def get_conn() -> Iterator[Connection]:
    engine = get_engine()
    with engine.begin() as conn:
        yield conn


@dataclass
class MinioWrapper:
    bucket: str
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool

    def presigned_put(self, key: str, expires: int = 3600) -> str:
        from minio import Minio
        client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )
        return client.presigned_put_object(
            bucket_name=self.bucket,
            object_name=key,
            expires=timedelta(seconds=expires),
        )


def get_minio() -> MinioWrapper:
    return MinioWrapper(
        bucket=config.MINIO_BUCKET,
        endpoint=config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE,
    )


async def _lease_reaper_loop(engine: Engine) -> None:
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(_REAPER_INTERVAL)
        try:
            count = await loop.run_in_executor(
                None, _reap_expired_sync, engine,
            )
            if count > 0:
                logger.info("leases_reaped count=%d", count)
        except Exception:
            logger.exception("reaper_error")


def _reap_expired_sync(engine: Engine) -> int:
    with engine.begin() as conn:
        return extractions_repo.reap_expired(conn)


def require_auth(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="auth_required")
    return user


def require_tenant_header(
    request: Request,
    user: AuthenticatedUser = Depends(require_auth),
) -> str:
    tid = request.headers.get("X-Tenant-Id")
    if not tid:
        raise HTTPException(status_code=400, detail="tenant_header_required")
    if tid not in user.tenant_ids:
        raise HTTPException(status_code=403, detail="tenant_not_allowed")
    return tid


@asynccontextmanager
async def lifespan(app: object) -> AsyncGenerator[None]:
    global _engine
    _db_url = os.environ.get("DB_URL") or config.DB_URL
    _engine = create_engine(_db_url)
    install_rls_listener(_engine)
    install_query_counter(_engine)
    instrument_engine(_engine)

    from central_api.repositories.dashboard_repo import DashboardRepo
    from cnes_infra.auth import (
        AccessTokenStore,
        CertAuthority,
        DeviceCodeStore,
        JWKSValidator,
        ProvisionedCertsRepo,
        RefreshTokenStore,
    )

    if config.DASHBOARD_OIDC_ISSUER:
        app.state.jwt_validator = JWKSValidator(  # type: ignore[attr-defined]
            issuer=config.DASHBOARD_OIDC_ISSUER,
            audience=config.DASHBOARD_OIDC_AUDIENCE,
        )
    else:
        app.state.jwt_validator = None  # type: ignore[attr-defined]
    app.state.dashboard_repo = DashboardRepo(_engine)  # type: ignore[attr-defined]
    app.state.device_code_store = DeviceCodeStore()  # type: ignore[attr-defined]
    app.state.access_token_store = AccessTokenStore()  # type: ignore[attr-defined]
    app.state.refresh_token_store = RefreshTokenStore(_engine)  # type: ignore[attr-defined]
    app.state.provisioned_certs = ProvisionedCertsRepo(_engine)  # type: ignore[attr-defined]
    _ca_cert_path = os.environ.get("AUTH_CA_CERT_PATH", "")
    _ca_key_path = os.environ.get("AUTH_CA_KEY_PATH", "")
    if _ca_cert_path and _ca_key_path:
        from pathlib import Path
        app.state.cert_authority = CertAuthority(  # type: ignore[attr-defined]
            root_cert_pem=Path(_ca_cert_path).read_bytes(),
            root_key_pem=Path(_ca_key_path).read_bytes(),
        )
    else:
        app.state.cert_authority = None  # type: ignore[attr-defined]
    app.state.verification_uri = os.environ.get("AUTH_DEVICE_VERIFICATION_URI", "")  # type: ignore[attr-defined]
    app.state.access_token_ttl = config.AUTH_ACCESS_TOKEN_TTL  # type: ignore[attr-defined]
    app.state.device_code_ttl = config.AUTH_DEVICE_CODE_TTL  # type: ignore[attr-defined]
    app.state.cert_ttl_days = config.AUTH_CERT_TTL_DAYS  # type: ignore[attr-defined]
    app.state.auth_required = config.AUTH_REQUIRED  # type: ignore[attr-defined]

    reaper = asyncio.create_task(_lease_reaper_loop(_engine))
    yield
    reaper.cancel()
    if _engine is not None:
        _engine.dispose()
        _engine = None
