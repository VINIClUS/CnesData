"""Dependências compartilhadas da API (engine, minio wrapper, reaper)."""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
    from collections.abc import AsyncGenerator, Callable, Iterator

    from sqlalchemy.engine import Connection, Engine

    from central_api.composition import LocalRuntime
    from central_api.routes.serving import ServingPrincipal
    from cnes_domain.profiles import ProfileSettings
    from cnes_infra.auth.local_auth import LocalAuthService

logger = logging.getLogger(__name__)

_engine: Engine | None = None
_REAPER_INTERVAL = 60


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(config.DB_URL)
    return _engine


def get_health_engine() -> Engine | None:
    if _local_profile_requested():
        return None
    return get_engine()


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
    return extractions_repo.reap_expired(engine)


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


def _local_profile_requested() -> bool:
    return os.environ.get("PROFILE", "").strip().lower() == "local"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _build_local_state(app: object) -> None:
    """Compõe uma única vez o grafo SQLite/filesystem do profile local."""

    from central_api.composition import build_local_runtime
    from central_api.services.national_ingestion import NationalIngestionService
    from central_api.services.raw_upload import RawUploadService
    from cnes_domain.profiles import parse_profile
    from cnes_infra.ingestion import DatasusCnesFtpTransport, DatasusCnesRawAdapter

    settings = parse_profile(os.environ)
    runtime = build_local_runtime(settings, _utc_now)
    app.state.settings = settings
    app.state.control_plane = runtime.control_plane
    app.state.raw_query = runtime.control_plane
    app.state.object_store = runtime.object_store
    app.state.raw_ingestion = runtime.raw_ingestion
    app.state.run_planning = runtime.run_planning
    app.state.source_catalog = runtime.source_catalog
    app.state.audit_sink = runtime.audit_sink
    app.state.executor = runtime.executor
    app.state.raw_upload = RawUploadService(runtime.control_plane, runtime.object_store, _utc_now)
    app.state.national_ingestion = NationalIngestionService(
        runtime.control_plane,
        DatasusCnesRawAdapter(DatasusCnesFtpTransport(), runtime.object_store, _utc_now),
        runtime.raw_ingestion,
        _utc_now,
    )
    _install_edge_overrides(app)
    _install_local_auth_and_serving(app, runtime, settings)
    logger.info(
        "local_profile_composed tenant_id=%s data_dir=%s",
        settings.tenant_id,
        settings.data_dir,
    )


def _install_edge_overrides(app: object) -> None:
    from central_api.routes.raw_jobs import (
        get_control_plane,
        get_raw_ingestion_service,
        get_raw_upload_service,
    )

    app.dependency_overrides[get_control_plane] = lambda: app.state.control_plane
    app.dependency_overrides[get_raw_upload_service] = lambda: app.state.raw_upload
    app.dependency_overrides[get_raw_ingestion_service] = lambda: app.state.raw_ingestion


def _serving_principal_resolver(
    auth_service: LocalAuthService,
) -> Callable[[Request], ServingPrincipal]:
    from central_api.routes.local_auth import SESSION_COOKIE_NAME
    from central_api.routes.serving import ServingPrincipal
    from cnes_infra.auth.local_auth import AuthenticationRejected

    def _resolve(request: Request) -> ServingPrincipal:
        token = request.cookies.get(SESSION_COOKIE_NAME)
        if token is None:
            raise HTTPException(status_code=401, detail="session_required")
        try:
            principal = auth_service.resolve_session(token)
        except AuthenticationRejected as error:
            raise HTTPException(status_code=401, detail="session_invalid") from error
        return ServingPrincipal(tenant_id=principal.tenant_id, user_id=principal.user_id)

    return _resolve


def _install_local_auth_and_serving(
    app: object, runtime: LocalRuntime, settings: ProfileSettings
) -> None:
    from central_api.routes import local_auth, serving
    from central_api.services.serving_access import LocalServingAccess
    from cnes_infra.auth.local_auth import LocalAuthDependencies, LocalAuthService
    from cnes_infra.auth.local_credentials import LocalCredentialStore

    credentials = LocalCredentialStore(settings.state_db)
    credentials.initialize()
    auth_service = LocalAuthService(
        LocalAuthDependencies(credentials, runtime.control_plane, settings), _utc_now
    )
    serving_access = LocalServingAccess(runtime.control_plane, runtime.object_store)
    app.state.local_auth_service = auth_service
    app.state.serving_access = serving_access
    app.dependency_overrides[local_auth.get_local_auth_service] = lambda: auth_service
    app.dependency_overrides[serving.get_serving_access] = lambda: serving_access
    app.dependency_overrides[serving.get_serving_object_store] = lambda: runtime.object_store
    app.dependency_overrides[serving.get_serving_principal] = _serving_principal_resolver(
        auth_service
    )


@asynccontextmanager
async def lifespan(app: object) -> AsyncGenerator[None]:
    global _engine
    if _local_profile_requested():
        _build_local_state(app)
        yield
        return
    _db_url = os.environ.get("DB_URL") or config.DB_URL
    _engine = create_engine(_db_url)
    install_rls_listener(_engine)
    install_query_counter(_engine)
    instrument_engine(_engine)

    from central_api.repositories.dashboard_repo import DashboardRepo
    from central_api.repositories.leads_repo import LeadsRepo
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
    app.state.leads_repo = LeadsRepo(_engine)  # type: ignore[attr-defined]
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
