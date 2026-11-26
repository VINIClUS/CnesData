"""Rota de prontidão do host gate: corpo constante só com banco e STS respondendo."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Final, Protocol, cast

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response
from sqlalchemy import text
from sqlalchemy.engine import Engine  # noqa: TC002 - resolved at runtime by FastAPI
from sqlalchemy.exc import SQLAlchemyError

from central_api.deps import get_health_engine

if TYPE_CHECKING:
    from collections.abc import Callable

    from boto3.session import Session

    from cnes_infra.aws import AwsRuntimeSettings

logger = logging.getLogger(__name__)

READINESS_PATH: Final = "/health/ready"
READY_BODY: Final = b'{"status":"ready"}'
NOT_READY_BODY: Final = b'{"status":"not_ready"}'
STS_CACHE_SECONDS: Final = 60.0
# The host gate gives the whole probe 20 s; boto3 defaults (60 s read, retries) would exceed it.
_STS_CONFIG: Final = Config(connect_timeout=5, read_timeout=5, retries={"total_max_attempts": 2})

router = APIRouter(include_in_schema=False)


class StsClient(Protocol):
    def get_caller_identity(self) -> dict[str, Any]: ...


class StsReadiness:
    """Confirma credenciais AWS da sessão do app via STS, com cache de sucesso."""

    def __init__(self, sts: StsClient, clock: Callable[[], float] = time.monotonic) -> None:
        self._sts = sts
        self._clock = clock
        self._confirmed_at: float | None = None

    def ready(self) -> bool:
        """Returns: True se GetCallerIdentity respondeu agora ou há menos de 60 s."""
        now = self._clock()
        if self._confirmed_at is not None and now - self._confirmed_at < STS_CACHE_SECONDS:
            return True
        try:
            self._sts.get_caller_identity()
        except (BotoCoreError, ClientError) as error:
            logger.warning("readiness_sts_failed error=%s", type(error).__name__)
            return False
        self._confirmed_at = now
        return True


def sts_readiness(session: Session, settings: AwsRuntimeSettings) -> StsReadiness:
    """Returns: StsReadiness sobre um cliente STS da sessão boto3 do lifespan aws."""
    client = cast("StsClient", session.client(  # pyright: ignore[reportUnknownMemberType]
        "sts", region_name=settings.region, endpoint_url=settings.service_endpoint_url,
        config=_STS_CONFIG,
    ))
    return StsReadiness(client)


def _database_ready(engine: Engine | None) -> bool:
    if engine is None:
        return True
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as error:
        logger.warning("readiness_db_failed error=%s", type(error).__name__)
        return False
    return True


@router.get(READINESS_PATH)
def readiness(request: Request, engine: Engine | None = Depends(get_health_engine)) -> Response:
    sts = cast("StsReadiness | None", getattr(request.app.state, "sts_readiness", None))
    ready = _database_ready(engine) and (sts is None or sts.ready())
    return Response(
        READY_BODY if ready else NOT_READY_BODY,
        status_code=200 if ready else 503,
        media_type="application/json",
    )
