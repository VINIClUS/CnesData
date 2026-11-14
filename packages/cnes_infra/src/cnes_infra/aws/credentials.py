"""Adoção sem restart da credencial do credential_process, provada por log sem segredo."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import threading
from typing import TYPE_CHECKING, cast

from botocore.credentials import RefreshableCredentials

from cnes_infra.aws.settings import AwsRuntimeConfigurationError

if TYPE_CHECKING:
    from boto3.session import Session
    from botocore.awsrequest import AWSPreparedRequest

logger = logging.getLogger(__name__)

PROCESS_METHOD = "custom-process"
WATCH_INTERVAL_SECONDS = 60.0
_SIGNED = re.compile(
    r"Credential=(?P<key>[A-Z0-9]{16,128})/\d{8}/[a-z0-9-]+/(?P<service>[a-z0-9-]+)/aws4_request"
)


def key_fingerprint(access_key: str) -> str:
    """Returns: 12 primeiros hex do sha256 do AccessKeyId; nunca o valor."""
    return hashlib.sha256(access_key.encode("utf-8")).hexdigest()[:12]


def require_process_credentials(session: Session) -> RefreshableCredentials:
    """Args: session: Sessão boto3 da política de produção.
    Returns: A credencial renovável do credential_process da imagem.
    Raises: AwsRuntimeConfigurationError: Sem credencial, outro provider ou sem Expiration.
    """
    credentials = session.get_credentials()
    if credentials is None:
        raise AwsRuntimeConfigurationError("aws_credentials=missing method=none")
    method = credentials.method
    if method != PROCESS_METHOD:
        raise AwsRuntimeConfigurationError(f"aws_credentials=forbidden method={method}")
    if not isinstance(credentials, RefreshableCredentials):
        raise AwsRuntimeConfigurationError(f"aws_credentials=not_refreshable method={method}")
    return credentials


class CredentialAdoption:
    """Loga cada geração adotada (watcher) e a chave que assinou cada serviço (before-send)."""

    def __init__(self, credentials: RefreshableCredentials) -> None:
        self._credentials = credentials
        self._lock = threading.Lock()
        self._current: str | None = None
        self._signed: dict[str, str] = {}

    @property
    def credentials(self) -> RefreshableCredentials:
        return self._credentials

    def observe(self) -> str:
        """Returns: Fingerprint vigente depois da renovação do botocore, se houver.
        Raises: Exception: Falha do botocore na janela mandatória ou credencial expirada.
        """
        frozen = self._credentials.get_frozen_credentials()
        fingerprint = key_fingerprint(cast("str", frozen.access_key))
        with self._lock:
            previous, self._current = self._current, fingerprint
        if previous != fingerprint:
            logger.info(
                "aws_credentials_refreshed key_fp=%s previous_fp=%s pid=%d",
                fingerprint, previous or "none", os.getpid(),
            )
        return fingerprint

    def tick(self) -> None:
        """Observa uma vez; falha de renovação vira log, nunca exceção."""
        try:
            self.observe()
        except Exception as error:  # the watcher must outlive any refresh failure
            logger.warning("aws_credentials_refresh_failed error=%s", type(error).__name__)
            return
        # botocore swallows a failed refresh in the advisory window and keeps the old generation.
        if self._credentials.refresh_needed():
            logger.warning("aws_credentials_refresh_failed error=advisory_refresh")

    def record_signature(self, request: AWSPreparedRequest, **_: object) -> None:
        """Args: request: Request assinado no evento before-send; nunca vira resposta."""
        match = _SIGNED.search(_authorization(request))
        if match is None:
            return
        fingerprint = key_fingerprint(match["key"])
        service = match["service"]
        with self._lock:
            if self._signed.get(service) == fingerprint:
                return
            self._signed[service] = fingerprint
        logger.info(
            "aws_credentials_signed key_fp=%s service=%s pid=%d",
            fingerprint, service, os.getpid(),
        )


def _authorization(request: AWSPreparedRequest) -> str:
    # botocore encodes header values to bytes before before-send; stubs and tests may keep str.
    value: object = request.headers.get("Authorization")
    if isinstance(value, bytes):
        return value.decode("latin-1")
    return value if isinstance(value, str) else ""


def register_signing_observer(session: Session, adoption: CredentialAdoption) -> None:
    """Registra o observer na sessão; só clients criados depois herdam o handler."""
    session.events.register("before-send", adoption.record_signature)


async def watch_credentials(
    adoption: CredentialAdoption, interval: float = WATCH_INTERVAL_SECONDS,
) -> None:
    """Dorme e depois observa, fora do event loop, até ser cancelado."""
    while True:
        await asyncio.sleep(interval)
        await asyncio.to_thread(adoption.tick)
