"""Provedor de segredos do AWS Secrets Manager, somente texto e fail-closed."""

import logging
import re
from collections.abc import Mapping
from typing import Protocol

from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

_RETRYABLE_SECRET_CODES = frozenset(
    {"ThrottlingException", "InternalServiceError", "ServiceUnavailableException"}
)
_SAFE_CODE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


class SecretsManagerClient(Protocol):
    def get_secret_value(
        self, *, SecretId: str,  # noqa: N803
    ) -> Mapping[str, object]: ...  # pragma: no cover


class SecretProviderError(RuntimeError):
    def __init__(self, code: str, retryable: bool) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(f"secret_provider_error code={code} retryable={str(retryable).lower()}")


def _fail(code: str, retryable: bool) -> SecretProviderError:
    logger.warning("secret_fetch_failed code=%s retryable=%s", code, retryable)
    return SecretProviderError(code, retryable)


def _client_error_code(error: ClientError) -> str:
    code = error.response.get("Error", {}).get("Code")
    if isinstance(code, str) and _SAFE_CODE.fullmatch(code):
        return code
    return "client_error"


class SecretsManagerSecretProvider:
    def __init__(self, client: SecretsManagerClient) -> None:
        self._client = client

    def get_secret(self, secret_arn: str) -> str:
        """Busca um segredo de texto.

        Args:
            secret_arn: ARN do segredo, enviado sem alteração.
        Returns:
            Valor de SecretString sem alteração.
        Raises:
            SecretProviderError: falha sanitizada, sem ARN nem valor.
        """
        if not isinstance(secret_arn, str) or not secret_arn.strip():
            raise _fail("secret_arn_empty", False)
        response = self._fetch(secret_arn)
        if "SecretBinary" in response:
            raise _fail("secret_binary_unsupported", False)
        value = response.get("SecretString")
        if not isinstance(value, str) or not value.strip():
            raise _fail("secret_not_text", False)
        return value

    def _fetch(self, secret_arn: str) -> Mapping[str, object]:
        try:
            return self._client.get_secret_value(SecretId=secret_arn)
        except ClientError as error:
            code = _client_error_code(error)
            raise _fail(code, code in _RETRYABLE_SECRET_CODES) from None
        except BotoCoreError:
            raise _fail("transport_error", True) from None
