"""Testes do provedor de segredos do AWS Secrets Manager."""

import logging
from collections.abc import Mapping
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from cnes_domain.billing.ports import SecretProviderPort
from cnes_infra.billing.secrets_manager import (
    SecretProviderError,
    SecretsManagerSecretProvider,
)

_ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:cnes/stripe"
_VALUE = "sk_test_super_secret_value"


class _FakeClient:
    def __init__(
        self,
        response: Mapping[str, object] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.response = response
        self.error = error
        self.calls: list[str] = []

    def get_secret_value(self, *, SecretId: str) -> Mapping[str, object]:  # noqa: N803
        self.calls.append(SecretId)
        if self.error is not None:
            raise self.error
        return self.response or {}


def _client_error(code: str | None) -> ClientError:
    error: dict[str, str] = {"Message": f"{_ARN} secret"}
    if code is not None:
        error["Code"] = code
    return ClientError(cast("Any", {"Error": error}), "GetSecretValue")


def _failure_cases() -> list[tuple[str, _FakeClient, str]]:
    return [
        ("empty", _FakeClient({"SecretString": ""}), "secret_not_text"),
        ("binary", _FakeClient({"SecretBinary": b"x"}), "secret_binary_unsupported"),
        ("denied", _FakeClient(error=_client_error("AccessDeniedException")),
         "AccessDeniedException"),
        ("throttle", _FakeClient(error=_client_error("ThrottlingException")),
         "ThrottlingException"),
        ("unsafe", _FakeClient(error=_client_error(f"bad code; {_ARN}")), "client_error"),
        ("transport", _FakeClient(error=EndpointConnectionError(
            endpoint_url=f"https://secretsmanager.us-east-1.amazonaws.com/{_ARN}")),
         "transport_error"),
    ]


def _fetch_error(client: _FakeClient, arn: str = _ARN) -> SecretProviderError:
    with pytest.raises(SecretProviderError) as info:
        SecretsManagerSecretProvider(client).get_secret(arn)
    return info.value


def test_envia_exatamente_o_arn_e_devolve_secret_string() -> None:
    client = _FakeClient({"SecretString": _VALUE})

    assert SecretsManagerSecretProvider(client).get_secret(_ARN) == _VALUE
    assert client.calls == [_ARN]


def test_devolve_secret_string_sem_alterar_espacos() -> None:
    client = _FakeClient({"SecretString": f" {_VALUE}\n"})

    assert SecretsManagerSecretProvider(client).get_secret(_ARN) == f" {_VALUE}\n"


def test_implementa_secret_provider_port() -> None:
    provider = SecretsManagerSecretProvider(_FakeClient({"SecretString": _VALUE}))

    assert isinstance(provider, SecretProviderPort)


@pytest.mark.parametrize("arn", ["", "   ", None, 42])
def test_arn_invalido_falha_fechado_sem_chamar_client(arn: object) -> None:
    client = _FakeClient({"SecretString": _VALUE})

    error = _fetch_error(client, arn)  # type: ignore[arg-type]

    assert (error.code, error.retryable) == ("secret_arn_empty", False)
    assert client.calls == []


@pytest.mark.parametrize(
    "response",
    [{}, {"SecretString": ""}, {"SecretString": "  \n"}, {"SecretString": 123},
     {"SecretString": None}, {"SecretString": b"bytes"}],
)
def test_secret_string_ausente_vazio_ou_nao_texto_falha_fechado(
    response: Mapping[str, object],
) -> None:
    error = _fetch_error(_FakeClient(response))

    assert (error.code, error.retryable) == ("secret_not_text", False)


@pytest.mark.parametrize(
    "response",
    [{"SecretBinary": b"\x00"}, {"SecretString": _VALUE, "SecretBinary": b"\x00"}],
)
def test_secret_binary_nao_e_suportado(response: Mapping[str, object]) -> None:
    error = _fetch_error(_FakeClient(response))

    assert (error.code, error.retryable) == ("secret_binary_unsupported", False)


@pytest.mark.parametrize(
    "code", ["ThrottlingException", "InternalServiceError", "ServiceUnavailableException"]
)
def test_codigos_transitorios_sao_retryable(code: str) -> None:
    error = _fetch_error(_FakeClient(error=_client_error(code)))

    assert (error.code, error.retryable) == (code, True)


@pytest.mark.parametrize(
    "code", ["AccessDeniedException", "ResourceNotFoundException", "DecryptionFailure"]
)
def test_codigos_permanentes_nao_sao_retryable(code: str) -> None:
    error = _fetch_error(_FakeClient(error=_client_error(code)))

    assert (error.code, error.retryable) == (code, False)


@pytest.mark.parametrize("code", [None, "bad code; arn:aws:x", "1Starts", "", "A" * 65])
def test_client_error_sem_codigo_seguro_usa_client_error(code: str | None) -> None:
    error = _fetch_error(_FakeClient(error=_client_error(code)))

    assert (error.code, error.retryable) == ("client_error", False)


def test_client_error_sem_bloco_error_usa_client_error() -> None:
    client = _FakeClient(error=ClientError({}, "GetSecretValue"))

    assert _fetch_error(client).code == "client_error"


def test_botocore_error_vira_transport_error_retryable() -> None:
    endpoint = EndpointConnectionError(endpoint_url="https://secretsmanager.example")

    error = _fetch_error(_FakeClient(error=endpoint))

    assert (error.code, error.retryable) == ("transport_error", True)


@pytest.mark.parametrize("retryable", [True, False])
def test_mensagem_da_excecao_segue_formato_chave_valor(retryable: bool) -> None:
    error = SecretProviderError("some_code", retryable)

    assert str(error) == f"secret_provider_error code=some_code retryable={str(retryable).lower()}"


@pytest.mark.parametrize(("_name", "client", "code"), _failure_cases())
def test_falhas_nao_vazam_arn_nem_segredo(
    _name: str, client: _FakeClient, code: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    error = _fetch_error(client)

    assert error.code == code
    assert error.__cause__ is None
    for text in (str(error), repr(error), caplog.text):
        assert _ARN not in text
        assert _VALUE not in text
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].getMessage() == (
        f"secret_fetch_failed code={code} retryable={error.retryable}"
    )


def test_falha_de_arn_vazio_registra_um_warning_sem_dados(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    _fetch_error(_FakeClient(), "  ")

    assert [r.getMessage() for r in caplog.records] == [
        "secret_fetch_failed code=secret_arn_empty retryable=False"
    ]


def test_sucesso_nao_registra_arn_nem_segredo(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    SecretsManagerSecretProvider(_FakeClient({"SecretString": _VALUE})).get_secret(_ARN)

    assert _ARN not in caplog.text
    assert _VALUE not in caplog.text


def test_nao_guarda_segredo_em_cache() -> None:
    client = _FakeClient({"SecretString": _VALUE})
    provider = SecretsManagerSecretProvider(client)

    provider.get_secret(_ARN)
    provider.get_secret(_ARN)

    assert client.calls == [_ARN, _ARN]
