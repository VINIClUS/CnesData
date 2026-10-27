"""Contrato público dos settings do despacho de auditoria AWS."""

import importlib
import socket
from typing import Any
from unittest.mock import Mock

import pytest

from cnes_infra.aws.processor_settings import AwsAuditDispatchSettings
from cnes_infra.aws.settings import AwsRuntimeConfigurationError

_MINIMUM = {
    "PROFILE": "aws",
    "AWS_REGION": "us-east-2",
    "AWS_CONTROL_PLANE_TABLE": "audit-control-configurado",
    "AWS_AUDIT_BUCKET": "audit-bucket-configurado",
    "AWS_AUDIT_RETENTION_DAYS": "365",
}


@pytest.fixture(autouse=True)
def _isolar_rede(monkeypatch: pytest.MonkeyPatch) -> None:
    def negar(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("network=forbidden")

    for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex"):
        monkeypatch.setattr(socket, name, negar)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, negar)


@pytest.mark.parametrize("bucket", ["invalid/name", r"invalid\name"], ids=["barra", "contrabarra"])
def test_settings_rejeitam_bucket_com_barra_antes_de_dependencias(
    monkeypatch: pytest.MonkeyPatch,
    bucket: str,
) -> None:
    boundaries = (
        (importlib.import_module("boto3.session"), "Session"),
        (importlib.import_module("boto3"), "client"),
        (importlib.import_module("botocore.client").BaseClient, "_make_api_call"),
        (importlib.import_module("cnes_infra.billing.composition"), "build_secret_provider"),
    )
    spies = [Mock(side_effect=AssertionError("settings=dependency_forbidden")) for _ in boundaries]
    for (module, name), spy in zip(boundaries, spies, strict=True):
        monkeypatch.setattr(module, name, spy)

    with pytest.raises(AwsRuntimeConfigurationError) as caught:
        AwsAuditDispatchSettings.from_mapping(_MINIMUM | {"AWS_AUDIT_BUCKET": bucket})

    assert type(caught.value) is AwsRuntimeConfigurationError
    for spy in spies:
        spy.assert_not_called()


@pytest.mark.parametrize("production", [False, True])
def test_settings_publicos_aceitam_apenas_recursos_de_auditoria(production: bool) -> None:
    module = importlib.import_module("cnes_infra.aws.processor_settings")
    settings_type = getattr(module, "AwsAuditDispatchSettings", None)
    assert settings_type is not None, "audit_settings=public_contract_required"
    values = _MINIMUM | {"BILLING_MODE": "invalid", "BILLING_ENFORCEMENT_MODE": "invalid"}
    if production:
        values["AWS_PRODUCTION_POLICY"] = "true"

    settings = settings_type.from_mapping(values)

    assert (
        settings.region,
        settings.control_plane_table,
        settings.audit_bucket,
        settings.audit_retention_days,
    ) == (
        "us-east-2",
        "audit-control-configurado",
        "audit-bucket-configurado",
        365,
    )


@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"AWS_REGION": None}, id="sem-regiao"),
        pytest.param({"AWS_REGION": " "}, id="branco-regiao"),
        pytest.param({"AWS_CONTROL_PLANE_TABLE": None}, id="sem-tabela"),
        pytest.param({"AWS_CONTROL_PLANE_TABLE": " "}, id="branco-tabela"),
        pytest.param({"AWS_AUDIT_BUCKET": None}, id="sem-bucket"),
        pytest.param({"AWS_AUDIT_BUCKET": " "}, id="branco-bucket"),
        pytest.param({"AWS_AUDIT_RETENTION_DAYS": None}, id="sem-retencao"),
        pytest.param({"AWS_AUDIT_RETENTION_DAYS": " "}, id="branco-retencao"),
        pytest.param({"AWS_AUDIT_RETENTION_DAYS": "0"}, id="retencao-zero"),
        pytest.param({"AWS_AUDIT_RETENTION_DAYS": "-1"}, id="retencao-negativa"),
        pytest.param({"AWS_AUDIT_RETENTION_DAYS": "1.5"}, id="retencao-fracionaria"),
        pytest.param({"AWS_AUDIT_RETENTION_DAYS": "abc"}, id="retencao-texto"),
        pytest.param({"AWS_PRODUCTION_POLICY": ""}, id="policy-vazia"),
        pytest.param({"AWS_PRODUCTION_POLICY": "TRUE"}, id="policy-maiuscula"),
        pytest.param({"AWS_PRODUCTION_POLICY": "yes"}, id="policy-invalida"),
        pytest.param(
            {"AWS_PRODUCTION_POLICY": "true", "AWS_REGION": "us-east-1"}, id="producao-regiao"
        ),
        pytest.param(
            {"AWS_PRODUCTION_POLICY": "true", "AWS_AUDIT_RETENTION_DAYS": "364"},
            id="producao-retencao",
        ),
    ],
)
def test_settings_rejeitam_configuracao_invalida_antes_de_dependencias(
    monkeypatch: pytest.MonkeyPatch,
    changes: dict[str, str | None],
) -> None:
    boundaries = (
        (importlib.import_module("boto3.session"), "Session"),
        (importlib.import_module("boto3"), "client"),
        (importlib.import_module("botocore.client").BaseClient, "_make_api_call"),
        (importlib.import_module("cnes_infra.billing.composition"), "build_secret_provider"),
    )
    spies = [Mock(side_effect=AssertionError("settings=dependency_forbidden")) for _ in boundaries]
    for (module, name), spy in zip(boundaries, spies, strict=True):
        monkeypatch.setattr(module, name, spy)
    values = dict(_MINIMUM)
    for key, value in changes.items():
        if value is None:
            values.pop(key)
        else:
            values[key] = value

    with pytest.raises(AwsRuntimeConfigurationError) as caught:
        AwsAuditDispatchSettings.from_mapping(values)

    assert type(caught.value) is AwsRuntimeConfigurationError
    for spy in spies:
        spy.assert_not_called()
