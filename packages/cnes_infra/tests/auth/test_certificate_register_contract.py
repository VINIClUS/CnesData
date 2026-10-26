"""Contrato de certificados executado nos dois adapters reais."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.auth.certificates import (
    CertificateConflict,
    CertificateKey,
    CertificateRecord,
    CertificateRegisterPort,
)
from cnes_infra.auth.dynamodb_certificates import DynamoDBCertificateRegister
from cnes_infra.auth.sqlite_certificates import SQLiteCertificateRegister
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane

_NOW = datetime(2026, 10, 26, 12, tzinfo=UTC)
_FINGERPRINT = "a" * 64
_OTHER_FINGERPRINT = "b" * 64
_TABLE = "cnesdata-control-plane"
_RECORD: dict[str, Any] = {
    "tenant_id": "tenant-a", "agent_id": "agent-a", "serial": "serial-old",
    "certificate_fingerprint_sha256": _FINGERPRINT, "subject_cn": "agent-a.example",
    "issued_at": _NOW - timedelta(days=1), "expires_at": _NOW + timedelta(days=1),
    "revoked_at": None,
}


def _certificate(**changes: Any) -> CertificateRecord:
    return CertificateRecord(**(_RECORD | changes))


def _key(**changes: str) -> CertificateKey:
    return CertificateKey(**({"tenant_id": "tenant-a", "agent_id": "agent-a",
                              "serial": "serial-old"} | changes))


@pytest.fixture(params=("sqlite", "dynamodb"))
def register(request: pytest.FixtureRequest, tmp_path: Any,
             monkeypatch: pytest.MonkeyPatch) -> Iterator[CertificateRegisterPort]:
    if request.param == "sqlite":
        plane = SQLiteControlPlane(tmp_path / "control.sqlite3", lambda: _NOW)
        plane.initialize()
        yield SQLiteCertificateRegister(plane)
        return
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName=_TABLE,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                       {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                  {"AttributeName": "sk", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        yield DynamoDBCertificateRegister(client, _TABLE)


def test_dois_seriais_sobrepostos_ficam_ativos_e_legiveis(
    register: CertificateRegisterPort,
) -> None:
    old = _certificate()
    new = _certificate(serial="serial-new", certificate_fingerprint_sha256=_OTHER_FINGERPRINT)
    register.record(old)
    register.record(new)
    assert register.get(_key()) == old
    assert register.get(_key(serial="serial-new")) == new
    assert register.is_active(_key(), _FINGERPRINT, _NOW) is True
    assert register.is_active(_key(serial="serial-new"), _OTHER_FINGERPRINT, _NOW) is True


def test_limites_de_emissao_e_expiracao_sao_exatos(register: CertificateRegisterPort) -> None:
    certificate = _certificate()
    register.record(certificate)
    before_issue = certificate.issued_at - timedelta(microseconds=1)
    before_expiry = certificate.expires_at - timedelta(microseconds=1)
    assert register.is_active(_key(), _FINGERPRINT, before_issue) is False
    assert register.is_active(_key(), _FINGERPRINT, certificate.issued_at) is True
    assert register.is_active(_key(), _FINGERPRINT, before_expiry) is True
    assert register.is_active(_key(), _FINGERPRINT, certificate.expires_at) is False


def test_certificado_emitido_no_futuro_nao_fica_ativo(register: CertificateRegisterPort) -> None:
    register.record(_certificate(issued_at=_NOW + timedelta(seconds=1)))
    assert register.is_active(_key(), _FINGERPRINT, _NOW) is False


def test_identidade_e_fingerprint_incorretos_nao_autorizam(
    register: CertificateRegisterPort,
) -> None:
    register.record(_certificate())
    for wrong_key in (_key(tenant_id="tenant-b"), _key(agent_id="agent-b"),
                      _key(serial="serial-unknown")):
        assert register.get(wrong_key) is None
        assert register.is_active(wrong_key, _FINGERPRINT, _NOW) is False
    assert register.is_active(_key(), _OTHER_FINGERPRINT, _NOW) is False


def test_revogacao_individual_e_replay_preservam_outro_serial(
    register: CertificateRegisterPort,
) -> None:
    register.record(_certificate())
    register.record(_certificate(serial="serial-new"))
    assert register.revoke(_key(), _NOW) is True
    assert register.revoke(_key(), _NOW + timedelta(minutes=1)) is False
    assert register.is_active(_key(), _FINGERPRINT, _NOW) is False
    assert register.is_active(_key(serial="serial-new"), _FINGERPRINT, _NOW) is True
    assert register.get(_key()) == _certificate(revoked_at=_NOW)


@pytest.mark.parametrize("revoked_at", [
    pytest.param(_NOW.replace(tzinfo=None), id="naive"),
    pytest.param(datetime(2026, 10, 26, 13, tzinfo=timezone(timedelta(hours=1))),
                 id="offset_nao_zero"),
])
def test_revogacao_rejeita_instante_nao_utc_e_preserva_registro(
    register: CertificateRegisterPort, revoked_at: datetime,
) -> None:
    original = _certificate()
    register.record(original)

    with pytest.raises(ValueError) as failure:
        register.revoke(_key(), revoked_at)

    assert str(failure.value) == "code=certificate_instant_not_utc"
    assert register.get(_key()) == original
    assert register.is_active(_key(), _FINGERPRINT, _NOW) is True


def test_revogacao_de_chave_ausente_ou_outro_agente_nao_muda_registro(
    register: CertificateRegisterPort,
) -> None:
    original = _certificate()
    register.record(original)
    assert register.revoke(_key(tenant_id="tenant-b"), _NOW) is False
    assert register.revoke(_key(agent_id="agent-b"), _NOW) is False
    assert register.revoke(_key(serial="serial-unknown"), _NOW) is False
    assert register.get(_key()) == original


def test_serial_ocupado_rejeita_replay_e_outro_agente(register: CertificateRegisterPort) -> None:
    original = _certificate()
    register.record(original)
    for duplicate in (original, _certificate(agent_id="agent-b")):
        with pytest.raises(CertificateConflict) as failure:
            register.record(duplicate)
        assert failure.value.code == "certificate_conflict"
        assert str(failure.value) == "code=certificate_conflict"
    assert register.get(_key()) == original


def test_mesmo_serial_pode_existir_em_tenants_distintos(register: CertificateRegisterPort) -> None:
    first = _certificate()
    second = _certificate(tenant_id="tenant-b")
    register.record(first)
    register.record(second)
    assert register.get(_key()) == first
    assert register.get(_key(tenant_id="tenant-b")) == second


def test_revogar_serial_em_tenant_nao_revoga_mesmo_agente_e_serial_no_vizinho(
    register: CertificateRegisterPort,
) -> None:
    first = _certificate()
    second = _certificate(tenant_id="tenant-b", certificate_fingerprint_sha256=_OTHER_FINGERPRINT)
    other_key = _key(tenant_id="tenant-b")
    register.record(first)
    register.record(second)

    assert register.revoke(_key(), _NOW) is True
    assert register.get(_key()) == _certificate(revoked_at=_NOW)
    assert register.get(other_key) == second
    assert register.is_active(other_key, _OTHER_FINGERPRINT, _NOW) is True
    assert register.revoke(_key(), _NOW + timedelta(minutes=1)) is False
    assert register.get(other_key) == second
    assert register.is_active(other_key, _OTHER_FINGERPRINT, _NOW) is True


def test_registro_e_chave_rejeitam_atribuicao_e_preservam_valores() -> None:
    record = _certificate()
    key = _key()
    with pytest.raises((AttributeError, TypeError, ValueError)):
        record.serial = "serial-new"
    with pytest.raises((AttributeError, TypeError, ValueError)):
        key.serial = "serial-new"
    assert record.serial == "serial-old"
    assert key.serial == "serial-old"


def test_registro_aceita_revoked_at_utc() -> None:
    assert _certificate(revoked_at=_NOW).revoked_at == _NOW


@pytest.mark.parametrize("field", ["tenant_id", "agent_id", "serial", "subject_cn"])
def test_registro_rejeita_identificador_vazio(field: str) -> None:
    with pytest.raises(ValueError):
        _certificate(**{field: ""})


@pytest.mark.parametrize("fingerprint", ["a" * 63, "A" * 64, "z" * 64])
def test_registro_exige_fingerprint_sha256_hexadecimal_minusculo(fingerprint: str) -> None:
    with pytest.raises(ValueError):
        _certificate(certificate_fingerprint_sha256=fingerprint)


@pytest.mark.parametrize("change", [
    {"issued_at": (_NOW - timedelta(days=1)).replace(tzinfo=None)},
    {"expires_at": (_NOW + timedelta(days=1)).replace(tzinfo=None)},
    {"issued_at": datetime(2026, 10, 25, 13, tzinfo=timezone(timedelta(hours=1)))},
    {"expires_at": _NOW - timedelta(days=1)},
    {"revoked_at": _NOW.replace(tzinfo=None)},
    {"revoked_at": datetime(2026, 10, 26, 13, tzinfo=timezone(timedelta(hours=1)))},
])
def test_registro_exige_instantes_utc_e_expiracao_posterior(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _certificate(**change)
