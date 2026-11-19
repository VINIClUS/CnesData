"""S3 Object Lock no Floci: COMPLIANCE, GOVERNANCE, legal hold e o sink de auditoria real."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.audit import S3ObjectLockAuditSink
from tests.integration.floci._floci import outcome, unique_name
from tests.integration.floci._iam import create_user, delete_user, statement
from tests.integration.floci._s3 import create_bucket, delete_bucket

if TYPE_CHECKING:
    from contextlib import ExitStack

    from tests.integration.floci._floci import FlociContext

pytestmark = [pytest.mark.floci]

# COMPLIANCE windows stay short so the module teardown can wait them out and leave no bucket.
_SHORT = timedelta(seconds=8)
_HOUR = timedelta(hours=1)
_OBJECT_ACTIONS = ("s3:GetObject", "s3:PutObject", "s3:GetObjectRetention", "s3:PutObjectRetention")
_DELETE_ACTIONS = ("s3:DeleteObject", "s3:DeleteObjectVersion")
_BYPASS = "s3:BypassGovernanceRetention"


@dataclass(frozen=True, slots=True)
class _Lock:
    floci: FlociContext
    s3: Any
    bucket: str

    def put(self, key: str, **lock: Any) -> str:
        return self.s3.put_object(Bucket=self.bucket, Key=key, Body=b"{}", **lock)["VersionId"]

    def retained(self, key: str, mode: str, until: datetime) -> str:
        return self.put(key, ObjectLockMode=mode, ObjectLockRetainUntilDate=until)

    def delete(self, key: str, version_id: str, **options: Any) -> str:
        return outcome(
            self.s3.delete_object, Bucket=self.bucket, Key=key, VersionId=version_id, **options,
        )

    def exists(self, key: str, version_id: str) -> bool:
        return outcome(
            self.s3.head_object, Bucket=self.bucket, Key=key, VersionId=version_id,
        ) == "OK"

    def retention(self, key: str, version_id: str) -> dict[str, Any]:
        response = self.s3.get_object_retention(Bucket=self.bucket, Key=key, VersionId=version_id)
        return response["Retention"]


@pytest.fixture(scope="module")
def lock(floci: FlociContext, module_cleanup: ExitStack) -> _Lock:
    s3 = floci.client("s3")
    bucket = unique_name()
    create_bucket(s3, bucket)
    module_cleanup.callback(delete_bucket, s3, bucket)
    return _Lock(floci, s3, bucket)


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _key(prefix: str) -> str:
    return f"{prefix}/{uuid4().hex}.json"


def _retention(mode: str, until: datetime) -> dict[str, Any]:
    return {"Mode": mode, "RetainUntilDate": until}


def _set_retention(lock: _Lock, key: str, version_id: str, retention: dict[str, Any]) -> str:
    return outcome(
        lock.s3.put_object_retention, Bucket=lock.bucket, Key=key, VersionId=version_id,
        Retention=retention,
    )


def _user_s3(lock: _Lock, cleanup: ExitStack, statements: list[dict[str, Any]]) -> Any:
    iam = lock.floci.client("iam")
    user = create_user(iam, unique_name(), statements)
    cleanup.callback(delete_user, iam, user.name)
    return lock.floci.client("s3", user.credentials)


def _allow(lock: _Lock, *actions: str) -> dict[str, Any]:
    arn = f"arn:aws:s3:::{lock.bucket}"
    return statement("Allow", actions, [arn, f"{arn}/*"])


def _event(event_id: str, payload: dict[str, Any]) -> OutboxEvent:
    created_at = _now() - timedelta(days=1) + _SHORT
    return OutboxEvent(
        tenant_id="354130", event_id=event_id, event_type="run.published",
        aggregate_id="run-cap", payload=payload, created_at=created_at, delivered_at=None,
    )


def test_bucket_com_object_lock_nasce_versionado_e_habilitado(lock: _Lock) -> None:
    versioning = lock.s3.get_bucket_versioning(Bucket=lock.bucket)
    configuration = lock.s3.get_object_lock_configuration(Bucket=lock.bucket)

    assert versioning["Status"] == "Enabled"
    assert configuration["ObjectLockConfiguration"] == {"ObjectLockEnabled": "Enabled"}


def test_delete_de_versao_retida_e_negado(lock: _Lock) -> None:
    key = _key("compliance")
    version = lock.retained(key, "COMPLIANCE", _now() + _SHORT)

    assert lock.delete(key, version) == "AccessDenied"
    assert lock.delete(key, version, BypassGovernanceRetention=True) == "AccessDenied"
    assert lock.exists(key, version)


def test_delete_sem_versao_cria_marcador_e_preserva_versao_retida(lock: _Lock) -> None:
    key = _key("compliance")
    version = lock.retained(key, "COMPLIANCE", _now() + _SHORT)

    marker = lock.s3.delete_object(Bucket=lock.bucket, Key=key)

    assert marker["DeleteMarker"] is True
    assert lock.exists(key, version)


def test_retencao_compliance_nao_pode_ser_encurtada_nem_rebaixada(lock: _Lock) -> None:
    key = _key("compliance")
    until = _now() + _SHORT
    version = lock.retained(key, "COMPLIANCE", until)
    extended = until + timedelta(seconds=2)

    earlier = until - timedelta(seconds=4)
    shorter = _set_retention(lock, key, version, _retention("COMPLIANCE", earlier))
    governance = _set_retention(lock, key, version, _retention("GOVERNANCE", until))
    longer = _set_retention(lock, key, version, _retention("COMPLIANCE", extended))

    assert (shorter, governance, longer) == ("AccessDenied", "AccessDenied", "OK")
    assert lock.retention(key, version) == _retention("COMPLIANCE", extended)


def test_retencao_compliance_vencida_libera_o_delete(lock: _Lock) -> None:
    key = _key("compliance")
    until = _now() + timedelta(seconds=2)
    version = lock.retained(key, "COMPLIANCE", until)
    time.sleep(max(0.0, (until - datetime.now(UTC)).total_seconds()) + 1)

    assert lock.delete(key, version) == "OK"
    assert not lock.exists(key, version)


def test_governance_sem_cabecalho_de_bypass_nega_delete_e_encurtamento(lock: _Lock) -> None:
    key = _key("governance")
    version = lock.retained(key, "GOVERNANCE", _now() + _HOUR)
    shorter = {"Mode": "GOVERNANCE", "RetainUntilDate": _now() + _SHORT}

    assert lock.delete(key, version) == "AccessDenied"
    assert _set_retention(lock, key, version, shorter) == "AccessDenied"
    assert lock.exists(key, version)


def test_bypass_governance_com_permissao_remove_versao(lock: _Lock, cleanup: ExitStack) -> None:
    s3 = _user_s3(lock, cleanup, [_allow(lock, *_OBJECT_ACTIONS, *_DELETE_ACTIONS, _BYPASS)])
    key = _key("governance")
    version = lock.retained(key, "GOVERNANCE", _now() + _HOUR)
    attempt = {"Bucket": lock.bucket, "Key": key, "VersionId": version}

    assert outcome(s3.delete_object, **attempt) == "AccessDenied"
    assert outcome(s3.delete_object, **attempt, BypassGovernanceRetention=True) == "OK"
    assert not lock.exists(key, version)


@pytest.mark.parametrize("explicit_deny", [False, True], ids=["sem_permissao", "deny_explicito"])
def test_floci_aceita_bypass_governance_sem_a_permissao_iam(
    lock: _Lock, cleanup: ExitStack, explicit_deny: bool,
) -> None:
    statements = [_allow(lock, *_OBJECT_ACTIONS, *_DELETE_ACTIONS)]
    if explicit_deny:
        statements.append(statement("Deny", [_BYPASS], [f"arn:aws:s3:::{lock.bucket}/*"]))
    s3 = _user_s3(lock, cleanup, statements)
    key = _key("governance")
    version = lock.retained(key, "GOVERNANCE", _now() + _HOUR)

    deleted = outcome(
        s3.delete_object, Bucket=lock.bucket, Key=key, VersionId=version,
        BypassGovernanceRetention=True,
    )

    assert deleted == "OK"
    assert not lock.exists(key, version)


@pytest.mark.parametrize(
    ("action", "expected"),
    [("s3:DeleteObject", "OK"), ("s3:DeleteObjectVersion", "AccessDenied")],
)
def test_floci_autoriza_delete_de_versao_pela_acao_delete_object(
    lock: _Lock, cleanup: ExitStack, action: str, expected: str,
) -> None:
    s3 = _user_s3(lock, cleanup, [_allow(lock, action)])
    key = _key("versao")
    version = lock.put(key)

    assert outcome(s3.delete_object, Bucket=lock.bucket, Key=key, VersionId=version) == expected


def test_floci_aceita_cabecalhos_de_lock_sem_put_object_retention(
    lock: _Lock, cleanup: ExitStack,
) -> None:
    s3 = _user_s3(lock, cleanup, [_allow(lock, "s3:PutObject")])
    key = _key("governance")
    until = _now() + _HOUR

    version = s3.put_object(
        Bucket=lock.bucket, Key=key, Body=b"{}", ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=until,
    )["VersionId"]

    assert lock.retention(key, version) == {"Mode": "GOVERNANCE", "RetainUntilDate": until}


def test_legal_hold_bloqueia_delete_ate_ser_removido(lock: _Lock) -> None:
    key = _key("hold")
    version = lock.put(key, ObjectLockLegalHoldStatus="ON")
    blocked = (lock.delete(key, version), lock.delete(key, version, BypassGovernanceRetention=True))
    lock.s3.put_object_legal_hold(
        Bucket=lock.bucket, Key=key, VersionId=version, LegalHold={"Status": "OFF"},
    )

    assert blocked == ("AccessDenied", "AccessDenied")
    assert lock.delete(key, version) == "OK"


def test_sobrescrita_cria_nova_versao_e_preserva_a_retida(lock: _Lock) -> None:
    key = _key("governance")
    until = _now() + _HOUR
    retained = lock.retained(key, "GOVERNANCE", until)
    latest = lock.put(key)
    body = lock.s3.get_object(Bucket=lock.bucket, Key=key, VersionId=retained)["Body"].read()

    assert latest != retained
    assert body == b"{}"
    assert lock.retention(key, retained) == {"Mode": "GOVERNANCE", "RetainUntilDate": until}
    assert lock.retention(key, latest) == {}


def test_retencao_padrao_do_bucket_aplica_governance_a_objetos_novos(
    lock: _Lock, cleanup: ExitStack,
) -> None:
    bucket = unique_name()
    create_bucket(lock.s3, bucket)
    cleanup.callback(delete_bucket, lock.s3, bucket)
    lock.s3.put_object_lock_configuration(Bucket=bucket, ObjectLockConfiguration={
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}},
    })
    response = lock.s3.put_object(Bucket=bucket, Key="padrao.json", Body=b"{}")
    retention = lock.s3.get_object_retention(
        Bucket=bucket, Key="padrao.json", VersionId=response["VersionId"],
    )["Retention"]

    assert retention["Mode"] == "GOVERNANCE"
    assert retention["RetainUntilDate"] > datetime.now(UTC) + timedelta(hours=23)


def test_floci_permite_suspender_versionamento_com_object_lock(
    lock: _Lock, cleanup: ExitStack,
) -> None:
    bucket = unique_name()
    create_bucket(lock.s3, bucket)
    cleanup.callback(delete_bucket, lock.s3, bucket)

    suspended = outcome(
        lock.s3.put_bucket_versioning, Bucket=bucket,
        VersioningConfiguration={"Status": "Suspended"},
    )

    assert suspended == "OK"
    assert lock.s3.get_bucket_versioning(Bucket=bucket)["Status"] == "Suspended"


def test_floci_aplica_retencao_em_bucket_sem_object_lock(lock: _Lock, cleanup: ExitStack) -> None:
    bucket = unique_name()
    create_bucket(lock.s3, bucket, object_lock=False)
    cleanup.callback(delete_bucket, lock.s3, bucket)
    until = _now() + _HOUR

    lock.s3.put_object(
        Bucket=bucket, Key="sem-lock.json", Body=b"{}", ObjectLockMode="GOVERNANCE",
        ObjectLockRetainUntilDate=until,
    )
    retention = lock.s3.get_object_retention(Bucket=bucket, Key="sem-lock.json")["Retention"]

    assert retention == {"Mode": "GOVERNANCE", "RetainUntilDate": until}
    assert outcome(lock.s3.delete_object, Bucket=bucket, Key="sem-lock.json") == "AccessDenied"


def test_audit_sink_grava_evento_com_retencao_compliance(lock: _Lock) -> None:
    event = _event(f"evt-{uuid4().hex}", {"run_id": "run-cap"})
    S3ObjectLockAuditSink(lock.s3, lock.bucket, 1).append(event)
    created = event.created_at
    dated = f"audit/354130/{created:%Y}/{created:%m}/{created:%d}/{event.event_id}.json"

    for key in (dated, f"audit/.event-id/{event.event_id}.json"):
        head = lock.s3.head_object(Bucket=lock.bucket, Key=key)
        assert head["ObjectLockMode"] == "COMPLIANCE"
        assert head["ObjectLockRetainUntilDate"] == created + timedelta(days=1)
        assert head["ContentType"] == "application/json"


def test_audit_sink_reenvio_identico_e_idempotente(lock: _Lock) -> None:
    event = _event(f"evt-{uuid4().hex}", {"run_id": "run-cap"})
    sink = S3ObjectLockAuditSink(lock.s3, lock.bucket, 1)
    sink.append(event)

    sink.append(event)

    prefix = f"audit/.event-id/{event.event_id}"
    listing = lock.s3.list_object_versions(Bucket=lock.bucket, Prefix=prefix)
    assert len(listing["Versions"]) == 1


def test_audit_sink_rejeita_evento_divergente_com_mesmo_id(lock: _Lock) -> None:
    event = _event(f"evt-{uuid4().hex}", {"run_id": "run-cap"})
    sink = S3ObjectLockAuditSink(lock.s3, lock.bucket, 1)
    sink.append(event)

    with pytest.raises(Conflict, match="object=immutable"):
        sink.append(_event(event.event_id, {"run_id": "outro"}))


def test_audit_sink_recusa_bucket_sem_object_lock(lock: _Lock, cleanup: ExitStack) -> None:
    bucket = unique_name()
    create_bucket(lock.s3, bucket, object_lock=False)
    cleanup.callback(delete_bucket, lock.s3, bucket)

    with pytest.raises(ClientError, match="ObjectLockConfigurationNotFoundError"):
        S3ObjectLockAuditSink(lock.s3, bucket, 1)
