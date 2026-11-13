"""Provider de admissão sintética: record e control fortes do control plane."""
from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from hashlib import sha256
from typing import TYPE_CHECKING, Any, cast

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    ModeCanaryGrant,
    SyntheticContextRequired,
)
from cnes_infra.aws.mode_canary import ModeCanaryContext, canonical_text
from cnes_infra.aws.runtime import build_dispatch_admission
from cnes_infra.aws.settings import AwsRuntimeConfigurationError, AwsRuntimeSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import tenant_partition
from packages.cnes_domain.tests.orchestration.test_mode_canary_records import (
    CANARY_ID,
    _control,
    _promotion,
)
from packages.cnes_infra.tests.aws.production_values import PRODUCTION_VALUES
from packages.cnes_infra.tests.aws.test_mode_canary_ledger import _Dispatches
from packages.cnes_infra.tests.aws.test_runtime import _LOCKED, _clients, _mock, _settings
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

_TENANT = f"prod-canary-{CANARY_ID}"
_RUN = "unit-aaaaaaaaaaaaaaaa"
_HOLDER = PermitHolder(_TENANT, _RUN, "0123456789abcdef")
_NOW = datetime(2026, 11, 12, 12, 10, tzinfo=UTC)
_OWNER = "a" * 64


class _Env:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.record = _promotion()
        self.control = _control()

    def put(self, record: dict[str, Any] | None = None, control: dict[str, Any] | None = None):
        record = self.record if record is None else record
        control = self.control if control is None else control
        text = canonical_text(record)
        self._put(f"PROD_CANARY#{_OWNER}#{record['phase']}", text)
        self._put(f"PROD_CANARY_CONTROL#{_OWNER}", canonical_text(control))
        return sha256(text.encode()).hexdigest()

    def _put(self, sort_key: str, text: str) -> None:
        self.client.put_item(TableName=_TABLE_NAME, Item={
            "pk": {"S": tenant_partition(_TENANT)}, "sk": {"S": sort_key},
            "entity": {"S": "MODECANARY"}, "payload": {"S": text},
        })

    def context(self, record_sha: str | None = None) -> ModeCanaryContext:
        digest = record_sha or sha256(canonical_text(self.record).encode()).hexdigest()
        binding = ModeCanaryBinding(
            tenant_id=_TENANT, run_ids=(_RUN,), owner_sha256=_OWNER, record_sha256=digest,
            phase="unit", context_kind="promotion",
        )
        return ModeCanaryContext(self.client, _TABLE_NAME, binding, _Dispatches())


@pytest.fixture
def env() -> Iterator[_Env]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        yield _Env(client)


def _mutated(base: dict[str, Any], **changes: Any) -> dict[str, Any]:
    mapping = deepcopy(base)
    mapping.update(changes)
    return mapping


def test_canonical_text_e_ordenado_compacto_com_lf() -> None:
    assert canonical_text({"b": 1, "a": "é"}) == '{"a":"é","b":1}\n'


def test_binding_for_devolve_o_promotion_id_do_record_ativo(env: _Env) -> None:
    env.put()

    grant = env.context().binding_for(_HOLDER, _NOW)

    assert grant == ModeCanaryGrant("promo-1")


def test_record_valido_nao_autoriza_scheduler_normal(env: _Env) -> None:
    env.put()
    other = PermitHolder("354130", "run-a", "0123456789abcdef")

    with pytest.raises(SyntheticContextRequired):
        env.context().binding_for(other, _NOW)


def test_mesma_flag_sem_registro_nao_admite(env: _Env) -> None:
    with pytest.raises(SyntheticContextRequired, match="reason=record_missing"):
        env.context().binding_for(_HOLDER, _NOW)


def test_control_ausente_ou_selado_nao_admite(env: _Env) -> None:
    env.put()
    env.client.delete_item(TableName=_TABLE_NAME, Key={
        "pk": {"S": tenant_partition(_TENANT)}, "sk": {"S": f"PROD_CANARY_CONTROL#{_OWNER}"},
    })
    with pytest.raises(SyntheticContextRequired, match="reason=control_missing"):
        env.context().binding_for(_HOLDER, _NOW)
    sealed = _mutated(env.control, admission_state="SEALED", sealed_at="2026-11-12T12:05:00Z")
    env.put(control=sealed)
    with pytest.raises(SyntheticContextRequired, match="reason=control_sealed"):
        env.context().binding_for(_HOLDER, _NOW)


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda e: e.put(_mutated(e.record, owner_sha256="c" * 64)), "record_mismatch"),
        (lambda e: e.put(_mutated(e.record, canary_id="f" * 32)), "record_mismatch"),
        (lambda e: e.put(_mutated(e.record, run_ids=["unit-bbbbbbbbbbbbbbbb"])), "run_not_bound"),
        (lambda e: e.put(control=_mutated(e.control, owner_sha256="c" * 64)), "control_mismatch"),
        (lambda e: e.put(control=_mutated(e.control, generation=9)), "control_mismatch"),
        (
            lambda e: e.put(control=_mutated(e.control, context_manifest_sha256="c" * 64)),
            "control_mismatch",
        ),
        (lambda e: e.put(control=_mutated(e.control, deadline="2026-11-12T12:05:00Z")),
         "context_expired"),
        (lambda e: e.put(_mutated(e.record, expires_at="2026-11-12T12:05:00Z")),
         "context_expired"),
        (lambda e: e.put(_mutated(e.record, canary_deadline="2026-11-12T12:05:00Z")),
         "context_expired"),
    ],
)
def test_contexto_divergente_ou_vencido_nao_admite(
    env: _Env, mutate: Any, reason: str,
) -> None:
    mutate(env)
    context = env.context(sha256(_stored(env).encode()).hexdigest())

    with pytest.raises(SyntheticContextRequired, match=f"reason={reason}"):
        context.binding_for(_HOLDER, _NOW)


def _stored(env: _Env) -> str:
    key = {"pk": {"S": tenant_partition(_TENANT)}, "sk": {"S": f"PROD_CANARY#{_OWNER}#unit"}}
    return env.client.get_item(TableName=_TABLE_NAME, Key=key)["Item"]["payload"]["S"]


def test_hash_do_record_divergente_do_binding_nao_admite(env: _Env) -> None:
    env.put()

    with pytest.raises(SyntheticContextRequired, match="reason=record_hash_mismatch"):
        env.context("f" * 64).binding_for(_HOLDER, _NOW)


def test_control_invalido_ou_nao_canonico_nao_admite(env: _Env) -> None:
    env.put()
    key = f"PROD_CANARY_CONTROL#{_OWNER}"
    pretty = json.dumps(env.control, indent=2)
    env._put(key, pretty)
    with pytest.raises(SyntheticContextRequired, match="reason=control_not_canonical"):
        env.context().binding_for(_HOLDER, _NOW)
    env._put(key, canonical_text(_mutated(env.control, admission_state="X")))
    with pytest.raises(SyntheticContextRequired, match="reason=control_invalid"):
        env.context().binding_for(_HOLDER, _NOW)


def test_record_nao_canonico_ou_invalido_nao_admite(env: _Env) -> None:
    env.put()
    key = f"PROD_CANARY#{_OWNER}#unit"
    pretty = json.dumps(env.record, indent=2)
    env._put(key, pretty)
    context = env.context(sha256(pretty.encode()).hexdigest())
    with pytest.raises(SyntheticContextRequired, match="reason=record_not_canonical"):
        context.binding_for(_HOLDER, _NOW)
    broken = canonical_text(_mutated(env.record, fault="shell"))
    env._put(key, broken)
    context = env.context(sha256(broken.encode()).hexdigest())
    with pytest.raises(SyntheticContextRequired, match="reason=record_invalid"):
        context.binding_for(_HOLDER, _NOW)
    env._put(key, "{")
    context = env.context(sha256(b"{").hexdigest())
    with pytest.raises(SyntheticContextRequired, match="reason=record_invalid"):
        context.binding_for(_HOLDER, _NOW)
    env._put(key, "[]")
    context = env.context(sha256(b"[]").hexdigest())
    with pytest.raises(SyntheticContextRequired, match="reason=record_invalid"):
        context.binding_for(_HOLDER, _NOW)


class _Provider:
    def binding_for(self, holder: object, now: datetime) -> object:
        raise AssertionError((holder, now))


def test_admissao_de_producao_repassa_o_provider_sintetico() -> None:
    settings = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES)
    clients = _clients(_LOCKED)
    control_plane = DynamoDBControlPlane(
        clients.dynamodb, settings.control_plane_table, lambda: _NOW,
    )
    provider = _Provider()

    admission = build_dispatch_admission(settings, clients, control_plane, cast("Any", provider))

    assert isinstance(admission, EnvironmentAdmission)
    assert admission._mode_canary is provider
    default = build_dispatch_admission(settings, clients, control_plane)
    assert isinstance(default, EnvironmentAdmission)
    assert default._mode_canary is None


def test_admissao_sintetica_fora_da_politica_de_producao_falha_fechado() -> None:
    clients = _clients(_LOCKED)

    with pytest.raises(AwsRuntimeConfigurationError, match=r"^mode_canary_requires_production$"):
        build_dispatch_admission(
            _settings(), clients, cast("Any", object()), cast("Any", _Provider()),
        )

    assert _mock(clients.step_functions).mock_calls == []


_VECTOR_RECORD = {
    "schema": "cnesdata.mode-canary/v2", "activation_sha256": "a" * 64,
    "candidate_sha256": "b" * 64, "source_sha": "c" * 40, "release_id": "liberação-é",
    "canary_id": "0123456789abcdef0123456789abcdef", "owner_sha256": "d" * 64,
    "promotion_id": "promoção-1", "promotion_generation": 3, "phase_receipt_sha256": "e" * 64,
    "machine_arn": "arn:aws:states:us-east-2:111122223333:stateMachine:m",
    "revisions": {"unit": "1", "recovery": "2", "audit": "3"}, "run_ids": ["r1", "r2"],
    "fixture_manifest_sha256": "1" * 64, "seed_manifest_sha256": "2" * 64,
    "fixed_deadline": "2026-11-13T12:00:00+00:00", "stage_deadline": "2026-11-13T12:30:00+00:00",
    "canary_deadline": "2026-11-13T11:45:00+00:00", "phase": "unit",
    "freeze_policy_sha256": "3" * 64, "expires_at": "2026-11-13T13:00:00+00:00",
    "context_kind": "promotion", "context_manifest_sha256": "4" * 64, "scenario": "unit_wave",
    "fault": "none", "mode": "unit",
}
_VECTOR_PK = (
    "TENANT#70726f642d63616e6172792d30313233343536373839616263646566"
    "30313233343536373839616263646566"
)


def test_item_do_vetor_compartilhado_admite_com_control_correspondente(env: _Env) -> None:
    text = canonical_text(_VECTOR_RECORD)
    assert sha256(text.encode()).hexdigest() == (
        "09ea5e922b648bfeedb1cbb5fd8ea724202707b888427e1bd9c30d032b5e7063"
    )
    assert tenant_partition(_TENANT) == _VECTOR_PK
    owner = "d" * 64
    for sort_key, payload in (
        (f"PROD_CANARY#{owner}#unit", text),
        (f"PROD_CANARY_CONTROL#{owner}", canonical_text(_mutated(
            _control(), owner_sha256=owner, context_manifest_sha256="4" * 64,
            admitted_at="2026-11-13T11:00:00Z", deadline="2026-11-13T12:00:00Z"))),
    ):
        env.client.put_item(TableName=_TABLE_NAME, Item={
            "pk": {"S": _VECTOR_PK}, "sk": {"S": sort_key}, "entity": {"S": "MODECANARY"},
            "payload": {"S": payload},
        })
    binding = ModeCanaryBinding(_TENANT, ("r1",), owner, sha256(text.encode()).hexdigest(),
                                "unit", "promotion")
    context = ModeCanaryContext(env.client, _TABLE_NAME, binding, _Dispatches())
    now = datetime(2026, 11, 13, 11, 30, tzinfo=UTC)

    assert context.binding_for(PermitHolder(_TENANT, "r1", "0123456789abcdef"), now) == (
        ModeCanaryGrant("promoção-1")
    )
