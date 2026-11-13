"""Provider de admissão sintética: record e control fortes do control plane."""
from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from hashlib import sha256
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    ModeCanaryGrant,
    SyntheticContextRequired,
)
from cnes_infra.aws.mode_canary import ModeCanaryContext, canonical_text
from cnes_infra.control_plane.dynamodb_keys import tenant_partition
from packages.cnes_domain.tests.orchestration.test_mode_canary_records import (
    CANARY_ID,
    _control,
    _promotion,
)
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
        return ModeCanaryContext(self.client, _TABLE_NAME, binding)


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
