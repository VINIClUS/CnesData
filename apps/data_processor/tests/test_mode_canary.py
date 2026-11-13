"""Facade do control plane e executor do canary de modo no runtime do processor."""
from __future__ import annotations

import inspect
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.mode_canary import ModeCanaryBinding, SyntheticContextRequired
from cnes_domain.ports.processing import StartRunExecution
from cnes_infra.control_plane.dynamodb_keys import run_partition
from data_processor.aws_entrypoint import AwsMode
from data_processor.mode_canary import (
    CanaryControlPlane,
    ModeCanaryExecutor,
    SyntheticFixtureReadFailure,
    enforce_mode_canary,
    fault_object_store,
    load_unit_binding,
)
from packages.cnes_domain.tests.orchestration.test_mode_canary_records import _promotion
from packages.cnes_infra.tests.aws.test_mode_canary_ledger import (
    _DISPATCH,
    _HOLDER,
    _NOW,
    _OWNER,
    _RUN,
    _TENANT,
    _UNITS,
    _Ledger,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _TABLE_NAME, _create_table

from .test_aws_entrypoint import ENVELOPE
from .test_deadline import _child_environment

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def ledger() -> Iterator[_Ledger]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        yield _Ledger(client)


@dataclass
class _Plane:
    runs: dict[tuple[str, str], object]
    calls: list[str] = field(default_factory=list[str])

    def get_run(self, tenant_id: str, run_id: str) -> object | None:
        self.calls.append(f"get_run:{run_id}")
        return self.runs.get((tenant_id, run_id))

    def list_recoverable_runs(self, now: datetime, limit: int = 100) -> tuple[object, ...]:
        raise AssertionError("global_gsi4")

    def reserve_run_dispatch(self, command: Any) -> str:
        self.calls.append(f"reserve:{command.run_id}")
        return "reserved"


def _facade(plane: _Plane) -> CanaryControlPlane:
    binding = ModeCanaryBinding(_TENANT, (_RUN, "unit-bbbbbbbbbbbbbbbb"), _OWNER, "b" * 64,
                                "recovery", "promotion")
    return CanaryControlPlane(cast("Any", plane), binding)


def test_recovery_canary_nao_consulta_gsi4_global() -> None:
    run = SimpleNamespace(tenant_id=_TENANT, run_id=_RUN)
    plane = _Plane({(_TENANT, _RUN): run})

    runs = _facade(plane).list_recoverable_runs(_NOW, 100)

    assert runs == (run,)
    assert plane.calls == [f"get_run:{_RUN}", "get_run:unit-bbbbbbbbbbbbbbbb"]
    assert _facade(plane).list_recoverable_runs(_NOW, 1) == (run,)


def test_facade_recusa_tenant_ou_run_fora_do_binding_e_metodos_globais() -> None:
    plane = _Plane({})
    facade = cast("Any", _facade(plane))
    command = SimpleNamespace(tenant_id=_TENANT, run_id=_RUN)

    assert facade.reserve_run_dispatch(cast("Any", command)) == "reserved"
    for tenant, run in (("354130", _RUN), (_TENANT, "unit-x")):
        with pytest.raises(SyntheticContextRequired, match="reason=outside_binding"):
            facade.get_run(tenant, run)
        with pytest.raises(SyntheticContextRequired, match="reason=outside_binding"):
            facade.reserve_run_dispatch(cast("Any", SimpleNamespace(tenant_id=tenant, run_id=run)))
    with pytest.raises(SyntheticContextRequired, match="reason=outside_binding"):
        facade.get_dataset_pointer("354130", "cnes")
    for name in ("read_outbox_page", "put_tenant", "create_job", "list_revocable_runs"):
        with pytest.raises(AttributeError):
            getattr(facade, name)
    assert inspect.getattr_static(facade, "claim_run_unit") is not None


@dataclass
class _Executor:
    calls: list[str] = field(default_factory=list[str])

    def start(self, request: StartRunExecution) -> str:
        self.calls.append("start")
        return "arn:exec"

    def start_with_claim(self, request: StartRunExecution, claim: Any) -> str:
        self.calls.append("start_with_claim")
        return "arn:exec"

    def status(self, execution_ref: str) -> str:
        return "RUNNING"

    def cancel(self, request: Any) -> None:
        self.calls.append("cancel")


def _request(tenant: str = _TENANT) -> StartRunExecution:
    return StartRunExecution(tenant_id=tenant, run_id=_RUN, wave_id="a" * 16,
                             dispatch_id=_DISPATCH, unit_ids=_UNITS, max_concurrency=1)


def test_start_sintetico_checa_ticket_vivo_imediatamente_antes(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    inner = _Executor()
    executor = ModeCanaryExecutor(cast("Any", inner), context, lambda: _NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=ticket_missing"):
        executor.start_with_claim(_request(), object())
    with pytest.raises(SyntheticContextRequired, match="reason=claim_required"):
        executor.start(_request())
    assert inner.calls == []
    context.binding_for(_HOLDER, _NOW)

    assert executor.start_with_claim(_request(), object()) == "arn:exec"
    assert executor.start(_request("354130")) == "arn:exec"
    assert executor.status("arn:exec") == "RUNNING"
    executor.cancel(object())
    assert inner.calls == ["start_with_claim", "start", "cancel"]
    context.seal(_NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=control_sealed"):
        executor.start_with_claim(_request(), object())


def test_facade_aceita_identidade_nomeada_e_executor_normal_dispensa_ticket() -> None:
    plane = _Plane({(_TENANT, _RUN): "run"})
    facade = cast("Any", _facade(plane))

    assert facade.get_run(tenant_id=_TENANT, run_id=_RUN) == "run"
    with pytest.raises(SyntheticContextRequired, match="reason=outside_binding"):
        facade.get_run(tenant_id="354130", run_id=_RUN)
    inner = _Executor()
    executor = ModeCanaryExecutor(cast("Any", inner), cast("Any", None), lambda: _NOW)
    assert executor.start_with_claim(_request("354130"), object()) == "arn:exec"
    assert inner.calls == ["start_with_claim"]


def test_unit_carrega_o_binding_gravado_para_o_seu_dispatch(ledger: _Ledger) -> None:
    ledger.seed()
    ledger.context().binding_for(_HOLDER, _NOW)

    binding = load_unit_binding(ledger.client, _TABLE_NAME, _HOLDER)

    assert binding == ModeCanaryBinding(
        _TENANT, (_RUN,), _OWNER, ledger.record_sha, "unit", "promotion",
    )


def test_unit_sem_binding_ou_com_binding_adulterado_nao_compoe(ledger: _Ledger) -> None:
    with pytest.raises(SyntheticContextRequired, match="reason=binding_missing"):
        load_unit_binding(ledger.client, _TABLE_NAME, _HOLDER)
    ledger.seed()
    ledger.context().binding_for(_HOLDER, _NOW)
    key = {"pk": {"S": run_partition(_TENANT, _RUN)}, "sk": {"S": f"PROD_CANARY#{_DISPATCH}"}}
    stored = ledger.client.get_item(TableName=_TABLE_NAME, Key=key)["Item"]
    original = stored["payload"]["S"]
    wrong_phase = original.replace('"phase":"unit"', '"phase":"x"')
    for payload in (original.replace(_DISPATCH, "f" * 16), original.rstrip("\n"), "[]\n", "{",
                    wrong_phase):
        ledger.client.put_item(TableName=_TABLE_NAME, Item=stored | {"payload": {"S": payload}})
        with pytest.raises(SyntheticContextRequired, match="reason=binding_invalid"):
            load_unit_binding(ledger.client, _TABLE_NAME, _HOLDER)


def _hang_record(ledger: _Ledger, fault: str, scenario: str) -> None:
    ledger.record = ledger.record | {"fault": fault, "scenario": scenario}
    ledger.seed()


def test_hang_no_dispatch_bloqueia_recovery_apos_encurtar_o_teto(
    ledger: _Ledger, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _hang_record(ledger, "hang_no_dispatch", "recovery_deadline")
    shortened: list[float] = []
    monkeypatch.setattr("data_processor.mode_canary.shorten_active_deadline", shortened.append)
    waits: list[str] = []

    enforce_mode_canary(ledger.context(), AwsMode.RECOVER_ONCE, lambda: _NOW,
                        lambda: waits.append("wait"))

    assert shortened == [20 * 60]
    assert waits == ["wait"]
    enforce_mode_canary(ledger.context(), AwsMode.RUN_UNIT, lambda: _NOW,
                        lambda: waits.append("unit"))
    assert waits == ["wait"]
    enforce_mode_canary(None, AwsMode.RECOVER_ONCE, lambda: _NOW, lambda: waits.append("x"))
    assert waits == ["wait"]


def test_contexto_selado_ou_vencido_nao_inicia_runtime(ledger: _Ledger) -> None:
    ledger.seed()
    context = ledger.context()
    with pytest.raises(SyntheticContextRequired, match="reason=context_expired"):
        enforce_mode_canary(context, AwsMode.RECOVER_ONCE,
                            lambda: datetime(2026, 11, 12, 12, 30, tzinfo=UTC), lambda: None)
    context.seal(_NOW)
    with pytest.raises(SyntheticContextRequired, match="reason=control_sealed"):
        enforce_mode_canary(context, AwsMode.RECOVER_ONCE, lambda: _NOW, lambda: None)


class _Store:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def open(self, key: str) -> Any:
        self.calls.append(f"open:{key}")
        return "handle"

    def put(self, key: str, body: Any, expected_sha256: str) -> str:
        self.calls.append(f"put:{key}")
        return "stat"


def test_falha_sintetica_de_leitura_so_atinge_a_fixture_raw(ledger: _Ledger) -> None:
    _hang_record(ledger, "synthetic_read_failure", "recovery_failure")
    inner = cast("Any", _Store())

    store = cast("Any", fault_object_store(ledger.context(), inner))

    with pytest.raises(SyntheticFixtureReadFailure, match="reason=synthetic_read_failure"):
        store.open(f"raw/{_TENANT}/CNES_LOCAL/2026-01/m/data.parquet")
    assert store.open("normalized/x") == "handle"
    assert store.put("tmp/x", b"", "0" * 64) == "stat"
    assert inner.calls == ["open:normalized/x", "put:tmp/x"]
    ledger.record = _promotion()
    ledger.seed()
    assert fault_object_store(ledger.context(), inner) is inner
    assert fault_object_store(None, inner) is inner


def _profile(argv: list[str], extra: dict[str, str], runtime: Any) -> tuple[int, list[Any]]:
    from data_processor import main as app

    calls: list[Any] = []
    values = _child_environment("in-process") | extra
    with (
        patch.dict(os.environ, values, clear=True),
        patch.object(sys, "argv", ["data_processor", *argv]),
        patch.object(app, "configure_json_stdout"),
        patch.object(app, "init_telemetry"),
        patch.object(app, "Session"),
        patch.object(app, "build_aws_unit_runtime",
                     side_effect=lambda *a: calls.append(("unit", a[3])) or runtime),
        patch.object(app, "build_aws_recovery_runtime",
                     side_effect=lambda *a: calls.append(("recovery", a[3])) or runtime),
        patch.object(app, "enforce_mode_canary",
                     side_effect=lambda *a: calls.append(("enforce", a[0], a[1]))),
        patch.object(app, "run_aws_entrypoint", side_effect=lambda *a: calls.append("run") or 0),
    ):
        return app._run_aws_profile(), calls


def test_main_instala_locator_e_aplica_teto_antes_do_trabalho() -> None:
    runtime = SimpleNamespace(mode_canary="context")
    locator = (
        '{"context_kind":"promotion","owner_sha256":"' + _OWNER + '","phase":"recovery",'
        '"record_sha256":"' + "b" * 64 + '","run_ids":["' + _RUN + '"],"tenant_id":"' + _TENANT
        + '"}'
    )

    code, calls = _profile(["recover-once"], {"PROD_CANARY_BINDING": locator}, runtime)

    assert code == 0
    assert calls[0][0] == "recovery"
    assert calls[0][1].mode_canary.run_ids == (_RUN,)
    assert calls[0][1].unit_dispatch is None
    assert calls[1:] == [("enforce", "context", AwsMode.RECOVER_ONCE), "run"]


def test_main_unit_sintetica_localiza_o_dispatch_e_normal_nao() -> None:
    runtime = SimpleNamespace(mode_canary=None)
    synthetic = ENVELOPE | {"TENANT_ID": _TENANT, "RUN_ID": _RUN}

    _, calls = _profile([], synthetic, runtime)
    _, normal = _profile([], ENVELOPE, runtime)

    policy = calls[0][1]
    assert policy.unit_dispatch == PermitHolder(_TENANT, _RUN, ENVELOPE["DISPATCH_ID"])
    assert policy.mode_canary is None
    assert normal[0][1].unit_dispatch is None
