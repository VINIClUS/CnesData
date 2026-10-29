"""Prontidão e TTL preparam o root sem aprovar perfis semânticos incompletos."""

import json
import logging
import subprocess
import time
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import pytest

from tests.integration.aws_capability._dynamodb_transactions_testing import queue_transactions_tail
from tests.integration.aws_capability._dynamodb_ttl_gsi_testing import queue_ttl_gsi_tail
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._service_ddb_prepare_testing import (
    Privacy,
    _data,
    _invalid_table,
)
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    ExpectedCall,
    assert_environment,
    assert_private,
    assert_rejected,
)
from tests.integration.aws_capability._service_read_testing import (
    CANARY,
    ReadLab,
    make_read_lab,
    respond,
)
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability._worker import Worker
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Policy,
    Ports,
    RequestContext,
    Resource,
    ServiceFailure,
)

ROOT = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")
Step = tuple[str, Any]


@dataclass
class Clock:
    current: float = 1000
    advances: bool = False
    sleeps: list[float] = field(default_factory=list)

    def now(self) -> float:
        return self.current

    def sleep(self, seconds: float) -> None:
        assert 0 < seconds <= 1
        self.sleeps.append(seconds)
        if self.advances:
            self.current += seconds


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock()
    monkeypatch.setattr(time, "sleep", clock.sleep)
    return clock


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture, clock: Clock
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    lab = make_read_lab(monkeypatch, tmp_path)
    lab.services = replace(lab.services, spec=replace(lab.services.spec, probe="ddb-transactions"))
    lab.services = replace(lab.services, ports=replace(lab.services.ports, now=clock.now))
    lab.context.budget.now = clock.now
    lab.context.budget.limits = Limits(192, 8, 2, 60, 96)
    return lab



@pytest.fixture
def privacy(caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]) -> Privacy:
    caplog.set_level(logging.DEBUG)
    return Privacy(caplog, capsys)


def _queue(lab: ReadLab, steps: Sequence[Step]) -> list[ExpectedCall]:
    expected = []
    for operation, outcome in steps:
        expected.append(tuple(_data()["requests"][operation]))
        if isinstance(outcome, BaseException | tuple):
            lab.process.outcomes.append(outcome)
        else:
            respond(lab, outcome)
    return cast("list[ExpectedCall]", expected)


def _trace(
    lab: ReadLab, expected: Sequence[ExpectedCall], start: int = 0, timeout: float = 7
) -> None:
    assert len(lab.process.calls) == len(expected)
    for (argv, options), (service, operation, region, payload) in zip(
        lab.process.calls, expected, strict=True
    ):
        assert argv == [
            "aws",
            service,
            operation,
            "--region",
            region,
            *FIXED_FLAGS,
            str(max(1, int(timeout))),
            "--cli-read-timeout",
            str(max(1, int(timeout))),
        ]
        assert json.loads(options["input"]) == payload
        if operation == "update-time-to-live":
            assert json.loads(options["input"])["TimeToLiveSpecification"]["Enabled"] is True
        assert options["timeout"] == timeout
        assert options["shell"] is False
        assert options["check"] is False
        assert options["capture_output"] is True
        assert options["text"] is True
        assert_environment(options["env"], region)
    budget = lab.context.budget
    assert budget.used == budget.work_used == start + len(expected)
    assert lab.process.observed_debits == list(range(start + 1, budget.used + 1))
    assert budget.cleanup_deadline is None
    assert lab.process.outcomes == []


def _probe(lab: ReadLab, reason: str = "probe=not_implemented") -> ServiceFailure:
    with pytest.raises(ServiceFailure) as error:
        lab.services.probe([ROOT], replace(lab.services.spec), lab.context)
    assert_rejected(error.value)
    assert error.value.args == (reason,)
    return error.value


def _ready() -> list[Step]:
    data = _data()
    return [("describe", data["active"]), ("tags", data["tags"])]


def _disabled() -> list[Step]:
    return [*_ready(), ("ttl", _data()["ttl"]["disabled"]), *_ready()]


@pytest.mark.parametrize("profile", ["ddb-transactions", "ddb-conditions", "ddb-ttl-gsi"])
def test_preparo_valido_mantem_perfil_incompleto(
    read_lab: ReadLab, profile: str, clock: Clock
) -> None:
    read_lab.services = replace(
        read_lab.services, spec=replace(read_lab.services.spec, probe=profile)
    )
    active = _data()["active"]
    table = active["Table"]
    for key in ("AttributeDefinitions", "KeySchema", "GlobalSecondaryIndexes"):
        table[key].reverse()
    for index in table["GlobalSecondaryIndexes"]:
        index["KeySchema"].reverse()
    steps = [("describe", active), ("tags", _data()["tags"]), ("ttl", _data()["ttl"]["enabled"])]
    expected = _queue(read_lab, steps)
    if profile == "ddb-transactions":
        expected += queue_transactions_tail(read_lab)
    expected += queue_ttl_gsi_tail(read_lab) if profile == "ddb-ttl-gsi" else []
    if profile == "ddb-conditions":
        path = Path(__file__).parent / "fixtures" / "cli" / "b4" / "ddb_cas.json"
        tail = json.loads(path.read_text())["steps"]
        tail.extend(json.loads(path.with_name("ddb_cancel.json").read_text())["steps"])
        tail.extend(json.loads(path.with_name("ddb_token.json").read_text())["steps"])
        for step in tail:
            expected.append(cast("ExpectedCall", tuple(step["request"])))
            reply = step["reply"]
            stdout = json.dumps(reply["data"]) if "data" in reply else ""
            stderr = json.dumps(reply["error"]) if "error" in reply else ""
            read_lab.process.outcomes.append((reply["exit"], stdout, stderr))
    _probe(read_lab)
    _trace(read_lab, expected)
    assert clock.sleeps == []


def test_polls_apos_terceiro_pending_sem_confundir_not_found_com_ausencia(
    read_lab: ReadLab, clock: Clock
) -> None:
    creating = _data()["active"]
    creating["Table"]["TableStatus"] = "CREATING"
    pending_index = _data()["active"]
    pending_index["Table"]["GlobalSecondaryIndexes"][0]["IndexStatus"] = "UPDATING"
    missing = tuple(_data()["failures"]["remote_not_found"])
    steps = [("describe", missing), ("describe", creating), ("describe", pending_index)]
    expected = _queue(read_lab, [*steps, *_ready(), ("ttl", _data()["ttl"]["enabled"])])
    expected += queue_transactions_tail(read_lab)
    _probe(read_lab)
    _trace(read_lab, expected)
    assert clock.sleeps == [1, 1, 1]


@pytest.mark.parametrize("initial", ["disabled", "disabled_ours"])
def test_habilita_ttl_uma_vez_apos_revalidar_root_e_tags(
    read_lab: ReadLab, initial: str, clock: Clock
) -> None:
    steps = [*_ready(), ("ttl", _data()["ttl"][initial]), *_ready()]
    steps += [("update", _data()["echo"])]
    steps += [("ttl", _data()["ttl"]["enabling"])] * 3
    expected = _queue(read_lab, [*steps, ("ttl", _data()["ttl"]["enabled"])])
    expected += queue_transactions_tail(read_lab)
    _probe(read_lab)
    _trace(read_lab, expected)
    assert clock.sleeps == [1, 1, 1]


def test_ttl_ja_enabling_so_le_ate_enabled(read_lab: ReadLab, clock: Clock) -> None:
    expected = _queue(
        read_lab,
        [*_ready(), ("ttl", _data()["ttl"]["enabling"]), ("ttl", _data()["ttl"]["enabled"])],
    )
    expected += queue_transactions_tail(read_lab)
    _probe(read_lab)
    _trace(read_lab, expected)
    assert clock.sleeps == [1]


@pytest.mark.parametrize(
    "case", ["spec", "extra", "duplicado", "estrangeiro", "filho", "lane", "vazio"])
def test_guard_local_rejeita_sem_io(read_lab: ReadLab, case: str) -> None:
    spec, resources, context = read_lab.services.spec, [ROOT], read_lab.context
    if case == "spec":
        spec = replace(spec, deadline=1060.25)
    elif case == "lane":
        context = RequestContext(context.budget, "cleanup")
    elif case == "extra":
        resources += [Resource("s3", ROOT.name, ROOT.name)]
    elif case == "duplicado":
        resources += [ROOT]
    elif case == "vazio":
        resources = []
    else:
        resources = [replace(ROOT, resource_id="foreign", name="foreign")]
        if case == "filho":
            resources = [replace(ROOT, resource_id="foreign")]
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.probe(resources, spec, context)
    assert_rejected(error.value)
    assert error.value.args == ("probe=ddb_prepare_invalid",)
    _trace(read_lab, [])


@pytest.mark.parametrize("profile", ["all", "cognito-login", "s3-hold", "iam-trust", "unknown"])
def test_outros_perfis_preservam_stub_e_resposta_enfileirada(
    read_lab: ReadLab, profile: str
) -> None:
    read_lab.services = replace(
        read_lab.services, spec=replace(read_lab.services.spec, probe=profile)
    )
    respond(read_lab, _data()["active"])
    queued = list(read_lab.process.outcomes)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.probe([ROOT], read_lab.services.spec, read_lab.context)
    assert error.value.args == ("service=not_implemented",)
    assert read_lab.process.calls == read_lab.process.observed_debits == []
    assert read_lab.context.budget.used == read_lab.context.budget.work_used == 0
    assert read_lab.process.outcomes == queued



@pytest.mark.parametrize(
    "case",
    ["arn_conta", "arn_regiao", "arn_nome", "nome", "gsi_ausente", "gsi_duplicado", "gsi_chave",
     "projecao", "atributo", "atributo_duplicado", "chave", "protection", "protection_int",
     "billing",
     "terminal", "status_tipo"],
)
def test_readiness_invalida_nao_le_tags_nem_muta(
    read_lab: ReadLab, case: str, clock: Clock, privacy: Privacy
) -> None:
    expected = _queue(read_lab, [("describe", _invalid_table(case))])
    error = _probe(read_lab, "probe=ddb_table_invalid")
    _trace(read_lab, expected)
    assert clock.sleeps == []
    privacy.check(error, read_lab)


def test_pagina_tags_e_preserva_expires_at_fracionario_em_ambas_leituras(read_lab: ReadLab) -> None:
    read_lab.services = replace(
        read_lab.services, spec=replace(read_lab.services.spec, deadline=1060.25)
    )
    ready = [
        ("describe", _data()["active"]),
        ("tags", _data()["tags1"]),
        ("tags2", _data()["tags2"]),
    ]
    steps = [*ready, ("ttl", _data()["ttl"]["disabled"]), *ready, ("update", _data()["echo"])]
    expected = _queue(read_lab, [*steps, ("ttl", _data()["ttl"]["enabled"])])
    expected += queue_transactions_tail(read_lab)
    _probe(read_lab)
    _trace(read_lab, expected)


@pytest.mark.parametrize("case", ["Run", "ExpiresAt", "protection", "pending"])
def test_drift_fresco_impede_update_sem_tentar_recuperar(
    read_lab: ReadLab, case: str, clock: Clock, privacy: Privacy
) -> None:
    steps = [*_ready(), ("ttl", _data()["ttl"]["disabled"])]
    reason = "probe=ddb_ownership_invalid"
    if case in {"Run", "ExpiresAt"}:
        tags = _data()["tags"]
        next(tag for tag in tags["Tags"] if tag["Key"] == case)["Value"] = CANARY
        steps += [("describe", _data()["active"]), ("tags", tags)]
    else:
        data = _invalid_table("protection") if case == "protection" else _data()["active"]
        if case == "pending":
            data["Table"]["TableStatus"] = "UPDATING"
        steps += [("describe", data)]
        reason = "probe=ddb_table_invalid"
    expected = _queue(read_lab, steps)
    error = _probe(read_lab, reason)
    _trace(read_lab, expected)
    assert clock.sleeps == []
    privacy.check(error, read_lab)


@pytest.mark.parametrize("case", ["duplicada", "ciclo", "ausente"])
def test_tags_ambiguas_ou_ausentes_recusam_preparo(read_lab: ReadLab, case: str) -> None:
    tags = _data()["tags"]
    if case == "duplicada":
        tags["Tags"].append(tags["Tags"][0])
    elif case == "ausente":
        tags["Tags"].pop(2)
    else:
        tags = {"Tags": [], "NextToken": "pagina-2"}
    steps = [("describe", _data()["active"]), ("tags", tags)]
    if case == "ciclo":
        steps += [("tags2", tags)]
    expected = _queue(read_lab, steps)
    reason = {
        "duplicada": "service=tags_ambiguous",
        "ciclo": "service=tags_cycle",
        "ausente": "probe=ddb_ownership_invalid",
    }[case]
    _probe(read_lab, reason)
    _trace(read_lab, expected)


@pytest.mark.parametrize(
    "case",
    ["disabling", "foreign", "disabled_foreign", "missing", "malformed", "unknown",
     "bad_echo", "post_disabled", "echo_int"],
)
def test_ttl_invalido_nao_repete_mutacao(
    read_lab: ReadLab, case: str, clock: Clock, privacy: Privacy
) -> None:
    if case in {"bad_echo", "post_disabled", "echo_int"}:
        echo = _data()["bad_echo" if case == "bad_echo" else "echo"]
        if case == "echo_int":
            echo["TimeToLiveSpecification"]["Enabled"] = 1
        steps = [*_disabled(), ("update", echo)]
        if case == "post_disabled":
            steps += [("ttl", _data()["ttl"]["disabled"])]
    else:
        steps = [*_ready(), ("ttl", _data()["ttl"][case])]
    expected = _queue(read_lab, steps)
    error = _probe(read_lab, "probe=ddb_ttl_invalid")
    _trace(read_lab, expected)
    assert clock.sleeps == []
    privacy.check(error, read_lab)


@pytest.mark.parametrize(("phase", "profile"),
                         [("describe", "ddb-transactions"), ("ttl", "ddb-conditions")])
def test_pending_esgota_saldo_existente_com_clock_constante_e_preserva_reserva(
    read_lab: ReadLab, phase: str, profile: str, clock: Clock
) -> None:
    budget = read_lab.context.budget
    read_lab.services = replace(
        read_lab.services, spec=replace(read_lab.services.spec, probe=profile)
    )
    if profile == "ddb-conditions":
        budget.limits = Limits(256, 8, 2, 60, 128)
    allowance = 94 if phase == "describe" else 126
    budget.acquire("work")
    budget.acquire("work")
    pending = _data()["active"] if phase == "describe" else _data()["ttl"]["enabling"]
    if phase == "describe":
        pending["Table"]["TableStatus"] = "UPDATING"
    steps = [] if phase == "describe" else _ready()
    steps += [(phase, pending)] * (allowance - len(steps))
    expected = _queue(read_lab, steps)
    with pytest.raises(BudgetExhausted, match=r"^budget=requests$"):
        read_lab.services.probe([ROOT], read_lab.services.spec, read_lab.context)
    _trace(read_lab, expected, 2)
    assert clock.current == 1000
    assert clock.sleeps == [1] * (93 if phase == "describe" else 123)
    assert budget.limits.calls - budget.used == budget.limits.cleanup_reserve
    assert budget.limits.cleanup_reserve == (96 if phase == "describe" else 128)
    assert budget.deadline == 1060


@pytest.mark.parametrize("phase", ["describe", "ttl"])
def test_expira_durante_pausa_limitada_ao_saldo_sem_novo_processo(
    read_lab: ReadLab, phase: str, clock: Clock
) -> None:
    clock.advances = True
    read_lab.context.budget.deadline = 1000.25
    pending = _data()["active"] if phase == "describe" else _data()["ttl"]["enabling"]
    if phase == "describe":
        pending["Table"]["TableStatus"] = "CREATING"
    steps = [] if phase == "describe" else _ready()
    expected = _queue(read_lab, [*steps, (phase, pending)])
    with pytest.raises(BudgetExhausted, match=r"^budget=deadline$"):
        read_lab.services.probe([ROOT], read_lab.services.spec, read_lab.context)
    _trace(read_lab, expected, timeout=0.25)
    assert clock.sleeps == [0.25]
    assert read_lab.context.budget.deadline == 1000.25


@pytest.mark.parametrize("case", ["reserva", "deadline"])
def test_orcamento_impede_primeira_leitura_sem_debito(read_lab: ReadLab, case: str) -> None:
    if case == "reserva":
        read_lab.context.budget.limits = replace(read_lab.context.budget.limits, calls=96)
    else:
        read_lab.context.budget.deadline = 1000
    with pytest.raises(BudgetExhausted):
        read_lab.services.probe([ROOT], read_lab.services.spec, read_lab.context)
    _trace(read_lab, [])


@pytest.mark.parametrize(
    "case",
    ["timeout", "invalid_json", "denied", "local_not_found", "generic_404", "internal",
     "tags_not_found", "update_lost"],
)
def test_falha_de_transporte_nao_repete_nem_vaza_dados(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    prefix, operation = [], "describe"
    if case == "tags_not_found":
        prefix, operation = [("describe", _data()["active"])], "tags"
    elif case == "update_lost":
        prefix, operation = _disabled(), "update"
    if case in {"timeout", "update_lost"}:
        outcome = subprocess.TimeoutExpired("aws", 7, output=CANARY, stderr=CANARY)
    else:
        key = "remote_not_found" if case == "tags_not_found" else case
        outcome = tuple(_data()["failures"][key])
    expected = _queue(read_lab, [*prefix, (operation, outcome)])
    reason = "service=describe_failed" if operation == "describe" else "service=request_failed"
    error = _probe(read_lab, reason)
    _trace(read_lab, expected)
    captured = capsys.readouterr()
    assert_private(error, read_lab, caplog.text + captured.out + captured.err)


def test_worker_persiste_root_confirmado_antes_do_primeiro_probe(
    read_lab: ReadLab, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = Journal(read_lab.services.spec)
    observed = []
    original = read_lab.process.run

    def record(argv: Sequence[str], **options: Any) -> subprocess.CompletedProcess[str]:
        if list(argv)[1:3] == ["dynamodb", "describe-table"]:
            saved = Journal(read_lab.services.spec)
            saved.load()
            assert saved.created == [ROOT]
            assert saved.pending_service is None
            assert saved.operation_state == "confirmed"
            assert saved.phase == "probing"
            observed.append(saved.path.read_bytes())
        return original(argv, **options)

    monkeypatch.setattr(subprocess, "run", record)
    path = Path(__file__).parent / "fixtures" / "cli" / "b3" / "ddb_create.json"
    creation = json.loads(path.read_text())
    respond(read_lab, creation["creating"])
    expected = [("dynamodb", "create-table", "us-east-2", creation["request"])]
    expected += _queue(read_lab, [*_ready(), ("ttl", _data()["ttl"]["enabled"])])
    expected += queue_transactions_tail(read_lab)
    budget = read_lab.context.budget
    policy = Policy("111111111111", frozenset({"us-east-2"}), budget.limits)
    ports = Ports(read_lab.services, ProcessBoundary([]), budget.now, policy, budget)
    assert Worker(journal, ports, 9876).execute() == "operation_failed"
    assert len(observed) == 1
    saved = Journal(journal.spec)
    saved.load()
    assert saved.created == [ROOT]
    assert saved.phase == "probing"
    assert saved.pending_service is None
    assert saved.operation_state == "confirmed"
    _trace(read_lab, expected)
