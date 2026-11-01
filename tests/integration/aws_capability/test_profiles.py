"""Perfis duráveis e limites finitos sem ampliar o ensaio sintético padrão."""

import json
from dataclasses import asdict, replace

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._testing import ACCOUNT, NAME, Lab
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Limits,
    Policy,
    Ports,
    RequestBudget,
    Resource,
)

PROFILES = [
    ("cognito-login", 512, 240, 256, ("cognito",)),
    ("cognito-pkce-negatives", 512, 240, 256, ("cognito",)),
    ("cognito-sdk", 256, 240, 128, ("cognito",)),
    ("ddb-transactions", 192, 150, 96, ("dynamodb",)),
    ("ddb-conditions", 256, 150, 128, ("dynamodb",)),
    ("ddb-ttl-gsi", 256, 150, 128, ("dynamodb",)),
    ("ddb-throughput", 128, 150, 64, ("dynamodb",)),
    ("s3-retention", 384, 240, 192, ("s3", "iam")),
    ("s3-hold", 256, 120, 128, ("s3",)),
    ("iam-trust", 512, 120, 256, ("iam", "iam")),
    ("iam-leadingkeys", 512, 90, 256, ("dynamodb", "iam")),
    ("iam-s3", 384, 90, 192, ("s3", "iam")),
]
ProfileCase = tuple[str, int, int, int, tuple[str, ...]]


def profile_ports(lab: Lab, limits: Limits) -> Ports:
    current = lab.ports()
    policy = Policy(ACCOUNT, frozenset({"us-east-2"}), limits)
    budget = RequestBudget(limits, lab.spec.deadline, lambda: lab.time)
    return replace(current, policy=policy, budget=budget)


def argv(lab: Lab, command: str = "run") -> list[str]:
    return [
        command, "--account", ACCOUNT, "--region", "us-east-2",
        "--run-id", lab.spec.run_id, "--deadline", str(lab.spec.deadline),
        "--journal-dir", str(lab.spec.journal_dir), "--retention", "GOVERNANCE",
        "--allow-governance-bypass",
    ]


def schema2(lab: Lab, resource: Resource) -> dict:
    prepare_directory(lab.spec.journal_dir)
    journal = Journal(lab.spec, phase="cleanup_requested", created=[resource])
    data = json.loads(journal.content())
    data.update({
        "schema_version": 2,
        "probe": "ddb-transactions",
        "profile_version": 1,
        "limits": {
            "calls": 192, "pages": 8, "attempts": 2,
            "cleanup_seconds": 150, "cleanup_reserve": 96,
        },
    })
    lab.journal().write_text(json.dumps(data), encoding="utf-8")
    lab.journal().chmod(0o600)
    return data


def assert_no_mutation(lab: Lab) -> None:
    assert not any(e[0] in {"create", "probe", "delete"} for e in lab.events)


@pytest.mark.parametrize("case", PROFILES, ids=[row[0] for row in PROFILES])
def test_perfil_duravel_nao_amplia_create(
    lab: Lab, case: ProfileCase
) -> None:
    profile, calls, seconds, reserve, services = case
    lab.spec = replace(lab.spec, probe=profile)
    limits = Limits(calls, 8, 2, seconds, reserve)
    at_create: list[dict] = []
    lab.remote.at_create = lambda spec: at_create.append(json.loads(lab.journal().read_bytes()))
    outcome = lifecycle.run(lab.spec, profile_ports(lab, limits))
    assert outcome.status == "succeeded"
    assert tuple(e[1] for e in lab.events if e[0] == "create") == services
    data = json.loads(lab.journal().read_bytes())
    assert data["schema_version"] == 2
    assert data["probe"] == profile
    assert data["profile_version"] == 1
    assert data["limits"] == asdict(limits)
    assert at_create
    assert all(row["probe"] == profile and row["limits"] == asdict(limits) for row in at_create)
    assert data["phase"] == "complete"
    assert data["created"] == []
    scans = [e[1] for e in lab.events if e[0] == "inventory"]
    assert scans[:4] == ["cognito", "dynamodb", "s3", "iam"]
    assert scans[-4:] == ["cognito", "dynamodb", "s3", "iam"]
    probes = [e for e in lab.events if e[0] == "probe"]
    assert len(probes) == 1
    assert probes[0][1] == tuple(e[1] for e in lab.events if e[0] == "created")


def test_schema2_persiste_mesmo_antes_da_primeira_criacao(lab: Lab) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions")
    snapshots: list[dict] = []
    lab.remote.at_create = lambda spec: snapshots.append(json.loads(lab.journal().read_bytes()))
    lifecycle.run(lab.spec, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert snapshots
    assert snapshots[0]["schema_version"] == 2
    assert snapshots[0]["probe"] == "ddb-transactions"
    assert snapshots[0]["pending_service"] == "dynamodb"
    assert snapshots[0]["operation_state"] == "unknown"


def test_default_all_mantem_roots_limites_e_bytes_schema1(lab: Lab) -> None:
    expected = {
        "schema_version": 1, "account": ACCOUNT, "region": "us-east-2",
        "run_id": "a1b2c3d4", "deadline": 1060, "phase": "prepared",
        "baseline": [], "created": [], "pending_service": None, "operation_state": "confirmed",
    }
    assert Journal(lab.spec).content() == json.dumps(expected).encode()
    ports = lab.ports()
    assert asdict(ports.policy.limits) == {
        "calls": 80, "pages": 8, "attempts": 2, "cleanup_seconds": 30, "cleanup_reserve": 40,
    }
    outcome = lifecycle.run(lab.spec, ports)
    assert outcome.status == "succeeded"
    assert [e[1] for e in lab.events if e[0] == "create"] == ["cognito", "dynamodb", "s3", "iam"]
    assert json.loads(lab.journal().read_bytes())["schema_version"] == 1
    command, parsed, _ = lifecycle.parse(argv(lab))
    assert command == "run"
    assert parsed.probe == "all"


def test_probe_explicito_atravessa_cli_e_supervisor(lab: Lab) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions")
    args = argv(lab) + ["--probe", "ddb-transactions"]
    outcome = lifecycle.main(args, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert outcome.status == "succeeded"
    starts = [e for e in lab.events if e[0] == "start"]
    assert len(starts) == 1
    started_argv = starts[0][1]
    assert isinstance(started_argv, tuple)
    monitor_args = list(started_argv)[3:]
    assert monitor_args[0] == "monitor"
    _, monitor_spec, _ = lifecycle.parse(monitor_args)
    assert monitor_spec.probe == "ddb-transactions"
    assert monitor_spec == lab.spec


def test_supervisor_de_run_por_perfil_recebe_probe_duravel(lab: Lab) -> None:
    lab.spec = replace(lab.spec, probe="iam-trust")
    outcome = lifecycle.run(lab.spec, profile_ports(lab, Limits(512, 8, 2, 120, 256)))
    assert outcome.status == "succeeded"
    starts = [e for e in lab.events if e[0] == "start"]
    assert len(starts) == 1
    started_argv = starts[0][1]
    assert isinstance(started_argv, tuple)
    _, parsed, _ = lifecycle.parse(list(started_argv)[3:])
    assert parsed.probe == "iam-trust"


def test_janitor_recupera_schema2_com_perfil_e_limites_originais(lab: Lab) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions")
    resource = Resource("dynamodb", "profile-table", NAME)
    lab.remote.add(resource)
    schema2(lab, resource)
    lab.time = 1061
    outcome = lifecycle.janitor(lab.spec, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert outcome.status == "succeeded"
    assert lab.remote.resources == {}
    data = json.loads(lab.journal().read_bytes())
    assert data["schema_version"] == 2
    assert data["probe"] == "ddb-transactions"
    assert data["limits"] == {
        "calls": 192, "pages": 8, "attempts": 2, "cleanup_seconds": 150, "cleanup_reserve": 96,
    }
    assert data["created"] == []
    assert not any(e[0] in {"create", "probe"} for e in lab.events)
    assert {e[1] for e in lab.events if e[0] == "inventory"} == {
        "cognito", "dynamodb", "s3", "iam"
    }


@pytest.mark.parametrize("profile", ["ddb-conditions", "all"])
def test_recuperacao_nao_aceita_perfil_trocado(lab: Lab, profile: str) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions")
    resource = Resource("dynamodb", "profile-table", NAME)
    lab.remote.add(resource)
    schema2(lab, resource)
    before = lab.journal().read_bytes()
    changed = replace(lab.spec, probe=profile)
    outcome = lifecycle.janitor(changed, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert outcome.status == "refused"
    assert_no_mutation(lab)
    assert lab.journal().read_bytes() == before
    assert "profile-table" in lab.remote.resources
    valid = lifecycle.janitor(lab.spec, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert valid.status == "succeeded"


def test_schema1_recusa_perfil_real_antes_de_mutacao(lab: Lab) -> None:
    resource = Resource("dynamodb", "schema1-table", NAME)
    lab.remote.add(resource)
    prepare_directory(lab.spec.journal_dir)
    Journal(lab.spec, phase="cleanup_requested", created=[resource]).save()
    before = lab.journal().read_bytes()
    changed = replace(lab.spec, probe="ddb-transactions")
    outcome = lifecycle.janitor(changed, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert outcome.status == "refused"
    assert_no_mutation(lab)
    assert lab.journal().read_bytes() == before
    control = lifecycle.janitor(lab.spec, lab.ports())
    assert control.status == "succeeded"


@pytest.mark.parametrize(("field", "value"), [
    ("calls", 193), ("pages", 9), ("attempts", 3),
    ("cleanup_seconds", 151), ("cleanup_reserve", 95),
])
def test_snapshot_de_limites_adulterado_recusa_sem_apagar(
    lab: Lab, field: str, value: int
) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions")
    resource = Resource("dynamodb", "tampered-table", NAME)
    lab.remote.add(resource)
    original = schema2(lab, resource)
    changed = json.loads(json.dumps(original))
    changed["limits"][field] = value
    lab.journal().write_text(json.dumps(changed))
    before = lab.journal().read_bytes()
    outcome = lifecycle.janitor(lab.spec, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert outcome.status == "refused"
    assert_no_mutation(lab)
    assert lab.journal().read_bytes() == before
    lab.journal().write_text(json.dumps(original))
    control = lifecycle.janitor(lab.spec, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert control.status == "succeeded"


def test_versao_de_perfil_desconhecida_nao_e_recuperada(lab: Lab) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions")
    resource = Resource("dynamodb", "future-version", NAME)
    lab.remote.add(resource)
    original = schema2(lab, resource)
    changed = dict(original, profile_version=2)
    lab.journal().write_text(json.dumps(changed))
    outcome = lifecycle.janitor(lab.spec, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert outcome.status == "refused"
    assert_no_mutation(lab)
    assert json.loads(lab.journal().read_bytes()) == changed
    lab.journal().write_text(json.dumps(original))
    control = lifecycle.janitor(lab.spec, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert control.status == "succeeded"


@pytest.mark.parametrize("profile", ["ddb-other", "cnesdata-cap-a1b2c3d4", "", "ALL"])
def test_perfil_fora_da_allowlist_recusa_antes_de_create(lab: Lab, profile: str) -> None:
    lab.spec = replace(lab.spec, probe=profile)
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert_no_mutation(lab)
    assert not lab.journal().exists()


@pytest.mark.parametrize("source", ["policy", "budget", "both"])
def test_limites_do_perfil_nao_podem_divergir_de_policy_ou_budget(lab: Lab, source: str) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions")
    ports = profile_ports(lab, Limits(192, 8, 2, 150, 96))
    if source == "policy":
        ports = replace(ports, policy=replace(ports.policy, limits=Limits(256, 8, 2, 60, 128)))
    elif source == "both":
        ports = profile_ports(lab, Limits(256, 8, 2, 60, 128))
    else:
        ports = replace(ports, budget=RequestBudget(Limits(), lab.spec.deadline, lambda: lab.time))
    outcome = lifecycle.run(lab.spec, ports)
    assert outcome.status == "refused"
    assert_no_mutation(lab)
    assert not lab.journal().exists()


@pytest.mark.parametrize(("field", "value"), [
    ("calls", 513), ("pages", 9), ("attempts", 3),
    ("cleanup_seconds", 151), ("cleanup_reserve", 1),
])
def test_policy_e_budget_concordantes_nao_substituem_registry(
    lab: Lab, field: str, value: int
) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions")
    limits = replace(Limits(192, 8, 2, 150, 96), **{field: value})
    outcome = lifecycle.run(lab.spec, profile_ports(lab, limits))
    assert outcome.status == "refused"
    assert_no_mutation(lab)
    assert not lab.journal().exists()


def test_schema1_nao_pode_esconder_probe_real_no_json(lab: Lab) -> None:
    resource = Resource("dynamodb", "hidden-profile", NAME)
    lab.remote.add(resource)
    prepare_directory(lab.spec.journal_dir)
    journal = Journal(lab.spec, phase="cleanup_requested", created=[resource])
    original = json.loads(journal.content())
    changed = dict(original, probe="ddb-transactions")
    lab.journal().write_text(json.dumps(changed))
    lab.journal().chmod(0o600)
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert_no_mutation(lab)
    assert json.loads(lab.journal().read_bytes()) == changed
    lab.journal().write_text(json.dumps(original))
    assert lifecycle.janitor(lab.spec, lab.ports()).status == "succeeded"


def test_snapshot_policy_e_budget_adulterados_juntos_nao_burlam_registry(lab: Lab) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions")
    resource = Resource("dynamodb", "coherent-tampering", NAME)
    lab.remote.add(resource)
    original = schema2(lab, resource)
    changed = json.loads(json.dumps(original))
    changed["limits"]["calls"] = 256
    changed["limits"]["cleanup_reserve"] = 128
    lab.journal().write_text(json.dumps(changed))
    outcome = lifecycle.janitor(lab.spec, profile_ports(lab, Limits(256, 8, 2, 60, 128)))
    assert outcome.status == "refused"
    assert_no_mutation(lab)
    assert json.loads(lab.journal().read_bytes()) == changed
    lab.journal().write_text(json.dumps(original))
    control = lifecycle.janitor(lab.spec, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert control.status == "succeeded"


@pytest.mark.parametrize("seconds", [181, float("inf"), float("nan")])
def test_perfil_recusa_deadline_de_trabalho_fora_do_teto(lab: Lab, seconds: float) -> None:
    lab.spec = replace(lab.spec, probe="ddb-transactions", deadline=1000 + seconds)
    outcome = lifecycle.run(lab.spec, profile_ports(lab, Limits(192, 8, 2, 150, 96)))
    assert outcome.status == "refused"
    assert_no_mutation(lab)


@pytest.mark.parametrize("case", PROFILES, ids=[row[0] for row in PROFILES])
def test_reserva_inteira_do_perfil_sobrevive_ao_esgotamento_de_trabalho(
    lab: Lab, case: ProfileCase
) -> None:
    profile, calls, seconds, reserve, _ = case
    lab.spec = replace(lab.spec, probe=profile)
    budget = profile_ports(lab, Limits(calls, 8, 2, seconds, reserve)).budget
    for _ in range(calls - reserve):
        budget.acquire("work")
    with pytest.raises(BudgetExhausted):
        budget.acquire("work")
    for _ in range(reserve):
        budget.acquire("cleanup")
    with pytest.raises(BudgetExhausted):
        budget.acquire("cleanup")
    assert budget.used == calls
    assert budget.work_used == calls - reserve
    assert budget.cleanup_deadline == 1000 + seconds
