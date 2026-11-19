"""Regressões dos três bloqueadores P1, sem alteração do core auditado."""

import fcntl
import json
from collections.abc import Mapping
from dataclasses import replace

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._oracle import assert_complete, assert_no_mutation
from tests.integration.aws_capability._testing import (
    NAME,
    SERVICES,
    Lab,
    ProcessBoundary,
    owned_tags,
    ref_json,
    seed_journal,
)
from tests.integration.aws_capability.contracts import (
    Outcome,
    Ownership,
    Page,
    Resource,
    ServiceFailure,
)


class ClosedWorkerChannel(ProcessBoundary):
    def send(self, pid: int, message: Mapping[str, object]) -> None:
        self.events.append(("send_closed_worker", pid))
        raise BrokenPipeError("ipc=worker_exited")


def assert_reconciled(lab: Lab, outcome: Outcome, resource: Resource) -> None:
    assert resource.resource_id not in lab.remote.resources
    assert ("delete", resource.resource_id, True) in lab.events
    assert outcome.status in {"succeeded", "incomplete"}
    if outcome.status == "succeeded":
        assert_complete(outcome, lab.events)
    else:
        assert json.loads(lab.journal().read_text())["phase"] != "complete"


def pending_journal(lab: Lab, created: list[Resource], baseline: list[Resource]) -> None:
    seed_journal(lab, created)
    data = json.loads(lab.journal().read_text())
    data.update(operation_state="unknown", pending_service="iam")
    data["baseline"] = [ref_json(resource) for resource in baseline]
    lab.journal().write_text(json.dumps(data))


@pytest.mark.parametrize("service", SERVICES)
@pytest.mark.parametrize("empty_scans", [3, 20])
def test_recurso_confirmado_omitido_nao_e_abandonado(
    lab: Lab, service: str, empty_scans: int
) -> None:
    lab.remote.pages[service] = [Page()] * empty_scans
    outcome = lifecycle.run(lab.spec, lab.ports())
    resource = Resource(service, f"synthetic-{service}", NAME)
    assert_reconciled(lab, outcome, resource)
    assert lab.remote.resources == {}
    assert lab.remote.calls <= 80


def test_janitor_retoma_id_confirmado_omitido_do_inventario(lab: Lab) -> None:
    resource = Resource("cognito", "confirmed-before-crash", NAME)
    lab.remote.add(resource)
    lab.remote.pages["cognito"] = [Page()] * 20
    seed_journal(lab, [resource])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert_reconciled(lab, outcome, resource)
    assert not any(event[0] in {"create", "probe"} for event in lab.events)


@pytest.mark.parametrize("protected", ["foreign", "baseline"])
def test_id_confirmado_omitido_nao_contorna_ownership_ou_baseline(lab: Lab, protected: str) -> None:
    resource = Resource("s3", "protected", NAME)
    owner = Ownership("222222222222", owned_tags(), "us-east-2") if protected == "foreign" else None
    lab.remote.add(resource, owner)
    seed_journal(lab, [resource])
    if protected == "baseline":
        data = json.loads(lab.journal().read_text())
        data["baseline"] = [ref_json(resource)]
        lab.journal().write_text(json.dumps(data))
    lab.remote.pages["s3"] = [Page()] * 20
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert lab.remote.resources == {"protected": resource}
    assert_no_mutation(lab.events)
    assert json.loads(lab.journal().read_text())["phase"] != "complete"


@pytest.mark.parametrize("closed", [True, False])
def test_monitor_recupera_worker_morto_independente_do_canal(lab: Lab, closed: bool) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    ports = lab.ports()
    boundary_type = ClosedWorkerChannel if closed else ProcessBoundary
    boundary = boundary_type(lab.events, workers={4321: False})
    outcome = lifecycle.monitor(lab.spec, replace(ports, processes=boundary), 4321)
    assert_complete(outcome, lab.events)
    assert lab.remote.resources == {}
    assert ("identity",) in lab.events
    assert ("delete", "pool", True) in lab.events


def test_monitor_com_worker_morto_e_canal_fechado_respeita_lease(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    ports = lab.ports()
    boundary = ClosedWorkerChannel(lab.events, workers={4321: False})
    lock = lab.spec.journal_dir / "a1b2c3d4.lock"
    with lock.open("a+") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        outcome = lifecycle.monitor(lab.spec, replace(ports, processes=boundary), 4321)
        assert outcome.status == "waiting"
        assert outcome.reason == "cleanup_owned"
        assert_no_mutation(lab.events)
    assert lab.remote.resources == {"pool": resource}


def test_controle_worker_vivo_com_ack_falho_nao_autoriza_cleanup(lab: Lab) -> None:
    resource = Resource("cognito", "pool", NAME)
    lab.remote.add(resource)
    seed_journal(lab, [resource])
    ports = lab.ports()
    boundary = ClosedWorkerChannel(lab.events, workers={4321: True})
    outcome = lifecycle.monitor(lab.spec, replace(ports, processes=boundary), 4321)
    assert outcome.status == "refused"
    assert outcome.reason == "supervisor_unavailable"
    assert_no_mutation(lab.events)
    assert lab.remote.resources == {"pool": resource}
    assert ("identity",) not in lab.events


def test_controle_run_sem_ack_nao_cria_mesmo_com_supervisor_vivo(lab: Lab) -> None:
    lab.processes.ack = None
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "refused"
    assert outcome.reason == "supervisor_not_ready"
    assert_no_mutation(lab.events)


@pytest.mark.parametrize("pending", ["dynamodb", "s3", "iam"])
def test_intencao_incerta_posterior_retem_ddb_e_limpa_demais_confirmados(
    lab: Lab, pending: str
) -> None:
    lab.remote.fail_service = pending
    lab.remote.create_error = ServiceFailure("create=response_lost")
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.reason == "creation_uncertain"
    assert outcome.remaining is None
    data = json.loads(lab.journal().read_text())
    assert data["operation_state"] == "unknown"
    assert data["pending_service"] == pending
    assert data["phase"] != "complete"
    if pending == "dynamodb":
        retained = {}
        assert not any(ref["service"] == "dynamodb" for ref in data["created"])
    else:
        retained = {
            "synthetic-dynamodb": Resource(
                "dynamodb", "synthetic-dynamodb", "cnesdata-cap-a1b2c3d4"
            )
        }
        known = json.loads(
            '{"service":"dynamodb","resource_id":"synthetic-dynamodb",'
            '"name":"cnesdata-cap-a1b2c3d4"}'
        )
        assert known in data["created"]
    assert lab.remote.resources == retained
    assert "dynamodb_delete_accepted" not in data
    expected = {
        "dynamodb": {"synthetic-cognito"},
        "s3": {"synthetic-cognito"},
        "iam": {"synthetic-cognito", "synthetic-s3"},
    }[pending]
    assert {event[1] for event in lab.events if event[0] == "delete"} == expected
    assert lab.remote.calls <= 80


def test_intencao_incerta_limpa_so_conhecidos_sem_tocar_foreign_ou_baseline(lab: Lab) -> None:
    known = Resource("cognito", "known", NAME)
    baseline = Resource("s3", "baseline", NAME)
    foreign = Resource("dynamodb", "foreign", NAME)
    lab.remote.add(known)
    lab.remote.add(baseline)
    lab.remote.add(foreign, Ownership("222222222222", owned_tags(), "us-east-2"))
    pending_journal(lab, [known, baseline, foreign], [baseline])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    data = json.loads(lab.journal().read_text())
    assert data["operation_state"] == "unknown"
    assert data["pending_service"] == "iam"
    assert lab.remote.resources == {"baseline": baseline, "foreign": foreign}
    assert [event[1] for event in lab.events if event[0] == "delete"] == ["known"]
    assert lab.remote.calls <= 80


def test_controle_janitor_conclui_quando_recurso_incerto_aparece_depois(lab: Lab) -> None:
    known = Resource("cognito", "known", NAME)
    late = Resource("iam", "late", NAME)
    lab.remote.add(known)
    pending_journal(lab, [known], [])
    first = lifecycle.janitor(lab.spec, lab.ports())
    assert first.status == "incomplete"
    data = json.loads(lab.journal().read_text())
    assert data["operation_state"] == "unknown"
    assert data["pending_service"] == "iam"
    lab.remote.add(late)
    recovered = lifecycle.janitor(lab.spec, lab.ports())
    assert_complete(recovered, lab.events)
    assert lab.remote.resources == {}
    assert ("delete", "late", True) in lab.events
    data = json.loads(lab.journal().read_text())
    assert data["phase"] == "complete"
    assert data["pending_service"] is None


def test_controle_run_visivel_nao_pode_retornar_sempre_incomplete(lab: Lab) -> None:
    outcome = lifecycle.run(lab.spec, lab.ports())
    assert_complete(outcome, lab.events)
    assert lab.remote.resources == {}
    assert len([event for event in lab.events if event[0] == "delete"]) == 4


def test_controle_recurso_estrangeiro_visivel_nao_pode_ser_apagado(lab: Lab) -> None:
    foreign = Resource("s3", "foreign", NAME)
    lab.remote.add(foreign, Ownership("222222222222", owned_tags(), "us-east-2"))
    seed_journal(lab, [foreign])
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.reason == "ownership_mismatch"
    assert lab.remote.resources == {"foreign": foreign}
    assert_no_mutation(lab.events)
