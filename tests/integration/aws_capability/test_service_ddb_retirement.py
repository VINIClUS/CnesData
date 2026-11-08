"""Ausência eventual DynamoDB não autoriza aposentar um root journalizado."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_read_testing import (
    ReadLab,
    make_read_lab,
    respond,
    respond_error,
)
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability.contracts import (
    Ownership,
    Policy,
    Ports,
    RequestContext,
    Resource,
)

ROOT = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")
TABLE_ARN = "arn:aws:dynamodb:us-east-2:111111111111:table/cnesdata-cap-a1b2c3d4"
DESCRIBE = ("dynamodb", "describe-table", "us-east-2", {"TableName": "cnesdata-cap-a1b2c3d4"})
TAGS = ("dynamodb", "list-tags-of-resource", "us-east-2", {"ResourceArn": TABLE_ARN})
OWNER = Ownership(
    "111111111111",
    {
        "Project": "CnesData",
        "Purpose": "aws-capability",
        "Run": "a1b2c3d4",
        "ExpiresAt": "1060",
    },
    "us-east-2",
)


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    private = tmp_path / "private"
    prepare_directory(private)
    lab = make_read_lab(monkeypatch, private)
    return replace(lab, context=RequestContext(lab.context.budget, "cleanup"))


def _cleanup(lab: ReadLab, root: Resource = ROOT) -> Cleanup:
    journal = Journal(lab.services.spec, phase="cleanup_requested", created=[root])
    journal.save()
    assert journal.path.stat().st_mode & 0o777 == 0o600
    assert lab.directory.stat().st_mode & 0o777 == 0o700
    budget = lab.context.budget
    ports = Ports(
        lab.services,
        ProcessBoundary([]),
        budget.now,
        Policy("111111111111", frozenset({"us-east-2"})),
        budget,
    )
    return Cleanup(journal, ports)


def _assert_disk_root(journal: Journal) -> None:
    data = json.loads(journal.path.read_text())
    assert data == {
        "schema_version": 1,
        "account": "111111111111",
        "region": "us-east-2",
        "run_id": "a1b2c3d4",
        "deadline": 1060,
        "phase": "cleanup_requested",
        "baseline": [],
        "created": [
            {
                "service": "dynamodb",
                "resource_id": "cnesdata-cap-a1b2c3d4",
                "name": "cnesdata-cap-a1b2c3d4",
            }
        ],
        "pending_service": None,
        "operation_state": "confirmed",
    }


def _assert_retained(cleanup: Cleanup, reason: str | None, before: bytes) -> None:
    cleanup.retire_confirmed([])
    disk = json.loads(cleanup.journal.path.read_text())
    assert {
        "reason": reason,
        "absent": cleanup.absent,
        "attempted": cleanup.attempted,
        "created": cleanup.journal.created,
        "phase": cleanup.journal.phase,
        "operation_state": cleanup.journal.operation_state,
        "pending_service": cleanup.journal.pending_service,
        "journal_bytes": cleanup.journal.path.read_bytes(),
        "durable_created": disk["created"],
        "durable_phase": disk["phase"],
    } == {
        "reason": "creation_uncertain",
        "absent": set(),
        "attempted": set(),
        "created": [ROOT],
        "phase": "cleanup_requested",
        "operation_state": "confirmed",
        "pending_service": None,
        "journal_bytes": before,
        "durable_created": [
            {
                "service": "dynamodb",
                "resource_id": "cnesdata-cap-a1b2c3d4",
                "name": "cnesdata-cap-a1b2c3d4",
            }
        ],
        "durable_phase": "cleanup_requested",
    }
    _assert_disk_root(cleanup.journal)


@pytest.mark.parametrize("restart", [False, True], ids=["memoria", "journal_recarregado"])
def test_rnf_nao_aposenta_root_confirmado_sem_delete_causal(
    read_lab: ReadLab, restart: bool
) -> None:
    cleanup = _cleanup(read_lab)
    before = cleanup.journal.path.read_bytes()
    if restart:
        loaded = Journal(cleanup.journal.spec)
        loaded.load()
        assert loaded is not cleanup.journal
        cleanup = Cleanup(loaded, cleanup.ports)
    _assert_disk_root(cleanup.journal)
    assert cleanup.journal.created == [ROOT]
    respond_error(read_lab, "remote_not_found")
    reason = cleanup.remove_omitted([])
    assert_process(read_lab, [DESCRIBE])
    assert read_lab.context.budget.work_used == 0
    _assert_retained(cleanup, reason, before)


def test_rnf_repetido_no_mesmo_cleanup_nao_cria_prova_de_ausencia(read_lab: ReadLab) -> None:
    cleanup = _cleanup(read_lab)
    before = cleanup.journal.path.read_bytes()
    _assert_disk_root(cleanup.journal)
    respond_error(read_lab, "remote_not_found")
    first = cleanup.remove_omitted([])
    respond_error(read_lab, "remote_not_found")
    second = cleanup.remove_omitted([])
    assert_process(read_lab, [DESCRIBE, DESCRIBE])
    assert read_lab.context.budget.work_used == 0
    _assert_retained(cleanup, second, before)
    assert first == "creation_uncertain"


def test_active_anterior_com_tags_frescas_nao_autoriza_aposentadoria_apos_rnf(
    read_lab: ReadLab,
) -> None:
    cleanup = _cleanup(read_lab)
    before = cleanup.journal.path.read_bytes()
    _assert_disk_root(cleanup.journal)
    respond(
        read_lab,
        {
            "Table": {
                "TableName": "cnesdata-cap-a1b2c3d4",
                "TableArn": TABLE_ARN,
                "TableStatus": "ACTIVE",
            }
        },
    )
    respond(
        read_lab,
        {
            "Tags": [
                {"Key": "Project", "Value": "CnesData"},
                {"Key": "Purpose", "Value": "aws-capability"},
                {"Key": "Run", "Value": "a1b2c3d4"},
                {"Key": "ExpiresAt", "Value": "1060"},
            ]
        },
    )
    assert read_lab.services.lookup(ROOT, cleanup.context) == OWNER
    assert_process(read_lab, [DESCRIBE, TAGS])
    respond_error(read_lab, "remote_not_found")
    reason = cleanup.remove_omitted([])
    assert_process(read_lab, [DESCRIBE, TAGS, DESCRIBE])
    assert read_lab.context.budget.work_used == 0
    _assert_retained(cleanup, reason, before)


def test_cognito_com_ausencia_modelada_preserva_aposentadoria_existente(read_lab: ReadLab) -> None:
    root = Resource("cognito", "us-east-2_a1b2c3d4", "cnesdata-cap-a1b2c3d4")
    cleanup = _cleanup(read_lab, root)
    before = cleanup.journal.path.read_bytes()
    assert json.loads(before)["created"] == [
        {
            "service": "cognito",
            "resource_id": "us-east-2_a1b2c3d4",
            "name": "cnesdata-cap-a1b2c3d4",
        }
    ]
    respond_error(read_lab, "remote_not_found")
    reason = cleanup.remove_omitted([])
    assert_process(
        read_lab,
        [("cognito-idp", "describe-user-pool", "us-east-2", {"UserPoolId": "us-east-2_a1b2c3d4"})],
    )
    assert reason is None
    assert cleanup.absent == {root}
    assert cleanup.attempted == set()
    assert cleanup.journal.path.read_bytes() == before
    cleanup.retire_confirmed([])
    assert cleanup.journal.created == []
    after = json.loads(cleanup.journal.path.read_text())
    assert after == {**json.loads(before), "created": []}
    assert read_lab.context.budget.work_used == 0


def test_root_dynamodb_visivel_e_ignorado_sem_io_ou_incerteza(read_lab: ReadLab) -> None:
    cleanup = _cleanup(read_lab)
    before = cleanup.journal.path.read_bytes()
    _assert_disk_root(cleanup.journal)
    reason = cleanup.remove_omitted([(ROOT, OWNER)])
    assert_process(read_lab, [])
    assert reason is None
    assert cleanup.absent == set()
    assert cleanup.attempted == set()
    cleanup.retire_confirmed([])
    assert cleanup.journal.created == [ROOT]
    assert cleanup.journal.path.read_bytes() == before
    _assert_disk_root(cleanup.journal)
