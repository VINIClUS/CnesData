"""Identidades fisicas distintas sobrevivem a varredura S3 real sem dedupe excessivo."""

from pathlib import Path

from tests.integration.aws_capability._inventory import dependents
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import (
    ReadLab,
    load_fixture,
    respond,
)
from tests.integration.aws_capability.contracts import Limits, Resource
from tests.integration.aws_capability.test_service_s3_children_lookup import (
    assert_failure,
    enqueue,
    read_lab,
    resource,
    trace,
)
from tests.integration.aws_capability.test_service_s3_dependents import cleanup

__all__ = ["read_lab"]

DATA = load_fixture(Path(__file__).parent / "fixtures/cli/children/s3_identity.json")


def test_varredura_preserva_pares_distintos_e_namespace_upload_em_seis_calls(
    read_lab: ReadLab,
) -> None:
    enqueue(read_lab, "location", "tags")
    respond(read_lab, DATA["replies"]["versions"])
    enqueue(read_lab, "location", "tags")
    respond(read_lab, DATA["replies"]["uploads"])
    janitor = cleanup(read_lab)
    found = dependents(resource("bucket"), janitor.ports, read_lab.context)
    expected = {Resource(**row) for row in DATA["expected"]["resources"]}
    assert set(found) == expected
    assert len(found) == len(expected) == DATA["expected"]["resources_count"] == 4
    assert_process(read_lab, trace(*DATA["expected"]["trace"]))
    assert read_lab.context.budget.used == DATA["expected"]["calls"] == 6
    assert read_lab.context.budget.work_used == DATA["expected"]["work_calls"] == 6
    assert read_lab.context.budget.limits == Limits()
    assert janitor.attempted == janitor.absent == set()
    assert janitor.journal.created == janitor.journal.baseline == []
    assert list(read_lab.directory.iterdir()) == []
    assert_failure(None, read_lab)
