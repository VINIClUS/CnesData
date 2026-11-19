"""Espera por ACTIVE da tabela e dos GSIs antes de qualquer escrita de probe."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._aws_fake_types_testing import denied, good
from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._probe_testing import ProbeLab, make_probe_lab
from tests.integration.aws_capability._service_contracts import Reply, ServicePorts
from tests.integration.aws_capability._table_ready import (
    READY_POLLS,
    READY_SECONDS,
    await_active_table,
)
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure


class Script:
    def __init__(self, replies: list[Reply]) -> None:
        self.replies = replies
        self.requests: list[Request] = []

    def call(self, request: Request, context: RequestContext) -> Reply:
        self.requests.append(request)
        return self.replies[min(len(self.requests), len(self.replies)) - 1]


def table(status: str, *indexes: str) -> Reply:
    rows = [{"IndexName": f"gsi{n}", "IndexStatus": s} for n, s in enumerate(indexes, 1)]
    return good({"Table": {"TableStatus": status, "GlobalSecondaryIndexes": rows}})


def ports_of(lab: ProbeLab, script: Script) -> ServicePorts:
    return ServicePorts(script, lab.services.ports.now, lab.services.ports.pause)


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    return make_probe_lab(tmp_path, "iam-leadingkeys")


def test_devolve_a_tabela_quando_ela_e_todos_os_gsis_estao_active(lab: ProbeLab) -> None:
    script = Script([table("ACTIVE", "ACTIVE", "ACTIVE")])
    found = await_active_table(ports_of(lab, script), "t", lab.context)
    assert found["TableStatus"] == "ACTIVE"
    assert [r.payload for r in script.requests] == [{"TableName": "t"}]
    assert lab.pauses == []


@pytest.mark.parametrize(
    "first",
    [
        table("CREATING"),
        table("ACTIVE", "ACTIVE", "CREATING"),
        denied("ResourceNotFoundException"),
    ],
)
def test_espera_ate_a_tabela_e_os_indices_ficarem_active(lab: ProbeLab, first: Reply) -> None:
    script = Script([first, first, table("ACTIVE", "ACTIVE", "ACTIVE")])
    await_active_table(ports_of(lab, script), "t", lab.context)
    assert len(script.requests) == 3
    assert lab.pauses == [READY_SECONDS, READY_SECONDS]


def test_nunca_ativa_falha_depois_de_todas_as_leituras(lab: ProbeLab) -> None:
    script = Script([table("CREATING")])
    with pytest.raises(ServiceFailure) as error:
        await_active_table(ports_of(lab, script), "t", lab.context)
    assert error.value.args == ("probe=table_not_ready",)
    assert len(script.requests) == READY_POLLS
    assert len(lab.pauses) == READY_POLLS - 1


def test_erro_diferente_de_ausencia_nao_e_repetido(lab: ProbeLab) -> None:
    script = Script([denied("AccessDeniedException")])
    with pytest.raises(ServiceFailure) as error:
        await_active_table(ports_of(lab, script), "t", lab.context)
    assert error.value.args == ("service=request_failed",)
    assert len(script.requests) == 1
    assert lab.pauses == []


def test_fake_descreve_gsis_creating_enquanto_a_tabela_nao_ativou(lab: ProbeLab) -> None:
    lab.aws.tables[NAME].status = "CREATING"
    lab.aws.tables[NAME].ready_in = 1
    request = Request("dynamodb", "describe-table", {"TableName": NAME})
    first = lab.aws.call(request, lab.context).data["Table"]
    assert first["TableStatus"] == "CREATING"  # type: ignore[index]
    assert {row["IndexStatus"] for row in first["GlobalSecondaryIndexes"]} == {"CREATING"}  # type: ignore[index]
    second = lab.aws.call(request, lab.context).data["Table"]
    assert {row["IndexStatus"] for row in second["GlobalSecondaryIndexes"]} == {"ACTIVE"}  # type: ignore[index]
    assert "OnDemandThroughput" not in first  # type: ignore[operator]
