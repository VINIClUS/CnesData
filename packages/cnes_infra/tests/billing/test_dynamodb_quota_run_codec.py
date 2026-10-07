"""Prova de equivalência do Run criado na reserva com o codec do control plane."""

from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.billing.errors import PermanentBillingError
from cnes_domain.control_plane.entities import Run, RunDependency
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.queries import RawIdentity, WaitingRunsForDependencyQuery
from cnes_infra.billing.dynamodb_quota import RUN_FIXED_ACTIONS
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_run_codec import run_dependency_actions
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, create_table
from packages.cnes_infra.tests.billing.quota_support import (
    TENANT,
    make_reserve_command,
    quota_env,
    table_items,
)

MARKER_LIMIT = 100 - RUN_FIXED_ACTIONS


def _dependencies(count: int) -> tuple[RunDependency, ...]:
    return tuple(
        RunDependency(source_type="CNES", file_subtype=f"SUB{index:03d}", required=index % 2 == 0)
        for index in range(count)
    )


def _expected_run(dependencies: tuple[RunDependency, ...]) -> Run:
    return Run(
        tenant_id=TENANT,
        run_id="run-01",
        competencia="2026-08",
        dataset_name="cnes_vinculos",
        state=RunState.WAITING_INPUTS,
        dependencies=dependencies,
        missing_sources=tuple(
            sorted(f"{d.source_type}/{d.file_subtype}" for d in dependencies if d.required)
        ),
        created_at=NOW,
    )


def _by_key(items: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(item["pk"]["S"], item["sk"]["S"]): item for item in items}


def _control_plane_items(run: Run) -> dict[tuple[str, str], dict[str, Any]]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        DynamoDBControlPlane(client, TABLE_NAME, lambda: NOW).put_run(run)
        return _by_key(table_items(client))


def test_run_criado_na_reserva_equivale_ao_put_run_do_control_plane() -> None:
    dependencies = _dependencies(3)
    expected = _expected_run(dependencies)
    reference = _control_plane_items(expected)
    with quota_env() as env:
        command = make_reserve_command(dependencies=dependencies)
        env.repo.reserve_and_create_run(command)
        stored = _by_key(table_items(env.client))
        assert len(reference) == 1 + len(dependencies)
        for key, item in reference.items():
            assert stored[key] == item
        assert env.control_plane.get_run(TENANT, "run-01") == expected


def test_runs_aguardando_sao_encontrados_por_cada_dependencia() -> None:
    dependencies = _dependencies(3)
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command(dependencies=dependencies))
        for dependency in dependencies:
            identity = RawIdentity(
                TENANT, dependency.source_type, dependency.file_subtype, "2026-08"
            )
            found = env.control_plane.query_waiting_runs_for_dependency(
                WaitingRunsForDependencyQuery(identity)
            )
            assert [run.run_id for run in found] == ["run-01"]


def test_falha_no_segundo_marcador_desfaz_toda_a_transacao() -> None:
    dependencies = _dependencies(3)
    with quota_env() as env:
        command = make_reserve_command(dependencies=dependencies)
        run = _expected_run(dependencies)
        second = run_dependency_actions(TABLE_NAME, run, RUN_FIXED_ACTIONS)[1]["Put"]["Item"]
        env.client.put_item(TableName=TABLE_NAME, Item=second)
        before = table_items(env.client)
        with pytest.raises(PermanentBillingError, match="quota_reservation_conflict"):
            env.repo.reserve_and_create_run(command)
        remaining = table_items(env.client)
        assert len(remaining) == len(before) == 2
        assert {item["entity"]["S"] for item in remaining} >= {second["entity"]["S"]}
        assert sorted(_by_key(remaining)) == sorted(_by_key(before))


def test_mais_dependencias_que_o_limite_da_transacao_falha_sem_escrever() -> None:
    dependencies = _dependencies(MARKER_LIMIT + 1)
    with quota_env() as env:
        before = table_items(env.client)
        with pytest.raises(Conflict) as error:
            env.repo.reserve_and_create_run(make_reserve_command(dependencies=dependencies))
        assert error.value.code is ErrorCode.TRANSACTION_LIMIT
        assert table_items(env.client) == before


def test_dependencias_no_limite_da_transacao_sao_aceitas() -> None:
    dependencies = _dependencies(MARKER_LIMIT)
    with quota_env() as env:
        env.repo.reserve_and_create_run(make_reserve_command(dependencies=dependencies))
        assert env.control_plane.get_run(TENANT, "run-01") is not None
