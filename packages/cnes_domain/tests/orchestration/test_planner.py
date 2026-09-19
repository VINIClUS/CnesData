import ast
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cnes_domain.control_plane.entities import Run, RunDependency, RunDispatch, RunUnit
from cnes_domain.control_plane.enums import (
    DispatchOutcome,
    DispatchState,
    RunStage,
    RunState,
    RunUnitState,
)
from cnes_domain.orchestration.planner import (
    PlanRequest,
    RawManifestRef,
    RunPlan,
    execution_request,
    logical_wave_id,
    plan_run,
    ready_units,
)

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)
TENANT = "354130"
RUN_ID = "run-1"
DEPENDENCIES = (
    RunDependency(source_type="SRC_A", file_subtype="SUB_1", required=True),
    RunDependency(source_type="SRC_B", file_subtype="SUB_2", required=False),
)


def _run(dependencies: tuple[RunDependency, ...], **updates: object) -> Run:
    values: dict[str, object] = {
        "tenant_id": TENANT,
        "run_id": RUN_ID,
        "competencia": "2026-07",
        "dataset_name": "generic-dataset",
        "state": RunState.PROCESSING,
        "dependencies": dependencies,
        "missing_sources": (),
        "created_at": NOW,
    }
    return Run.model_validate(values | updates)


def _standard_run(**updates: object) -> Run:
    return _run(DEPENDENCIES, **updates)


def _manifest_key(source_type: str, manifest_id: str) -> str:
    return f"raw/{TENANT}/{source_type}/2026-07/{manifest_id}/manifest.json"


def _raw_ref(
    manifest_id: str, source_type: str, file_subtype: str, *, partition: str = "all"
) -> RawManifestRef:
    return RawManifestRef(
        manifest_id=manifest_id,
        manifest_key=_manifest_key(source_type, manifest_id),
        source_type=source_type,
        file_subtype=file_subtype,
        partition=partition,
    )


def _standard_manifests() -> tuple[RawManifestRef, ...]:
    return (
        _raw_ref("manifest-a1", "SRC_A", "SUB_1"),
        _raw_ref("manifest-b1", "SRC_B", "SUB_2"),
    )


def _standard_plan(**run_updates: object) -> RunPlan:
    run = _standard_run(**run_updates)
    request = PlanRequest(run=run, manifests=_standard_manifests(), deployment_limit=4)
    return plan_run(request)


def _reserved_dispatch(
    plan: RunPlan, ready: tuple[RunUnit, ...], **updates: object
) -> RunDispatch:
    values: dict[str, object] = {
        "tenant_id": plan.run.tenant_id,
        "run_id": plan.run.run_id,
        "wave_id": logical_wave_id(ready),
        "dispatch_id": "fedcba9876543210",
        "generation": 1,
        "unit_ids": tuple(sorted(unit.unit_id for unit in ready)),
        "state": DispatchState.RESERVED,
        "lease_until": NOW,
    }
    return RunDispatch.model_validate(values | updates)


def test_planner_cria_uma_unit_por_source_subtype_partition() -> None:
    request = PlanRequest(run=_standard_run(), manifests=_standard_manifests(), deployment_limit=4)

    first, second = plan_run(request), plan_run(request)

    assert first.units == second.units
    assert len({unit.unit_id for unit in first.units}) == len(first.units)
    normalize = tuple(u for u in first.units if u.stage is RunStage.NORMALIZE)
    reconcile = next(u for u in first.units if u.stage is RunStage.RECONCILE)
    materialize = next(u for u in first.units if u.stage is RunStage.MATERIALIZE)
    assert len(normalize) == 2
    assert reconcile.depends_on_unit_ids == tuple(u.unit_id for u in normalize)
    assert materialize.depends_on_unit_ids == (reconcile.unit_id,)
    assert [u.source_type for u in normalize] == ["SRC_A", "SRC_B"]


@given(st.permutations([0, 1]))
def test_plan_run_e_invariante_a_ordem_dos_manifestos(order: list[int]) -> None:
    manifests = _standard_manifests()
    permuted = tuple(manifests[i] for i in order)
    permuted_request = PlanRequest(run=_standard_run(), manifests=permuted, deployment_limit=4)
    baseline_request = PlanRequest(
        run=_standard_run(), manifests=manifests, deployment_limit=4
    )

    assert plan_run(permuted_request).units == plan_run(baseline_request).units


def test_stage_altera_unit_id() -> None:
    plan = _standard_plan()
    reconcile = next(u for u in plan.units if u.stage is RunStage.RECONCILE)
    materialize = next(u for u in plan.units if u.stage is RunStage.MATERIALIZE)
    assert reconcile.unit_id != materialize.unit_id


def test_planner_rejeita_manifesto_nao_declarado_no_run() -> None:
    manifests = (*_standard_manifests(), _raw_ref("manifest-c1", "SRC_C", "SUB_3"))
    request = PlanRequest(run=_standard_run(), manifests=manifests, deployment_limit=4)

    with pytest.raises(ValueError, match="undeclared_dependency"):
        plan_run(request)


def test_planner_rejeita_manifest_id_duplicado() -> None:
    ref = _raw_ref("manifest-a1", "SRC_A", "SUB_1")
    duplicate_id = RawManifestRef(
        manifest_id="manifest-a1",
        manifest_key=_manifest_key("SRC_A", "manifest-a1-dup"),
        source_type="SRC_A",
        file_subtype="SUB_1",
        partition="all",
    )
    request = PlanRequest(
        run=_standard_run(), manifests=(ref, duplicate_id), deployment_limit=4
    )

    with pytest.raises(ValueError, match="duplicate_manifest_ref"):
        plan_run(request)


def test_planner_rejeita_manifest_key_duplicada() -> None:
    ref = _raw_ref("manifest-a1", "SRC_A", "SUB_1")
    duplicate_key = RawManifestRef(
        manifest_id="manifest-a2",
        manifest_key=ref.manifest_key,
        source_type="SRC_A",
        file_subtype="SUB_1",
        partition="all",
    )
    request = PlanRequest(
        run=_standard_run(), manifests=(ref, duplicate_key), deployment_limit=4
    )

    with pytest.raises(ValueError, match="duplicate_manifest_ref"):
        plan_run(request)


def test_planner_rejeita_cadeia_de_particao_mista() -> None:
    manifests = (
        _raw_ref("manifest-a1", "SRC_A", "SUB_1", partition="2026-07-01"),
        _raw_ref("manifest-a2", "SRC_A", "SUB_1", partition="2026-07-02"),
    )
    single_dependency = (RunDependency(source_type="SRC_A", file_subtype="SUB_1", required=True),)
    request = PlanRequest(run=_run(single_dependency), manifests=manifests, deployment_limit=4)

    with pytest.raises(ValueError, match="mixed_partition_chain"):
        plan_run(request)


def test_required_ausente_nao_cria_dag_executavel() -> None:
    request = PlanRequest(
        run=_standard_run(),
        manifests=(_raw_ref("manifest-b1", "SRC_B", "SUB_2"),),
        deployment_limit=4,
    )

    plan = plan_run(request)

    assert plan.units == ()
    assert plan.missing_required == ("SRC_A/SUB_1",)
    assert plan.missing_optional == ()


def test_opcional_ausente_fica_congelado_quando_required_fecha() -> None:
    plan = plan_run(
        PlanRequest(
            run=_standard_run(),
            manifests=(_raw_ref("manifest-a1", "SRC_A", "SUB_1"),),
            deployment_limit=4,
        )
    )

    assert plan.missing_required == ()
    assert plan.missing_optional == ("SRC_B/SUB_2",)
    assert len(plan.units) == 3


def test_planner_rejeita_run_sem_normalizacoes_possiveis() -> None:
    optional_dependency = (
        RunDependency(source_type="SRC_A", file_subtype="SUB_1", required=False),
    )
    request = PlanRequest(run=_run(optional_dependency), manifests=(), deployment_limit=4)

    with pytest.raises(ValueError, match="empty_normalize_set"):
        plan_run(request)


def test_planner_rejeita_mais_de_20_units() -> None:
    dependencies = tuple(
        RunDependency(source_type=f"SRC_{i}", file_subtype="SUB", required=True)
        for i in range(19)
    )
    manifests = tuple(_raw_ref(f"manifest-{i}", f"SRC_{i}", "SUB") for i in range(19))
    request = PlanRequest(run=_run(dependencies), manifests=manifests, deployment_limit=4)

    with pytest.raises(ValueError, match="unit_budget_exceeded"):
        plan_run(request)


def test_so_normalize_fica_ready_na_primeira_onda() -> None:
    plan = _standard_plan()

    ready = ready_units(plan, NOW)

    assert {u.stage for u in ready} == {RunStage.NORMALIZE}
    assert len(ready) == 2


def test_reconcile_espera_todas_as_normalizacoes_congeladas() -> None:
    plan = _standard_plan()
    one_succeeded = plan.units[0].model_copy(update={"state": RunUnitState.SUCCEEDED})
    partial_plan = replace(plan, units=(one_succeeded, *plan.units[1:]))

    ready = ready_units(partial_plan, NOW)
    assert {u.stage for u in ready} == {RunStage.NORMALIZE}

    both_succeeded = partial_plan.units[1].model_copy(update={"state": RunUnitState.SUCCEEDED})
    complete_plan = replace(
        partial_plan, units=(partial_plan.units[0], both_succeeded, *partial_plan.units[2:])
    )

    ready_after = ready_units(complete_plan, NOW)
    assert {u.stage for u in ready_after} == {RunStage.RECONCILE}


def test_materialize_espera_reconcile() -> None:
    plan = _standard_plan()
    succeeded_normalize = tuple(
        u.model_copy(update={"state": RunUnitState.SUCCEEDED})
        for u in plan.units
        if u.stage is RunStage.NORMALIZE
    )
    reconcile = next(u for u in plan.units if u.stage is RunStage.RECONCILE)
    materialize = next(u for u in plan.units if u.stage is RunStage.MATERIALIZE)
    pending_reconcile_plan = replace(plan, units=(*succeeded_normalize, reconcile, materialize))

    ready = ready_units(pending_reconcile_plan, NOW)
    assert {u.stage for u in ready} == {RunStage.RECONCILE}

    succeeded_reconcile = reconcile.model_copy(update={"state": RunUnitState.SUCCEEDED})
    ready_plan = replace(
        pending_reconcile_plan,
        units=(*succeeded_normalize, succeeded_reconcile, materialize),
    )

    ready_after = ready_units(ready_plan, NOW)
    assert {u.stage for u in ready_after} == {RunStage.MATERIALIZE}


def test_lease_ativa_bloqueia_nova_dispatch() -> None:
    plan = _standard_plan()
    leased_normalize = plan.units[0].model_copy(
        update={
            "state": RunUnitState.LEASED,
            "lease_owner": "worker",
            "lease_until": NOW + timedelta(minutes=5),
            "dispatch_id": "fedcba9876543210",
        }
    )
    leased_plan = replace(plan, units=(leased_normalize, *plan.units[1:]))

    assert ready_units(leased_plan, NOW) == ()


def test_lease_expirada_fica_pronta_para_novo_attempt() -> None:
    plan = _standard_plan()
    expired_normalize = plan.units[0].model_copy(
        update={
            "state": RunUnitState.LEASED,
            "lease_owner": "worker",
            "lease_until": NOW - timedelta(minutes=1),
            "dispatch_id": "fedcba9876543210",
            "attempt": 1,
        }
    )
    expired_plan = replace(plan, units=(expired_normalize, *plan.units[1:]))

    ready = ready_units(expired_plan, NOW)

    assert expired_normalize.unit_id in {u.unit_id for u in ready}


def test_lease_sem_lease_until_e_tratada_como_expirada() -> None:
    plan = _standard_plan()
    leased_no_until = plan.units[0].model_copy(
        update={"state": RunUnitState.LEASED, "lease_owner": None, "lease_until": None}
    )
    dangling_plan = replace(plan, units=(leased_no_until, *plan.units[1:]))

    ready = ready_units(dangling_plan, NOW)

    assert leased_no_until.unit_id in {u.unit_id for u in ready}


def test_failed_retryable_fica_pronta() -> None:
    plan = _standard_plan()
    retryable = plan.units[0].model_copy(update={"state": RunUnitState.FAILED_RETRYABLE})
    retry_plan = replace(plan, units=(retryable, *plan.units[1:]))

    ready = ready_units(retry_plan, NOW)

    assert retryable.unit_id in {u.unit_id for u in ready}


def test_logical_wave_id_e_hex16_estavel_por_conjunto() -> None:
    plan = _standard_plan()
    ready = ready_units(plan, NOW)
    reversed_ready = tuple(reversed(ready))

    wave = logical_wave_id(ready)

    assert len(wave) == 16
    assert all(char in "0123456789abcdef" for char in wave)
    assert logical_wave_id(reversed_ready) == wave


def test_logical_wave_id_rejeita_onda_vazia() -> None:
    with pytest.raises(ValueError, match="empty_wave"):
        logical_wave_id(())


def test_retry_mantem_wave_logica_e_request_usa_dispatch_reservado() -> None:
    plan = _standard_plan()
    first_ready = ready_units(plan, NOW)

    retried_normalize = plan.units[0].model_copy(
        update={"state": RunUnitState.FAILED_RETRYABLE, "attempt": 1}
    )
    retried_plan = replace(plan, units=(retried_normalize, *plan.units[1:]))
    retried_ready = ready_units(retried_plan, NOW)

    assert logical_wave_id(retried_ready) == logical_wave_id(first_ready)

    dispatch = _reserved_dispatch(plan, first_ready)

    request = execution_request(plan, dispatch, max_concurrency=4)

    assert request.dispatch_id == dispatch.dispatch_id
    assert request.unit_ids == dispatch.unit_ids
    assert request.max_concurrency == len(dispatch.unit_ids)


def test_execution_request_rejeita_dispatch_terminal() -> None:
    plan = _standard_plan()
    ready = ready_units(plan, NOW)
    dispatch = _reserved_dispatch(
        plan,
        ready,
        state=DispatchState.TERMINAL,
        terminal_outcome=DispatchOutcome.SUCCEEDED,
        execution_ref=None,
    )

    with pytest.raises(ValueError, match="invalid_dispatch_state"):
        execution_request(plan, dispatch, max_concurrency=4)


def test_execution_request_rejeita_tenant_divergente() -> None:
    plan = _standard_plan()
    ready = ready_units(plan, NOW)
    dispatch = _reserved_dispatch(plan, ready, tenant_id="other-tenant")

    with pytest.raises(ValueError, match="dispatch_identity_mismatch"):
        execution_request(plan, dispatch, max_concurrency=4)


def test_execution_request_rejeita_run_divergente() -> None:
    plan = _standard_plan()
    ready = ready_units(plan, NOW)
    dispatch = _reserved_dispatch(plan, ready, run_id="other-run")

    with pytest.raises(ValueError, match="dispatch_identity_mismatch"):
        execution_request(plan, dispatch, max_concurrency=4)


def test_execution_request_rejeita_unit_fora_do_plano() -> None:
    plan = _standard_plan()
    ready = ready_units(plan, NOW)
    unknown_unit_id = "0" * 32
    dispatch = _reserved_dispatch(
        plan,
        ready,
        unit_ids=tuple(sorted((*(u.unit_id for u in ready), unknown_unit_id))),
    )

    with pytest.raises(ValueError, match="dispatch_unit_missing"):
        execution_request(plan, dispatch, max_concurrency=4)


def test_execution_request_rejeita_wave_id_divergente() -> None:
    plan = _standard_plan()
    ready = ready_units(plan, NOW)
    dispatch = _reserved_dispatch(plan, ready, wave_id="0123456789abcdef")

    with pytest.raises(ValueError, match="wave_mismatch"):
        execution_request(plan, dispatch, max_concurrency=4)


def test_execution_request_clampa_max_concurrency() -> None:
    plan = _standard_plan()
    ready = ready_units(plan, NOW)
    dispatch = _reserved_dispatch(plan, ready)

    by_deployment_limit = execution_request(
        replace(plan, deployment_limit=1), dispatch, max_concurrency=99
    )
    assert by_deployment_limit.max_concurrency == 1

    by_unit_count = execution_request(plan, dispatch, max_concurrency=99)
    assert by_unit_count.max_concurrency == len(dispatch.unit_ids)

    by_max_concurrency = execution_request(plan, dispatch, max_concurrency=1)
    assert by_max_concurrency.max_concurrency == 1


@pytest.mark.parametrize(
    ("deployment_limit", "max_concurrency"), [(0, 4), (4, 0), (0, 0)]
)
def test_execution_request_rejeita_limites_nao_positivos(
    deployment_limit: int, max_concurrency: int
) -> None:
    plan = replace(_standard_plan(), deployment_limit=deployment_limit)
    ready = ready_units(plan, NOW)
    dispatch = _reserved_dispatch(plan, ready)

    with pytest.raises(ValueError, match="positive_limit_required"):
        execution_request(plan, dispatch, max_concurrency=max_concurrency)


def test_modulos_de_orchestration_nao_importam_infra_ou_frameworks() -> None:
    root = Path(__file__).parents[2] / "src/cnes_domain/orchestration"
    forbidden = {"boto3", "fastapi", "sqlalchemy", "cnes_infra"}
    imported: set[str] = set()
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
    assert imported.isdisjoint(forbidden)


def test_planner_nao_hardcoda_nomes_de_dataset_cnes() -> None:
    # source_catalog.py e frozen por design (CND-060) para conter a definicao
    # CNES; a genericidade protegida aqui e a do DAG em planner.py/fan_in.py.
    root = Path(__file__).parents[2] / "src/cnes_domain/orchestration"
    forbidden = ("CNES_LOCAL", "CNES_NACIONAL", "SIHD", "BPA", "SIA")
    for path in root.glob("*.py"):
        if path.name == "source_catalog.py":
            continue
        content = path.read_text(encoding="utf-8")
        for name in forbidden:
            assert name not in content
