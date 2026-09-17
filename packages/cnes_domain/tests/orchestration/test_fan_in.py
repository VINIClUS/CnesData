from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from cnes_domain.control_plane.entities import Run, RunDependency
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.orchestration.fan_in import decide_fan_in
from cnes_domain.orchestration.planner import PlanRequest, RawManifestRef, RunPlan, plan_run

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


def _map_units(plan: RunPlan, stage: RunStage, state: RunUnitState) -> RunPlan:
    units = tuple(
        unit.model_copy(update={"state": state}) if unit.stage is stage else unit
        for unit in plan.units
    )
    return replace(plan, units=units)


def _succeed_all(plan: RunPlan) -> RunPlan:
    units = tuple(unit.model_copy(update={"state": RunUnitState.SUCCEEDED}) for unit in plan.units)
    return replace(plan, units=units)


def test_fonte_opcional_ausente_degrada_sem_fingir_completo() -> None:
    plan = _succeed_all(
        plan_run(
            PlanRequest(
                run=_standard_run(),
                manifests=(_raw_ref("manifest-a1", "SRC_A", "SUB_1"),),
                deployment_limit=4,
            )
        )
    )

    decision = decide_fan_in(plan)

    assert decision.state == RunState.PUBLISHING
    assert decision.publish_ready is True
    assert decision.missing_sources == ("SRC_B/SUB_2",)


def test_normalizacao_opcional_degradada_satisfaz_reconciliacao_e_entra_em_missing_sources() -> (
    None
):
    plan = _standard_plan()
    normalize_a = next(
        u for u in plan.units if u.source_type == "SRC_A" and u.stage is RunStage.NORMALIZE
    )
    normalize_b = next(
        u for u in plan.units if u.source_type == "SRC_B" and u.stage is RunStage.NORMALIZE
    )
    succeeded_a = normalize_a.model_copy(update={"state": RunUnitState.SUCCEEDED})
    degraded_b = normalize_b.model_copy(
        update={"state": RunUnitState.SUCCEEDED_DEGRADED, "error_code": "raw_source_unavailable"}
    )
    reconcile = next(u for u in plan.units if u.stage is RunStage.RECONCILE)
    materialize = next(u for u in plan.units if u.stage is RunStage.MATERIALIZE)
    degraded_plan = replace(plan, units=(succeeded_a, degraded_b, reconcile, materialize))

    decision_plan = _map_units(
        _map_units(degraded_plan, RunStage.RECONCILE, RunUnitState.SUCCEEDED),
        RunStage.MATERIALIZE,
        RunUnitState.SUCCEEDED,
    )

    decision = decide_fan_in(decision_plan)

    assert decision.state == RunState.PUBLISHING
    assert decision.missing_sources == ("SRC_B/SUB_2",)


def test_normalizacao_required_com_falha_final_falha_o_run() -> None:
    plan = _standard_plan()
    normalize_a = next(
        u for u in plan.units if u.source_type == "SRC_A" and u.stage is RunStage.NORMALIZE
    )
    failed_a = normalize_a.model_copy(
        update={"state": RunUnitState.FAILED_FINAL, "error_code": "source_unreachable"}
    )
    others = tuple(u for u in plan.units if u.unit_id != normalize_a.unit_id)
    failed_plan = replace(plan, units=(failed_a, *others))

    decision = decide_fan_in(failed_plan)

    assert decision.state == RunState.FAILED
    assert decision.publish_ready is False


@pytest.mark.parametrize("stage", [RunStage.RECONCILE, RunStage.MATERIALIZE])
def test_falha_final_downstream_sempre_falha_o_run(stage: RunStage) -> None:
    plan = _standard_plan()
    units = tuple(
        unit.model_copy(update={"state": RunUnitState.FAILED_FINAL, "error_code": "downstream"})
        if unit.stage is stage
        else unit
        for unit in plan.units
    )
    failed_plan = replace(plan, units=units)

    decision = decide_fan_in(failed_plan)

    assert decision.state == RunState.FAILED


def test_normalizacao_opcional_em_failed_final_nao_falha_o_run() -> None:
    plan = _standard_plan()
    normalize_b = next(
        u for u in plan.units if u.source_type == "SRC_B" and u.stage is RunStage.NORMALIZE
    )
    failed_b = normalize_b.model_copy(
        update={"state": RunUnitState.FAILED_FINAL, "error_code": "raw_source_unavailable"}
    )
    others = tuple(u for u in plan.units if u.unit_id != normalize_b.unit_id)
    plan_with_failed_optional = replace(plan, units=(failed_b, *others))

    decision = decide_fan_in(plan_with_failed_optional)

    assert decision.state == RunState.PROCESSING


def test_missing_sources_e_ordenada_e_sem_duplicatas() -> None:
    three_optional = (
        RunDependency(source_type="SRC_A", file_subtype="SUB_1", required=True),
        RunDependency(source_type="SRC_Z", file_subtype="SUB_9", required=False),
        RunDependency(source_type="SRC_M", file_subtype="SUB_5", required=False),
    )
    plan = plan_run(
        PlanRequest(
            run=_run(three_optional),
            manifests=(_raw_ref("manifest-a1", "SRC_A", "SUB_1"),),
            deployment_limit=4,
        )
    )

    decision = decide_fan_in(plan)

    assert decision.missing_sources == ("SRC_M/SUB_5", "SRC_Z/SUB_9")


def _processing_plan() -> RunPlan:
    return _standard_plan()


def _waiting_plan() -> RunPlan:
    return plan_run(
        PlanRequest(
            run=_standard_run(),
            manifests=(_raw_ref("manifest-b1", "SRC_B", "SUB_2"),),
            deployment_limit=4,
        )
    )


def _publishing_plan() -> RunPlan:
    return _succeed_all(_standard_plan())


def _failed_plan() -> RunPlan:
    plan = _standard_plan()
    normalize_a = next(
        u for u in plan.units if u.source_type == "SRC_A" and u.stage is RunStage.NORMALIZE
    )
    failed_a = normalize_a.model_copy(
        update={"state": RunUnitState.FAILED_FINAL, "error_code": "source_unreachable"}
    )
    others = tuple(u for u in plan.units if u.unit_id != normalize_a.unit_id)
    return replace(plan, units=(failed_a, *others))


@pytest.mark.parametrize(
    ("build_plan", "expected_state", "expected_publish_ready"),
    [
        (_processing_plan, RunState.PROCESSING, False),
        (_waiting_plan, RunState.WAITING_INPUTS, False),
        (_publishing_plan, RunState.PUBLISHING, True),
        (_failed_plan, RunState.FAILED, False),
    ],
)
def test_matriz_de_fan_in_cobre_todos_os_estados(
    build_plan: Callable[[], RunPlan], expected_state: RunState, expected_publish_ready: bool
) -> None:
    decision = decide_fan_in(build_plan())

    assert decision.state == expected_state
    assert decision.publish_ready == expected_publish_ready


@pytest.mark.parametrize(
    ("source_type", "file_subtype"),
    [("ALPHA_SOURCE", "ALPHA_SUB"), ("ZETA_SOURCE", "ZETA_SUB"), ("X1", "Y2")],
)
def test_decide_fan_in_e_generico_para_qualquer_run_dependency(
    source_type: str, file_subtype: str
) -> None:
    dependency = RunDependency(source_type=source_type, file_subtype=file_subtype, required=True)
    manifest = _raw_ref("manifest-1", source_type, file_subtype)
    plan = _succeed_all(
        plan_run(PlanRequest(run=_run((dependency,)), manifests=(manifest,), deployment_limit=4))
    )

    decision = decide_fan_in(plan)

    assert decision.state == RunState.PUBLISHING
    assert decision.missing_sources == ()
