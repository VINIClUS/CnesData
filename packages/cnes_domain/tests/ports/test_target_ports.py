"""Testes dos ports alvo do data plane."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone
from inspect import Parameter, signature

import pytest
from pydantic import ValidationError

from cnes_domain.ports.audit import AuditSinkPort
from cnes_domain.ports.control_plane import ControlPlanePort
from cnes_domain.ports.object_storage import NullObjectStoragePort, ObjectStoragePort
from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort
from cnes_domain.ports.processing import (
    CancelRunExecution,
    ConcurrencyPolicy,
    ExecutionCallbacks,
    ExecutionPermit,
    ExecutionPolicyConfig,
    ExecutionStarted,
    ExecutionStatus,
    ProcessorExecutorPort,
    RunUnitMessage,
    StartRunExecution,
)
from cnes_domain.ports.repository import (
    EquipeRepository,
    EstabelecimentoRepository,
    ProfissionalRepository,
)
from cnes_domain.ports.serving import ServingAccessPort, ServingGrant, ServingRequest
from cnes_domain.ports.storage import (
    EstabelecimentoStoragePort,
    NullUnitOfWork,
    ProfissionalStoragePort,
    UnitOfWorkPort,
    VinculoStoragePort,
)

NOW = datetime(2026, 8, 26, 12, tzinfo=UTC)
HEX_ID = "0123456789abcdef"
OTHER_HEX_ID = "fedcba9876543210"
LEGACY_PROTOCOLS = (
    ObjectStoragePort, EstabelecimentoRepository, ProfissionalRepository, EquipeRepository,
    ProfissionalStoragePort, EstabelecimentoStoragePort, VinculoStoragePort, UnitOfWorkPort,
)
CONTROL_PLANE_SIGNATURES = {
    "get_tenant": (("tenant_id", str),),
    "put_tenant": (("tenant", "Tenant"),),
    "get_membership": (("tenant_id", str), ("user_id", str)),
    "put_membership": (("membership", "Membership"),),
    "get_agent": (("tenant_id", str), ("agent_id", str)),
    "put_agent": (("agent", "Agent"),),
    "create_job": (("job", "Job"), ("event", "OutboxEvent")),
    "get_job": (("tenant_id", str), ("job_id", str)),
    "latest_succeeded_job": (
        ("tenant_id", str),
        ("agent_id", str),
        ("source_type", str),
        ("file_subtype", str),
        ("competencia", str),
    ),
    "list_raw_manifest_chain": (
        ("tenant_id", str),
        ("source_type", str),
        ("file_subtype", str),
        ("competencia", str),
        ("limit", int, 31),
    ),
    "list_claimable_jobs": (
        ("tenant_id", str),
        ("agent_id", str),
        ("limit", int),
    ),
    "claim_job": (("command", "ClaimJob"),),
    "renew_job_lease": (("command", "RenewJobLease"),),
    "complete_job": (("command", "CompleteJob"), ("event", "OutboxEvent")),
    "fail_job": (("command", "FailJob"), ("event", "OutboxEvent")),
    "cancel_job": (("command", "CancelJob"), ("event", "OutboxEvent")),
    "put_run": (("run", "Run"),),
    "get_run": (("tenant_id", str), ("run_id", str)),
    "list_waiting_runs_for_dependency": (
        ("tenant_id", str),
        ("source_type", str),
        ("file_subtype", str),
        ("competencia", str),
        ("limit", int, 100),
    ),
    "list_recoverable_runs": (("now", datetime), ("limit", int, 100)),
    "transition_run": (("command", "TransitionRun"), ("event", "OutboxEvent")),
    "put_run_units": (("command", "PutRunUnits"),),
    "list_run_units": (("tenant_id", str), ("run_id", str)),
    "claim_run_unit": (("command", "ClaimRunUnit"),),
    "commit_run_unit": (("command", "CommitRunUnit"), ("event", "OutboxEvent")),
    "fail_run_unit": (("command", "FailRunUnit"), ("event", "OutboxEvent")),
    "finalize_run_cancellation": (
        ("command", "FinalizeRunCancellation"),
        ("event", "OutboxEvent"),
    ),
    "reserve_run_dispatch": (("command", "ReserveRunDispatch"),),
    "bind_run_dispatch": (("command", "BindRunDispatch"),),
    "finish_run_dispatch": (("command", "FinishRunDispatch"),),
    "get_active_run_dispatch": (("tenant_id", str), ("run_id", str)),
    "begin_idempotency": (("command", "BeginIdempotency"),),
    "publish_dataset": (("command", "PublishDataset"),),
    "get_dataset_pointer": (("tenant_id", str), ("dataset_name", str)),
    "get_dataset_version": (
        ("tenant_id", str),
        ("dataset_name", str),
        ("version_id", str),
    ),
    "put_access_request": (("request", "AccessRequest"), ("event", "OutboxEvent")),
    "get_access_request": (("tenant_id", str), ("request_id", str)),
    "decide_access_request": (("request", "AccessRequest"), ("event", "OutboxEvent")),
    "pending_outbox": (("limit", int),),
    "mark_outbox_delivered": (("event_id", str), ("delivered_at", datetime)),
}
OTHER_SIGNATURES = {
    ObjectStorePort: {
        "put": (("key", str), ("body", "BinaryIO"), ("expected_sha256", str)),
        "open": (("key", str),),
        "stat": (("key", str),),
        "promote": (
            ("source_key", str),
            ("destination_key", str),
            ("expected_sha256", str),
        ),
        "delete": (("key", str),),
    },
    ProcessorExecutorPort: {
        "start": (("request", StartRunExecution),),
        "cancel": (("request", CancelRunExecution),),
        "status": (("execution_ref", str),),
    },
    ServingAccessPort: {"authorize": (("request", ServingRequest),)},
    AuditSinkPort: {"append": (("event", "OutboxEvent"),)},
}
CONTROL_PLANE_RETURNS = {
    "get_tenant": "Tenant | None", "put_tenant": "None",
    "get_membership": "Membership | None", "put_membership": "None",
    "get_agent": "Agent | None", "put_agent": "None",
    "create_job": "Job", "get_job": "Job | None",
    "latest_succeeded_job": "Job | None", "claim_job": "Job | None",
    "list_raw_manifest_chain": "tuple[ManifestRef, ...]", "renew_job_lease": "Job",
    "list_claimable_jobs": "tuple[Job, ...]", "complete_job": "Job",
    "fail_job": "Job", "cancel_job": "Job", "put_run": "None",
    "get_run": "Run | None", "list_recoverable_runs": "tuple[Run, ...]",
    "list_waiting_runs_for_dependency": "tuple[Run, ...]", "transition_run": "Run",
    "put_run_units": "tuple[RunUnit, ...]", "list_run_units": "tuple[RunUnit, ...]",
    "claim_run_unit": "RunUnit | None", "commit_run_unit": "RunUnit",
    "fail_run_unit": "RunUnit", "finalize_run_cancellation": "Run",
    "reserve_run_dispatch": "RunDispatch", "bind_run_dispatch": "RunDispatch",
    "finish_run_dispatch": "RunDispatch", "get_active_run_dispatch": "RunDispatch | None",
    "begin_idempotency": "IdempotencyOutcome", "publish_dataset": "DatasetPointer",
    "get_dataset_pointer": "DatasetPointer | None", "put_access_request": "None",
    "get_dataset_version": "DatasetVersion | None", "decide_access_request": "AccessRequest",
    "get_access_request": "AccessRequest | None", "pending_outbox": "tuple[OutboxEvent, ...]",
    "mark_outbox_delivered": "None",
}
OTHER_RETURNS = {
    ObjectStorePort: {
        "put": "ObjectStat",
        "open": "ContextManager[BinaryIO]",
        "stat": "ObjectStat | None",
        "promote": "ObjectStat",
        "delete": "None",
    },
    ProcessorExecutorPort: {"start": "str", "cancel": "None", "status": "ExecutionStatus"},
    ServingAccessPort: {"authorize": "ServingGrant"},
    AuditSinkPort: {"append": "None"},
}

def _annotation_name(annotation: object) -> object:
    annotations = {
        "str": str, "int": int, "datetime": datetime,
        "StartRunExecution": StartRunExecution, "CancelRunExecution": CancelRunExecution,
        "ServingRequest": ServingRequest,
    }
    return annotations.get(annotation, annotation) if isinstance(annotation, str) else annotation


def _public_parameters(method: object) -> tuple[tuple[object, ...], ...]:
    parameters = tuple(signature(method).parameters.values())[1:]
    result = []
    for parameter in parameters:
        item = (parameter.name, _annotation_name(parameter.annotation))
        if parameter.default is not Parameter.empty:
            item += (parameter.default,)
        result.append(item)
    return tuple(result)


def _structural_adapter(protocol: type, missing: str | None = None) -> object:
    methods = {
        name: (lambda self, *args, **kwargs: None)
        for name in protocol.__protocol_attrs__
        if name != missing
    }
    return type("AdapterEstrutural", (), methods)()


def _start_values() -> dict[str, object]:
    return {
        "tenant_id": "354130",
        "run_id": "run-01",
        "wave_id": HEX_ID,
        "dispatch_id": OTHER_HEX_ID,
        "unit_ids": ("unit-01", "unit-02"),
        "max_concurrency": 2,
    }


def _message_values() -> dict[str, object]:
    return {
        "tenant_id": "354130",
        "run_id": "run-01",
        "wave_id": HEX_ID,
        "dispatch_id": OTHER_HEX_ID,
        "unit_id": "unit-01",
        "owner": "worker-01",
        "now": NOW,
        "lease_seconds": 30,
    }


def _permit_values() -> dict[str, object]:
    return {
        "tenant_id": "354130",
        "run_id": "run-01",
        "max_concurrency": 2,
        "policy_version": 0,
        "fencing_token": 0,
    }


def _grant_values() -> dict[str, object]:
    return {
        "tenant_id": "354130",
        "run_id": "run-01",
        "version_id": "run-01",
        "object_keys": (
            "serving/354130/run-01/overview.json",
            "serving/354130/run-01/divergences.json",
        ),
    }


@pytest.mark.parametrize(
    ("protocol", "signatures"),
    [(ControlPlanePort, CONTROL_PLANE_SIGNATURES), *OTHER_SIGNATURES.items()],
)
def test_protocolos_expoem_assinaturas_congeladas(protocol, signatures):
    assert set(protocol.__protocol_attrs__) == set(signatures)
    for name, expected in signatures.items():
        method = getattr(protocol, name)
        assert _public_parameters(method) == expected
        returns = CONTROL_PLANE_RETURNS if protocol is ControlPlanePort else OTHER_RETURNS[protocol]
        assert method.__annotations__["return"] == returns[name]
        method(object(), *(None for _ in expected))


@pytest.mark.parametrize(
    "protocol",
    [ControlPlanePort, ObjectStorePort, ProcessorExecutorPort, ServingAccessPort, AuditSinkPort],
)
def test_adapter_estrutural_satisfaz_protocolos_runtime(protocol):
    method = next(iter(protocol.__protocol_attrs__))
    assert isinstance(_structural_adapter(protocol), protocol)
    assert not isinstance(_structural_adapter(protocol, method), protocol)


@pytest.mark.parametrize("protocol", LEGACY_PROTOCOLS)
def test_protocolos_legados_permanecem_importaveis(protocol):
    methods = (name for name in protocol.__protocol_attrs__ if hasattr(protocol, name))
    for name in methods:
        method = getattr(protocol, name)
        parameters = tuple(signature(method).parameters.values())[1:]
        method(object(), *(None for _ in parameters))


def test_ports_nulos_legados_permanecem_operacionais():
    objects = NullObjectStoragePort()
    assert objects.generate_presigned_upload_url("bucket", "key") == "null://bucket/key"
    assert not objects.object_exists("bucket", "key")
    assert objects.get_presigned_download_url("bucket", "key") == "null://bucket/key"
    with NullUnitOfWork() as unit_of_work:
        assert unit_of_work.profissionais.gravar(()) == 0
        assert unit_of_work.estabelecimentos.gravar(()) == 0
        assert unit_of_work.vinculos.snapshot_replace("2026-01", "CNES", ()) == 0


def test_object_stat_preserva_valores_e_eh_imutavel():
    stat = ObjectStat("raw/object.parquet", 4, "a" * 64)
    assert (stat.key, stat.size_bytes, stat.sha256) == ("raw/object.parquet", 4, "a" * 64)
    assert not hasattr(stat, "__dict__")
    with pytest.raises(FrozenInstanceError):
        stat.key = "other"


@pytest.mark.parametrize(
    ("model", "values"),
    [
        (StartRunExecution, _start_values()),
        (RunUnitMessage, _message_values()),
        (
            CancelRunExecution,
            {"tenant_id": "354130", "run_id": "run-01", "execution_ref": None},
        ),
        (ExecutionPermit, _permit_values()),
        (
            ServingRequest,
            {"user_id": "gestor", "tenant_id": "354130", "dataset_name": "cnes"},
        ),
        (ServingGrant, _grant_values()),
    ],
)
def test_modelos_sao_strict_frozen_e_proibem_campos_extras(model, values):
    instance = model.model_validate(values)
    expected_fields = set(values) | ({"binding_context"} if model is ExecutionPermit else set())
    assert set(model.model_fields) == expected_fields
    field = next(iter(model.model_fields))
    with pytest.raises(ValidationError, match="frozen"):
        setattr(instance, field, "other")
    with pytest.raises(ValidationError, match="Extra inputs"):
        model.model_validate(values | {"extra": "forbidden"})
    with pytest.raises(ValidationError):
        model.model_validate(values | {field: 1})


@pytest.mark.parametrize("field", ["tenant_id", "run_id"])
def test_start_rejeita_identidade_vazia(field):
    with pytest.raises(ValidationError, match="blank_value"):
        StartRunExecution.model_validate(_start_values() | {field: " "})


@pytest.mark.parametrize("field", ["wave_id", "dispatch_id"])
@pytest.mark.parametrize("value", ["ABCDEF0123456789", "0123456789abcde", "g123456789abcdef"])
def test_start_rejeita_identificador_que_nao_e_lowercase_16_hex(field, value):
    with pytest.raises(ValidationError, match=f"invalid_{field}"):
        StartRunExecution.model_validate(_start_values() | {field: value})


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("unit_ids", (), "unit_ids_required"),
        ("unit_ids", ("unit-01", "unit-01"), "duplicate_unit_id"),
        ("unit_ids", ("unit-01", " "), "blank_value"),
        ("max_concurrency", 0, "positive_value_required"),
    ],
)
def test_start_rejeita_unidades_ou_concorrencia_invalidas(field, value, message):
    with pytest.raises(ValidationError, match=message):
        StartRunExecution.model_validate(_start_values() | {field: value})


@pytest.mark.parametrize("field", ["tenant_id", "run_id", "unit_id", "owner"])
def test_mensagem_rejeita_identidade_vazia(field):
    with pytest.raises(ValidationError, match="blank_value"):
        RunUnitMessage.model_validate(_message_values() | {field: ""})


@pytest.mark.parametrize("field", ["wave_id", "dispatch_id"])
def test_mensagem_rejeita_identificador_invalido(field):
    with pytest.raises(ValidationError, match=f"invalid_{field}"):
        RunUnitMessage.model_validate(_message_values() | {field: "not-hex"})


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("now", NOW.replace(tzinfo=None), "datetime_not_utc"),
        ("now", NOW.astimezone(timezone(timedelta(hours=-3))), "datetime_not_utc"),
        ("lease_seconds", 0, "positive_value_required"),
    ],
)
def test_mensagem_valida_utc_e_lease(field, value, message):
    with pytest.raises(ValidationError, match=message):
        RunUnitMessage.model_validate(_message_values() | {field: value})


@pytest.mark.parametrize(
    ("model", "values", "field"),
    [
        (
            CancelRunExecution,
            {"tenant_id": "354130", "run_id": "run-01", "execution_ref": None},
            "tenant_id",
        ),
        (ExecutionPermit, _permit_values(), "run_id"),
        (
            ServingRequest,
            {"user_id": "gestor", "tenant_id": "354130", "dataset_name": "cnes"},
            "user_id",
        ),
        (ServingGrant, _grant_values(), "version_id"),
    ],
)
def test_valores_rejeitam_identidade_vazia(model, values, field):
    with pytest.raises(ValidationError, match="blank_value"):
        model.model_validate(values | {field: " "})


def test_cancel_rejeita_execution_ref_vazia_quando_presente():
    values = {"tenant_id": "354130", "run_id": "run-01", "execution_ref": ""}
    with pytest.raises(ValidationError, match="blank_value"):
        CancelRunExecution.model_validate(values)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("max_concurrency", 0, "positive_value_required"),
        ("policy_version", -1, "negative_counter"),
        ("fencing_token", -1, "negative_counter"),
    ],
)
def test_permit_rejeita_limites_invalidos(field, value, message):
    with pytest.raises(ValidationError, match=message):
        ExecutionPermit.model_validate(_permit_values() | {field: value})


def test_permit_preserva_binding_context_por_identidade():
    binding_context = object()
    permit = ExecutionPermit.model_validate(_permit_values() | {"binding_context": binding_context})
    assert permit.binding_context is binding_context


def test_callbacks_e_config_preservam_callables_e_sao_imutaveis():
    def policy(run, dispatch, limit):
        return None

    def started(run, request, ref, permit):
        return None

    callbacks = ExecutionCallbacks(policy=policy, started=started)
    config = ExecutionPolicyConfig(
        deployment_limit=2,
        dispatch_lease_seconds=30,
        callbacks=callbacks,
    )
    assert config.callbacks.policy is policy
    assert config.callbacks.started is started
    with pytest.raises(FrozenInstanceError):
        config.deployment_limit = 3


@pytest.mark.parametrize("field", ["deployment_limit", "dispatch_lease_seconds"])
@pytest.mark.parametrize("value", [0, -1])
def test_config_rejeita_limites_nao_positivos(field, value):
    callbacks = ExecutionCallbacks(lambda *args: None, lambda *args: None)
    values = {
        "deployment_limit": 2,
        "dispatch_lease_seconds": 30,
        "callbacks": callbacks,
    }
    with pytest.raises(ValueError, match="positive_value_required"):
        ExecutionPolicyConfig(**(values | {field: value}))


def test_execution_status_contem_estados_congelados():
    expected = ["RUNNING", "SUCCEEDED", "FAILED", "CANCELED"]
    assert [status.value for status in ExecutionStatus] == expected


def test_aliases_de_callback_resolvem_sem_imports_runtime():
    assert ConcurrencyPolicy.__value__
    assert ExecutionStarted.__value__


@pytest.mark.parametrize(
    ("object_keys", "message"),
    [
        ((), "serving_keys_required"),
        (("serving/354130/run-01/overview.json",) * 2, "duplicate_serving_key"),
        (("",), "serving_key_forbidden"),
        (("serving/354130/run-01/../overview.json",), "serving_key_forbidden"),
        (("serving/354130/run-01/nested/overview.json",), "serving_key_forbidden"),
        (("serving/354130/run-01/overview.parquet",), "serving_key_forbidden"),
        (("serving/other/run-01/overview.json",), "serving_key_forbidden"),
        (("serving/354130/other/overview.json",), "serving_key_forbidden"),
        (("serving/354130/run-01/.json",), "serving_key_forbidden"),
    ],
)
def test_serving_grant_rejeita_keys_invalidas(object_keys, message):
    with pytest.raises(ValidationError, match=message):
        ServingGrant.model_validate(_grant_values() | {"object_keys": object_keys})


def test_serving_grant_preserva_ordem_das_keys():
    grant = ServingGrant.model_validate(_grant_values())
    assert grant.object_keys == _grant_values()["object_keys"]


@pytest.mark.parametrize("field", ["tenant_id", "run_id"])
def test_serving_grant_rejeita_traversal_na_identidade(field):
    values = _grant_values() | {field: ".."}
    values["object_keys"] = (f"serving/{values['tenant_id']}/{values['run_id']}/overview.json",)
    with pytest.raises(ValidationError, match="serving_key_forbidden"):
        ServingGrant.model_validate(values)
