"""Canonical DynamoDB encoding of Run items and dependency markers."""

from cnes_domain.control_plane.entities import Run
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.dynamodb_codec import (
    Action,
    Item,
    encode_marker,
    encode_model,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import (
    dependency_marker_key,
    key_component,
    run_entity_key,
    timestamp,
)

RECOVERABLE_RUN_STATES = {
    RunState.WAITING_INPUTS,
    RunState.PROCESSING,
    RunState.PUBLISHING,
    RunState.CANCEL_REQUESTED,
}


def run_item(run: Run) -> Item:
    """Codifica o Run canônico com o índice de recuperação."""
    attributes = {}
    if run.state in RECOVERABLE_RUN_STATES:
        attributes = {
            "gsi4pk": "RUN_RECOVERABLE",
            "gsi4sk": (
                f"{timestamp(run.created_at)}#{key_component(run.tenant_id)}#"
                f"{key_component(run.run_id)}"
            ),
        }
    return encode_model(run, "RUN", run_entity_key(run.tenant_id, run.run_id), attributes)


def run_dependency_actions(
    table_name: str, run: Run, reserved_actions: int
) -> tuple[Action, ...]:
    """Cria os Puts dos marcadores de dependência de um Run em WAITING_INPUTS.

    Raises: Conflict: TRANSACTION_LIMIT quando dependências + reservadas > 100.
    """
    if run.state is not RunState.WAITING_INPUTS:
        return ()
    if len(run.dependencies) + reserved_actions > 100:
        raise Conflict(ErrorCode.TRANSACTION_LIMIT)
    base_key = run_entity_key(run.tenant_id, run.run_id)
    actions = []
    for dependency in run.dependencies:
        values = (run.tenant_id, dependency.source_type,
                  dependency.file_subtype, run.competencia)
        identity = "RUN_DEP#" + "#".join(key_component(value) for value in values)
        marker_key = dependency_marker_key(run.tenant_id, run.run_id, identity)
        attributes = {
            "gsi3pk": identity,
            "gsi3sk": f"{timestamp(run.created_at)}#{key_component(run.run_id)}",
        }
        marker = encode_marker("RUN_DEP", marker_key, base_key, attributes)
        actions.append(put_action(table_name, marker, None))
    return tuple(actions)
