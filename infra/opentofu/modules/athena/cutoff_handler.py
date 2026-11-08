"""Lambda de cutoff: desabilita o workgroup Athena configurado no ambiente."""

import os
from typing import Any, Protocol

import boto3

WORKGROUP_VARIABLE = "ATHENA_WORKGROUP"
DISABLED = "DISABLED"


class AthenaClient(Protocol):
    def update_work_group(self, *, WorkGroup: str, State: str) -> object: ...  # noqa: N803


def _workgroup() -> str:
    name = os.environ.get(WORKGROUP_VARIABLE, "")
    if not name:
        raise RuntimeError(f"env={WORKGROUP_VARIABLE} reason=missing")
    return name


def handler(event: Any, context: Any, client: AthenaClient | None = None) -> dict[str, str]:
    """Args: event e context ignorados; client opcional para testes.
    Returns: Workgroup e estado aplicados.
    Raises: RuntimeError sem a variável de ambiente; ClientError da API, sem tratamento.
    """
    name = _workgroup()
    athena: AthenaClient = client if client is not None else boto3.client("athena")
    athena.update_work_group(WorkGroup=name, State=DISABLED)
    return {"workgroup": name, "state": DISABLED}
