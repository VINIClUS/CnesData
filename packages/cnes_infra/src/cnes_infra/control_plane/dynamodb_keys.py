"""Pure DynamoDB control-plane key builders."""

from datetime import datetime


def tenant_partition(tenant_id: str) -> str:
    """Cria a partição base do tenant."""
    return f"TENANT#{tenant_id}"


def global_partition() -> str:
    """Cria a partição global com prefixo de tenant."""
    return "TENANT#_GLOBAL"


def run_partition(tenant_id: str, run_id: str) -> str:
    """Cria a partição agregada do run."""
    return f"{tenant_partition(tenant_id)}#RUN#{run_id}"


def entity_key(tenant_id: str, entity: str, identifier: str) -> tuple[str, str]:
    """Cria a chave base de uma entidade do tenant."""
    return tenant_partition(tenant_id), f"{entity}#{identifier}"


def run_entity_key(tenant_id: str, run_id: str) -> tuple[str, str]:
    """Cria a chave base de um run."""
    return entity_key(tenant_id, "RUN", run_id)


def unit_key(tenant_id: str, run_id: str, unit_id: str) -> tuple[str, str]:
    """Cria a chave base de uma unidade."""
    return run_partition(tenant_id, run_id), f"UNIT#{unit_id}"


def dispatch_key(tenant_id: str, run_id: str) -> tuple[str, str]:
    """Cria a chave base do dispatch ativo."""
    return run_partition(tenant_id, run_id), "DISPATCH#ACTIVE"


def idempotency_key(tenant_id: str, scope: str, key: str) -> tuple[str, str]:
    """Cria a chave base de idempotência."""
    return tenant_partition(tenant_id), f"IDEMPOTENCY#{scope}#{key}"


def version_key(tenant_id: str, dataset_name: str, version_id: str) -> tuple[str, str]:
    """Cria a chave base de uma versão."""
    return tenant_partition(tenant_id), f"VERSION#{dataset_name}#{version_id}"


def pointer_key(tenant_id: str, dataset_name: str, pointer_name: str) -> tuple[str, str]:
    """Cria a chave base de um ponteiro."""
    return tenant_partition(tenant_id), f"POINTER#{dataset_name}#{pointer_name}"


def outbox_key(event_id: str) -> tuple[str, str]:
    """Cria a chave global de um evento."""
    return global_partition(), f"OUTBOX#{event_id}"


def dependency_marker_key(tenant_id: str, run_id: str, dependency_key: str) -> tuple[str, str]:
    """Cria a chave de um marcador de dependência."""
    return tenant_partition(tenant_id), f"RUN_DEP#{run_id}#{dependency_key}"


def timestamp(value: datetime) -> str:
    """Codifica um instante UTC ordenável."""
    return value.isoformat(timespec="microseconds")


def item_key(pk: str, sk: str) -> dict[str, dict[str, str]]:
    """Codifica uma chave no formato low-level do DynamoDB."""
    return {"pk": {"S": pk}, "sk": {"S": sk}}
