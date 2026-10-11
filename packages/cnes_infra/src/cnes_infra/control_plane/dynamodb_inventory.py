"""Inventário canônico de chaves, índices e TTL da tabela do plano de controle."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GlobalSecondaryIndex:
    """Índice global esparso; só itens com as duas chaves entram no índice."""

    name: str
    partition_key: str
    sort_key: str
    projection: str


@dataclass(frozen=True, slots=True)
class ControlPlaneTableInventory:
    """Chaves, índices e atributo de TTL usados pelos adapters DynamoDB."""

    partition_key: str
    sort_key: str
    key_attribute_type: str
    indexes: tuple[GlobalSecondaryIndex, ...]
    ttl_attribute: str

    def key_attributes(self) -> tuple[str, ...]:
        """Returns: atributos de chave da tabela seguidos dos atributos de cada índice."""
        index_keys = (
            key for index in self.indexes for key in (index.partition_key, index.sort_key)
        )
        return (self.partition_key, self.sort_key, *index_keys)


def _sparse_index(name: str) -> GlobalSecondaryIndex:
    return GlobalSecondaryIndex(name, f"{name}pk", f"{name}sk", "ALL")


CONTROL_PLANE_INVENTORY = ControlPlaneTableInventory(
    partition_key="pk",
    sort_key="sk",
    key_attribute_type="S",
    indexes=tuple(map(_sparse_index, ("gsi1", "gsi2", "gsi3", "gsi4", "gsi5", "gsi6"))),
    ttl_attribute="expires_at",
)
