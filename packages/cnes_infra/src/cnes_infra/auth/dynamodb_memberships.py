"""Candidatos de membership por usuário via projeção esparsa do gsi1."""
from typing import TYPE_CHECKING

from cnes_infra.control_plane.dynamodb_codec import query_pages
from cnes_infra.control_plane.dynamodb_keys import key_component

if TYPE_CHECKING:
    from botocore.client import BaseClient

_TENANT_PREFIX = "TENANT#"


class DynamoDBMembershipCandidates:
    def __init__(self, client: "BaseClient", table_name: str, index_name: str = "gsi1") -> None:
        self._client = client
        self._table_name = table_name
        self._index_name = index_name

    def list_candidates(self, user_id: str) -> tuple[str, ...]:
        """Returns: tenants candidatos do usuário; exigem revalidação na chave base."""
        partition, sort = f"{self._index_name}pk", f"{self._index_name}sk"
        request = {
            "TableName": self._table_name,
            "IndexName": self._index_name,
            "KeyConditionExpression": f"{partition} = :user",
            "ExpressionAttributeValues": {":user": {"S": f"USER#{key_component(user_id)}"}},
            "ProjectionExpression": sort,
        }
        tenant_ids: dict[str, None] = {}
        for page in query_pages(self._client, request):
            for item in page:
                tenant_id = _tenant_id(item.get(sort, {}).get("S", ""))
                if tenant_id is not None:
                    tenant_ids[tenant_id] = None
        return tuple(tenant_ids)


def _tenant_id(value: str) -> str | None:
    encoded = value.removeprefix(_TENANT_PREFIX)
    if encoded == value or not encoded:
        return None
    try:
        tenant_id = bytes.fromhex(encoded).decode()
    except ValueError:
        return None
    return tenant_id if key_component(tenant_id) == encoded else None
