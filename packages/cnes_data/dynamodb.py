"""Primitivas portáveis de encoding e paginação DynamoDB."""
from collections.abc import Iterator
from typing import Any

type Item = dict[str, Any]

def key_component(value: str) -> str:
    """Codifica componente preservando ordem e removendo ambiguidade."""
    return value.encode().hex()

def query_pages(client: Any, request: dict[str, Any]) -> Iterator[tuple[Item, ...]]:
    """Percorre páginas de uma Query sob demanda."""
    page_request = dict(request)
    while True:
        response = client.query(**page_request)
        yield tuple(response.get("Items", ()))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return
        page_request["ExclusiveStartKey"] = last_key
