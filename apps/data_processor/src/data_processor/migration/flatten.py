"""Achatamento canonico de documentos em metricas `doc_id::caminho` para comparacao exata."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

Scalar = bool | int | str | None
APPLIED_NORMALIZATIONS = ("date_iso8601",)


@dataclass(frozen=True, slots=True)
class Empty:
    empty: Literal["list", "dict"]


EMPTY_LIST = Empty("list")
EMPTY_DICT = Empty("dict")
Leaf = Scalar | Empty


def _child(node: object, segment: str, column: str) -> object:
    children: dict[str, object] = cast("dict[str, object]", node) if isinstance(node, dict) else {}
    if segment not in children:
        raise ValueError(f"key_missing column={column}")
    return children[segment]


def _dig(row: Mapping[str, object], column: str) -> object:
    current: object = row
    for segment in column.split("."):
        current = _child(current, segment, column)
    return current


def _key_text(row: Mapping[str, object], key: Sequence[str]) -> str:
    values = [_scalar(_dig(row, column), f"key:{column}") for column in key]
    text = "|".join("" if value is None else str(value) for value in values)
    if "]" in text:
        raise ValueError(f"key_bracket_invalid columns={'|'.join(key)}")
    return text


def canonical_rows(
    rows: Iterable[Mapping[str, object]], key: Sequence[str]
) -> dict[str, Mapping[str, object]]:
    """Indexa as linhas pela chave composta, em ordem canonica.

    Args: rows: linhas em qualquer ordem. key: colunas (caminhos pontuados) da chave.
    Returns: mapa chave -> linha, ordenado pela chave.
    Raises: ValueError: coluna da chave ausente, com colchete de fechamento ou duplicada.
    """
    indexed: dict[str, Mapping[str, object]] = {}
    for row in rows:
        text = _key_text(row, key)
        if text in indexed:
            raise ValueError(f"duplicate_key key={text}")
        indexed[text] = row
    return dict(sorted(indexed.items()))


def _scalar(value: object, metric: str) -> Scalar:
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        raise ValueError(f"float_not_allowed metric={metric}")
    raise ValueError(f"unsupported_value metric={metric} type={type(value).__name__}")


def _join(prefix: str, name: str) -> str:
    return f"{prefix}.{name}" if prefix else name


@dataclass(slots=True)
class _Flattener:
    doc_id: str
    list_keys: Mapping[str, Sequence[str]]
    metrics: dict[str, Leaf] = field(default_factory=dict[str, Leaf])

    def _emit(self, path: str, value: object) -> None:
        metric = f"{self.doc_id}::{path}"
        if metric in self.metrics:
            raise ValueError(f"duplicate_metric metric={metric}")
        self.metrics[metric] = value if isinstance(value, Empty) else _scalar(value, metric)

    def walk(self, value: object, path: str, schema: str) -> None:
        if isinstance(value, dict):
            children = cast("dict[str, object]", value)
            if not children:
                self._emit(path, EMPTY_DICT)
            for name, child in children.items():
                self.walk(child, _join(path, name), _join(schema, name))
        elif isinstance(value, list):
            self._walk_list(cast("list[object]", value), path, schema)
        else:
            self._emit(path, value)

    def _walk_list(self, items: list[object], path: str, schema: str) -> None:
        if not items:
            self._emit(path, EMPTY_LIST)
            return
        key = self.list_keys.get(schema)
        if not key:
            for index, item in enumerate(items):
                self.walk(item, f"{path}[{index}]", schema)
            return
        rows = canonical_rows(cast("list[Mapping[str, object]]", items), key)
        for text, row in rows.items():
            for name, child in row.items():
                self.walk(child, _join(f"{path}[{text}]", name), _join(schema, name))


def flatten_payload(
    doc_id: str, value: object, list_keys: Mapping[str, Sequence[str]]
) -> dict[str, Leaf]:
    """Achata um documento em metricas `doc_id::caminho` -> folha, sem depender da ordem.

    Args: doc_id: prefixo das metricas. value: documento. list_keys: caminho da lista -> colunas
        da chave (`""` e a lista raiz); listas sem chave sao posicionais.
    Returns: metricas ordenadas; lista ou objeto vazio vira a folha `Empty` do seu tipo.
    Raises: ValueError: float, tipo nao suportado, chave ausente, invalida ou duplicada.
    """
    flattener = _Flattener(doc_id, list_keys)
    flattener.walk(value, "", "")
    return dict(sorted(flattener.metrics.items()))
