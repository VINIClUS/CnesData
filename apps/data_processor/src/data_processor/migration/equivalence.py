"""Contrato e comparacao exata de equivalencia entre oraculo congelado e candidato."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from enum import StrEnum
from functools import lru_cache
from hashlib import sha256
from typing import TYPE_CHECKING, Annotated, Literal, Self, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from pathlib import Path

Scalar = bool | int | str | None
Absent = Literal["legacy", "candidate"]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CheckKind = Literal[
    "candidate_equals_context",
    "candidate_equals_context_by_legacy",
    "candidate_is_text_of_legacy_int",
]

_FORBIDDEN_KEY = re.compile(r"tolerance|percent|epsilon", re.IGNORECASE)
_CONTEXT_SHAPE: dict[str, type] = {
    "candidate_equals_context": str,
    "candidate_equals_context_by_legacy": dict,
    "candidate_is_text_of_legacy_int": type(None),
}


class ComparisonStatus(StrEnum):
    MATCH = "MATCH"
    EXPLAINED = "EXPLAINED"
    MISMATCH = "MISMATCH"


@dataclass(frozen=True, slots=True)
class MetricComparison:
    metric: str
    legacy_value: Scalar
    candidate_value: Scalar
    status: ComparisonStatus
    rule_id: str | None
    absent: Absent | None = None


@dataclass(frozen=True, slots=True)
class SourceEquivalenceReport:
    tenant_id: str
    dataset: str
    source_types: tuple[str, ...]
    competencia: str
    legacy_sha256: str
    candidate_version_id: str
    comparisons: tuple[MetricComparison, ...]

    @property
    def accepted(self) -> bool:
        return bool(self.comparisons) and all(
            item.status is not ComparisonStatus.MISMATCH for item in self.comparisons
        )


class ContractInvalid(ValueError):
    pass


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)


class OracleRef(_Model):
    file: str = Field(min_length=1)
    path: tuple[str, ...] = ()


class CandidateRef(_Model):
    layer: Literal["normalized", "reconciliation", "serving"]
    leaf: str = Field(min_length=1)


class DocumentSpec(_Model):
    doc_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    oracle: OracleRef
    candidate: CandidateRef
    row_key: tuple[str, ...] = ()
    list_keys: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    raw_subtype: str | None = None

    @property
    def key_paths(self) -> dict[str, tuple[str, ...]]:
        return {**self.list_keys, "": self.row_key} if self.row_key else dict(self.list_keys)


class RawInput(_Model):
    manifest: dict[str, str | int | None]
    parquet_file: str | None = None
    rows: OracleRef | None = None
    dtypes: dict[str, Literal["String", "Int64", "Float64"]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _exactly_one_body(self) -> Self:
        if (self.parquet_file is None) == (self.rows is None):
            raise ValueError("raw_input_body_required")
        return self

    @property
    def file(self) -> str:
        return self.rows.file if self.rows is not None else cast("str", self.parquet_file)


class Provenance(_Model):
    kind: Literal["independent_frozen", "reproduction"]
    frozen_in: str = Field(min_length=1)
    data_nature: Literal["synthetic", "real"]


class DatasetSpec(_Model):
    tenant_id: str = Field(pattern=r"^[0-9]{6}$")
    competencias: tuple[str, ...] = Field(min_length=1, max_length=1)
    oracle_dir: str = Field(min_length=1)
    oracle_files: dict[str, Sha256]
    provenance: Provenance
    raw_inputs: tuple[RawInput, ...] = Field(min_length=1)
    documents: tuple[DocumentSpec, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _inputs_are_pinned_and_native(self) -> Self:
        referenced = {item.oracle.file for item in self.documents}
        for raw in self.raw_inputs:
            referenced.add(raw.file)
            if raw.manifest.get("competencia") != self.competencias[0]:
                raise ValueError("raw_manifest_competencia_mismatch")
        unpinned = sorted(referenced - set(self.oracle_files))
        if unpinned:
            raise ValueError(f"file_not_pinned file={unpinned[0]}")
        return self


class Rule(_Model):
    rule_id: str = Field(min_length=1)
    metrics: tuple[str, ...] = Field(min_length=1)
    check: CheckKind
    context: str | dict[str, str] | None = None
    absent_ok: bool
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def _context_matches_check(self) -> Self:
        if not isinstance(self.context, _CONTEXT_SHAPE[self.check]):
            raise ValueError(f"rule_context_invalid rule_id={self.rule_id}")
        return self


class EquivalenceContract(_Model):
    contract_version: Literal[1]
    clock: datetime
    datasets: dict[str, DatasetSpec]
    rules: tuple[Rule, ...]

    @field_validator("clock")
    @classmethod
    def _clock_is_utc(cls, value: datetime) -> datetime:
        if value.utcoffset() != timedelta(0):
            raise ValueError("clock_utc_required")
        return value

    @model_validator(mode="after")
    def _identifiers_are_unique(self) -> Self:
        rule_ids = [rule.rule_id for rule in self.rules]
        if len(set(rule_ids)) != len(rule_ids):
            raise ValueError("duplicate_rule_id")
        doc_ids = [item.doc_id for spec in self.datasets.values() for item in spec.documents]
        if len(set(doc_ids)) != len(doc_ids):
            raise ValueError("duplicate_doc_id")
        for name, spec in self.datasets.items():
            if any(not item.doc_id.startswith(f"{name}-") for item in spec.documents):
                raise ValueError(f"doc_id_prefix_invalid dataset={name}")
        return self

    @property
    def clock_offset(self) -> str:
        return self.clock.isoformat()

    @property
    def clock_z(self) -> str:
        return self.clock.isoformat().replace("+00:00", "Z")


def _reject_forbidden_keys(node: object) -> None:
    if isinstance(node, dict):
        for name, child in cast("dict[str, object]", node).items():
            if _FORBIDDEN_KEY.search(name):
                raise ContractInvalid(f"forbidden_field key={name}")
            _reject_forbidden_keys(child)
    elif isinstance(node, list):
        for child in cast("list[object]", node):
            _reject_forbidden_keys(child)


def load_contract(path: Path) -> EquivalenceContract:
    """Carrega o contrato e rejeita tolerancia estatistica, ids repetidos e checks abertos.

    Args: path: arquivo JSON do contrato.
    Returns: contrato validado e imutavel.
    Raises: ContractInvalid: JSON ilegivel, campo proibido ou violacao de schema.
    """
    data = path.read_bytes()
    try:
        _reject_forbidden_keys(json.loads(data))
        return EquivalenceContract.model_validate_json(data)
    except json.JSONDecodeError as error:
        raise ContractInvalid(f"contract_unreadable path={path.name}") from error
    except ValidationError as error:
        first = error.errors()[0]
        location = ".".join(str(part) for part in first["loc"])
        raise ContractInvalid(f"contract_invalid loc={location} msg={first['msg']}") from error


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
    return "|".join("" if value is None else str(value) for value in values)


def canonical_rows(
    rows: Iterable[Mapping[str, object]], key: Sequence[str]
) -> dict[str, Mapping[str, object]]:
    """Indexa as linhas pela chave composta, em ordem canonica.

    Args: rows: linhas em qualquer ordem. key: colunas (caminhos pontuados) da chave.
    Returns: mapa chave -> linha, ordenado pela chave.
    Raises: ValueError: coluna da chave ausente ou chave duplicada.
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
    metrics: dict[str, Scalar] = field(default_factory=dict[str, Scalar])

    def walk(self, value: object, path: str, schema: str) -> None:
        if isinstance(value, dict):
            for name, child in cast("dict[str, object]", value).items():
                self.walk(child, _join(path, name), _join(schema, name))
        elif isinstance(value, list):
            self._walk_list(cast("list[object]", value), path, schema)
        else:
            metric = f"{self.doc_id}::{path}"
            if metric in self.metrics:
                raise ValueError(f"duplicate_metric metric={metric}")
            self.metrics[metric] = _scalar(value, metric)

    def _walk_list(self, items: list[object], path: str, schema: str) -> None:
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
) -> dict[str, Scalar]:
    """Achata um documento em metricas `doc_id::caminho` -> escalar, sem depender da ordem.

    Args: doc_id: prefixo das metricas. value: documento. list_keys: caminho da lista -> colunas
        da chave (`""` e a lista raiz); listas sem chave sao posicionais.
    Returns: metricas ordenadas pelo nome.
    Raises: ValueError: float, tipo nao suportado, chave ausente ou duplicada.
    """
    flattener = _Flattener(doc_id, list_keys)
    flattener.walk(value, "", "")
    return dict(sorted(flattener.metrics.items()))


@dataclass(frozen=True, slots=True)
class _Observation:
    metric: str
    legacy: Scalar
    candidate: Scalar
    absent: Absent | None


type _Predicate = Callable[[Rule, _Observation, Mapping[str, str]], bool]


def _scalar_equal(left: Scalar, right: Scalar) -> bool:
    return type(left) is type(right) and left == right


@lru_cache(maxsize=256)
def _glob_regex(pattern: str) -> re.Pattern[str]:
    tokens = {"*": ".*", "?": "."}
    return re.compile("".join(tokens.get(char, re.escape(char)) for char in pattern), re.DOTALL)


def _covers(rule: Rule, metric: str) -> bool:
    return any(_glob_regex(pattern).fullmatch(metric) for pattern in rule.metrics)


def _candidate_is_context(key: str, seen: _Observation, context: Mapping[str, str]) -> bool:
    resolved = key.replace("{doc}", seen.metric.partition("::")[0])
    return resolved in context and _scalar_equal(seen.candidate, context[resolved])


def _equals_context(rule: Rule, seen: _Observation, context: Mapping[str, str]) -> bool:
    return _candidate_is_context(cast("str", rule.context), seen, context)


def _equals_context_by_legacy(rule: Rule, seen: _Observation, context: Mapping[str, str]) -> bool:
    mapping = cast("dict[str, str]", rule.context)
    if not isinstance(seen.legacy, str) or seen.legacy not in mapping:
        return False
    return _candidate_is_context(mapping[seen.legacy], seen, context)


def _text_of_legacy_int(rule: Rule, seen: _Observation, context: Mapping[str, str]) -> bool:
    return (
        type(seen.legacy) is int
        and type(seen.candidate) is str
        and seen.candidate == str(seen.legacy)
    )


_PREDICATES: dict[str, _Predicate] = {
    "candidate_equals_context": _equals_context,
    "candidate_equals_context_by_legacy": _equals_context_by_legacy,
    "candidate_is_text_of_legacy_int": _text_of_legacy_int,
}


def _explaining_rule(
    rules: Sequence[Rule], seen: _Observation, context: Mapping[str, str]
) -> str | None:
    if seen.absent == "candidate":
        return None
    for rule in rules:
        scoped = rule.absent_ok == (seen.absent == "legacy")
        if scoped and _covers(rule, seen.metric) and _PREDICATES[rule.check](rule, seen, context):
            return rule.rule_id
    return None


def _observe(
    name: str, legacy: Mapping[str, Scalar], candidate: Mapping[str, Scalar]
) -> _Observation:
    absent: Absent | None = None
    if name not in legacy:
        absent = "legacy"
    elif name not in candidate:
        absent = "candidate"
    return _Observation(name, legacy.get(name), candidate.get(name), absent)


def _compare_metric(
    rules: Sequence[Rule], context: Mapping[str, str], seen: _Observation
) -> MetricComparison:
    rule_id: str | None = None
    status = ComparisonStatus.MATCH
    if seen.absent is not None or not _scalar_equal(seen.legacy, seen.candidate):
        rule_id = _explaining_rule(rules, seen, context)
        status = ComparisonStatus.MISMATCH if rule_id is None else ComparisonStatus.EXPLAINED
    return MetricComparison(
        seen.metric, seen.legacy, seen.candidate, status, rule_id, seen.absent
    )


def compare_shadow_run(
    *,
    contract: EquivalenceContract,
    legacy: Mapping[str, Scalar],
    candidate: Mapping[str, Scalar],
    context: Mapping[str, str] | None = None,
) -> tuple[MetricComparison, ...]:
    """Compara metrica a metrica, sem tolerancia; so uma regra do contrato explica diferenca.

    Args: contract: regras aprovadas. legacy/candidate: metricas achatadas. context: valores
        observados do candidato que as regras podem exigir (run, relogio, ids, hashes).
    Returns: uma comparacao por metrica da uniao, ordenadas pelo nome.
    """
    known: Mapping[str, str] = {} if context is None else context
    names = sorted(legacy.keys() | candidate.keys())
    return tuple(
        _compare_metric(contract.rules, known, _observe(name, legacy, candidate))
        for name in names
    )


def _dump(payload: Mapping[str, object]) -> bytes:
    text = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
    return f"{text}\n".encode()


def report_bytes(report: SourceEquivalenceReport, evidence: Mapping[str, object]) -> bytes:
    """Serializa o relatorio por dataset em JSON de chaves ordenadas.

    Args: report: comparacoes do dataset. evidence: proveniencia, saidas e hashes do candidato.
    Returns: bytes canonicos (UTF-8, chaves ordenadas, newline final).
    """
    counts = [item.status.value for item in report.comparisons]
    payload: dict[str, object] = {
        **evidence,
        "tenant_id": report.tenant_id,
        "dataset": report.dataset,
        "source_types": list(report.source_types),
        "competencia": report.competencia,
        "legacy_sha256": report.legacy_sha256,
        "candidate_version_id": report.candidate_version_id,
        "accepted": report.accepted,
        "summary": {status.value: counts.count(status.value) for status in ComparisonStatus},
        "comparisons": [asdict(item) for item in report.comparisons],
    }
    return _dump(payload)


def aggregate_bytes(payload: Mapping[str, object]) -> bytes:
    """Serializa o agregado em JSON de chaves ordenadas.

    Args: payload: hashes dos relatorios, cobertura, falhas, contrato e commit.
    Returns: bytes canonicos (UTF-8, chaves ordenadas, newline final).
    """
    return _dump(payload)


def sha256_hex(data: bytes) -> str:
    """Args: data: bytes a resumir. Returns: SHA-256 hexadecimal minusculo."""
    return sha256(data).hexdigest()
