"""Contrato e comparacao exata de equivalencia entre oraculo congelado e candidato."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from functools import lru_cache
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

from data_processor.migration.flatten import APPLIED_NORMALIZATIONS, Leaf

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from pathlib import Path

Absent = Literal["legacy", "candidate"]
LegacyForm = Literal["text", "utc_instant_z", "utc_instant_offset"]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CheckKind = Literal[
    "candidate_equals_context",
    "candidate_equals_context_by_legacy",
    "candidate_is_text_of_legacy_int",
]

_FORBIDDEN_KEY = re.compile(r"tolerance|percent|epsilon", re.IGNORECASE)
_ROW_FIELD_PATTERN = re.compile(r"[^:\[\]]+::\[\*\][^*?\[\]]+")
_ROW_PREFIX = re.compile(r"[^:]+::\[[^\]]*\](?=\.)")
_UTC_INSTANT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(Z|\+00:00)")
_INSTANT_SUFFIX = {"utc_instant_z": "Z", "utc_instant_offset": "+00:00"}
_GLOB_TOKEN = re.compile(r"\[\*\]|.", re.DOTALL)
_GLOB_REGEX = {"[*]": r"\[[^\]]*\]", "*": ".*", "?": "."}
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
    legacy_value: Leaf
    candidate_value: Leaf
    status: ComparisonStatus
    rule_id: str | None
    absent: Absent | None = None


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
    legacy_form: LegacyForm | None = None
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def _context_matches_check(self) -> Self:
        if not isinstance(self.context, _CONTEXT_SHAPE[self.check]):
            raise ValueError(f"rule_context_invalid rule_id={self.rule_id}")
        return self

    @model_validator(mode="after")
    def _legacy_form_matches_check(self) -> Self:
        needs_form = self.check == "candidate_equals_context" and not self.absent_ok
        if (self.legacy_form is not None) != needs_form:
            raise ValueError(f"rule_legacy_form_invalid rule_id={self.rule_id}")
        return self

    @model_validator(mode="after")
    def _absent_ok_declares_row_fields(self) -> Self:
        if self.absent_ok and not all(_ROW_FIELD_PATTERN.fullmatch(p) for p in self.metrics):
            raise ValueError(f"rule_absent_ok_pattern_invalid rule_id={self.rule_id}")
        return self


class EquivalenceContract(_Model):
    contract_version: Literal[1]
    normalizations: tuple[str, ...]
    clock: datetime
    datasets: dict[str, DatasetSpec]
    rules: tuple[Rule, ...]

    @field_validator("normalizations")
    @classmethod
    def _normalizations_are_the_applied_set(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if tuple(sorted(value)) != APPLIED_NORMALIZATIONS:
            raise ValueError("normalizations_invalid")
        return value

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


@dataclass(frozen=True, slots=True)
class _Observation:
    metric: str
    legacy: Leaf
    candidate: Leaf
    absent: Absent | None


type _Predicate = Callable[[Rule, _Observation, Mapping[str, str]], bool]


def _scalar_equal(left: Leaf, right: Leaf) -> bool:
    return type(left) is type(right) and left == right


def _glob_part(token: str) -> str:
    return _GLOB_REGEX.get(token, re.escape(token))


@lru_cache(maxsize=256)
def _glob_regex(pattern: str) -> re.Pattern[str]:
    parts = [_glob_part(token) for token in _GLOB_TOKEN.findall(pattern)]
    return re.compile("".join(parts), re.DOTALL)


def _covers(rule: Rule, metric: str) -> bool:
    return any(_glob_regex(pattern).fullmatch(metric) for pattern in rule.metrics)


def _candidate_is_context(key: str, seen: _Observation, context: Mapping[str, str]) -> bool:
    resolved = key.replace("{doc}", seen.metric.partition("::")[0])
    return resolved in context and _scalar_equal(seen.candidate, context[resolved])


def _is_utc_instant(text: str, suffix: str) -> bool:
    found = _UTC_INSTANT.fullmatch(text)
    if found is None or found.group(1) != suffix:
        return False
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return False
    return True


def _has_form(value: Leaf, form: LegacyForm | None) -> bool:
    if not isinstance(value, str) or not value:
        return False
    return form == "text" or _is_utc_instant(value, _INSTANT_SUFFIX[cast("LegacyForm", form)])


def _equals_context(rule: Rule, seen: _Observation, context: Mapping[str, str]) -> bool:
    legacy_fits = rule.absent_ok or _has_form(seen.legacy, rule.legacy_form)
    return legacy_fits and _candidate_is_context(cast("str", rule.context), seen, context)


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
    name: str, legacy: Mapping[str, Leaf], candidate: Mapping[str, Leaf]
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


def _row_of(metric: str) -> str | None:
    found = _ROW_PREFIX.match(metric)
    return found.group() if found else None


def _required_fields(rules: Sequence[Rule], rows: Iterable[str]) -> set[str]:
    required: set[str] = set()
    for rule in rules:
        for pattern in rule.metrics if rule.absent_ok else ():
            doc_glob, _, rest = pattern.partition("::")
            matcher = _glob_regex(doc_glob)
            tail = rest.removeprefix("[*]")
            required.update(
                f"{row}{tail}" for row in rows if matcher.fullmatch(row.partition("::")[0])
            )
    return required


def _missing_required(
    rules: Sequence[Rule], items: Sequence[MetricComparison]
) -> tuple[MetricComparison, ...]:
    rows = {row for item in items if item.absent != "candidate" and (row := _row_of(item.metric))}
    known = {item.metric for item in items}
    return tuple(
        MetricComparison(name, None, None, ComparisonStatus.MISMATCH, None, "candidate")
        for name in sorted(_required_fields(rules, rows) - known)
    )


def compare_shadow_run(
    *,
    contract: EquivalenceContract,
    legacy: Mapping[str, Leaf],
    candidate: Mapping[str, Leaf],
    context: Mapping[str, str] | None = None,
) -> tuple[MetricComparison, ...]:
    """Compara metrica a metrica, sem tolerancia; so uma regra do contrato explica diferenca.

    Args: contract: regras aprovadas. legacy/candidate: metricas achatadas. context: valores
        observados do candidato que as regras podem exigir (run, relogio, ids, hashes).
    Returns: uma comparacao por metrica da uniao, ordenadas pelo nome; campo exigido por regra
        `absent_ok` e ausente numa linha do candidato vira MISMATCH.
    """
    known: Mapping[str, str] = {} if context is None else context
    names = sorted(legacy.keys() | candidate.keys())
    items = tuple(
        _compare_metric(contract.rules, known, _observe(name, legacy, candidate))
        for name in names
    )
    missing = _missing_required(contract.rules, items)
    return tuple(sorted((*items, *missing), key=lambda item: item.metric))
