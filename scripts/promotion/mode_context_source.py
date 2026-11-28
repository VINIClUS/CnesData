"""mode-canary.json do static-inventory: secret do dono, marker do estágio e valores de S."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from cnes_domain.orchestration.mode_canary import is_canonical_mode_canary_tenant
from scripts.promotion._canonical import canonical_bytes
from scripts.promotion.lifecycle_inputs import mode_canary_input
from scripts.promotion.mode_canary_contract import (
    REPOSITORY,
    SESSION_OPERATIONS,
    parse_context_manifest,
)
from scripts.promotion.mode_scope import mode_scope

if TYPE_CHECKING:
    from scripts.promotion.closed_ready_probe import Target

ROOT: Final = Path(__file__).resolve().parents[2]
FIXTURE_MANIFEST: Final = "tests/production/fixtures/mode_canary/fixture-manifest.json"
STAGE: Final = "application-prepare"
PROMOTION_CONTEXT_SECONDS: Final = 1200
# The keeper job holding the lease runs on a GitHub-hosted runner, so it cannot outlive 6 hours.
MODE_ENABLED_SECONDS: Final = 21600
CONTEXT_FIELDS: Final = frozenset({
    "shared_source_sha", "product_source_closure_sha256", "shared_source_closure_sha256",
    "release_id", "activation_sha256", "application_principal_arn", "resources",
    "distribution_arn",
})
MARKER_FIELDS: Final = frozenset({
    "version", "owner_sha256", "generation", "run_attempt", "stage", "sequence",
    "checkpoint_sha256", "admitted_at", "deadline",
})
_SHAPES: Final = {
    "shared_source_sha": re.compile(r"[0-9a-f]{40}"),
    "product_source_closure_sha256": re.compile(r"[0-9a-f]{64}"),
    "shared_source_closure_sha256": re.compile(r"[0-9a-f]{64}"),
    "activation_sha256": re.compile(r"[0-9a-f]{64}"),
    "release_id": re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}"),
    "application_principal_arn": re.compile(r"arn:aws:iam::([0-9]{12}):role/[A-Za-z0-9+=,.@_-]+"),
}
_DISTRIBUTION: Final = re.compile(r"arn:aws:cloudfront::([0-9]{12}):distribution/[A-Z0-9]+")
_HEX64: Final = re.compile(r"[0-9a-f]{64}")
_FORMAT: Final = "%Y-%m-%dT%H:%M:%SZ"


def _refuse(field: str) -> ValueError:
    return ValueError(f"mode_context field={field}")


def _matches(pattern: re.Pattern[str], value: Any) -> re.Match[str] | None:
    return pattern.fullmatch(value) if type(value) is str else None


def _check_resources(resources: Any) -> None:
    scope = resources.get("scope") if type(resources) is dict else None
    if (type(scope) is not dict or set(scope) != {"tenant_id"}
            or not is_canonical_mode_canary_tenant(str(scope["tenant_id"]))):
        raise _refuse("resources")


def mode_context(raw: str) -> dict[str, Any]:
    """Returns: o secret CNESDATA_MODE_CONTEXT, conjunto fechado e formas conferidos.

    Raises: ValueError: JSON inválido, campo desconhecido ou ausente, ou forma inválida.
    """
    try:
        value = json.loads(raw)
    except ValueError as error:
        raise _refuse("fields") from error
    if type(value) is not dict or set(value) != CONTEXT_FIELDS:
        raise _refuse("fields")
    for field, pattern in _SHAPES.items():
        if _matches(pattern, value[field]) is None:
            raise _refuse(field)
    _check_resources(value["resources"])
    account = _SHAPES["application_principal_arn"].fullmatch(value["application_principal_arn"])
    distribution = _matches(_DISTRIBUTION, value["distribution_arn"])
    if distribution is None or account is None or distribution[1] != account[1]:
        raise _refuse("distribution_arn")
    return value


def _moment(value: Any) -> datetime:
    if type(value) is not str:
        raise _refuse("marker")
    try:
        return datetime.strptime(value, _FORMAT).replace(tzinfo=UTC)
    except ValueError as error:
        raise _refuse("marker") from error


def _positive(value: Any) -> bool:
    return type(value) is int and value >= 1


def stage_marker_name(target: Target) -> str:
    """Returns: Nome do marker de abertura do estágio application-prepare do run."""
    return f"cnesdata-stage-open-{STAGE}-{target.run_id}-{target.attempt}"


def stage_marker(raw: bytes, target: Target) -> dict[str, Any]:
    """Returns: index.json do marker do estágio, do mesmo attempt e com janela válida.

    Raises: ValueError: marker de outro estágio ou attempt, campos ou janela inválidos.
    """
    try:
        value = json.loads(raw)
    except ValueError as error:
        raise _refuse("marker") from error
    if (type(value) is not dict or set(value) != MARKER_FIELDS or value["version"] != 1
            or value["stage"] != STAGE or value["run_attempt"] != target.attempt
            or not (_positive(value["generation"]) and _positive(value["sequence"]))
            or _matches(_HEX64, value["owner_sha256"]) is None):
        raise _refuse("marker")
    if _moment(value["admitted_at"]) >= _moment(value["deadline"]):
        raise _refuse("marker")
    return value


def scenario_plan_sha256() -> str:
    """Returns: sha256 do plano de cenários de promoção commitado em S."""
    operations = sorted(name for name, spec in SESSION_OPERATIONS.items() if spec[3] == "promotion")
    plan = {"context_kind": "promotion", "operations": operations}
    return hashlib.sha256(canonical_bytes(plan)).hexdigest()


def _manifest(context: dict[str, Any], owner: dict[str, Any], source_sha: str,
              window: tuple[datetime, datetime]) -> dict[str, Any]:
    fixture = (ROOT / FIXTURE_MANIFEST).read_bytes()
    sources = ("shared_source_sha", "product_source_closure_sha256",
               "shared_source_closure_sha256", "release_id", "activation_sha256")
    return {
        "schema_version": 2, "context_kind": "promotion", "owner": owner,
        "product_source_sha": source_sha, **{name: context[name] for name in sources},
        "resources_sha256": hashlib.sha256(canonical_bytes(context["resources"])).hexdigest(),
        "fixture_manifest_sha256": hashlib.sha256(fixture).hexdigest(),
        "scenario_plan_sha256": scenario_plan_sha256(), "predecessor_terminal_sha256": None,
        "admitted_at": window[0].strftime(_FORMAT), "deadline": window[1].strftime(_FORMAT),
    }


def mode_canary_file(context: dict[str, Any], marker: dict[str, Any], target: Target,
                     source_sha: str) -> bytes:
    """Returns: mode-canary.json canônico, validado pelo oráculo de produto.

    Raises: ValueError: recursos, principal ou sessões fora do contrato do produto.
    """
    admitted = _moment(marker["admitted_at"])
    deadline = min(_moment(marker["deadline"]),
                   admitted + timedelta(seconds=PROMOTION_CONTEXT_SECONDS))
    owner = {"repository": REPOSITORY, "run_id": target.run_id, "run_attempt": target.attempt,
             "generation": marker["generation"], "owner_sha256": marker["owner_sha256"]}
    scope = mode_scope(context["resources"]["scope"]["tenant_id"], marker["owner_sha256"])
    resources = context["resources"] | {"scope": scope}
    manifest_fields = _manifest(context | {"resources": resources}, owner, source_sha,
                                (admitted, deadline))
    text = canonical_bytes(manifest_fields).decode()
    manifest = parse_context_manifest(
        {"canonical_utf8": text, "sha256": hashlib.sha256(text.encode()).hexdigest()})
    enabled = admitted + timedelta(seconds=MODE_ENABLED_SECONDS)
    value = mode_canary_input(manifest, resources, context["application_principal_arn"],
                              enabled.strftime(_FORMAT))
    return canonical_bytes(value)
