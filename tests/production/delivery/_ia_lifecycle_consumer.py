"""Espelho das checagens puras do consumidor de lifecycle do infra-ansible, fixado por commit.

static_promotion e seus nove campos são contrato do produto, não do consumidor.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

# infra-ansible 11251c77 (X6 mergeado):
# tools/cnesdata_delivery/{contract,lifecycle_contract,lifecycle_runtime}.py.
IA_PIN = "11251c77e503be406beaa74985fcb0a5eb778826"
IA_MIRRORED_SOURCES = {
    "tools/cnesdata_delivery/contract.py": (
        "315d42b9a6c43ebac822bb0b73e4ac853abaf724eecaccd737024df39e4afb60"
    ),
    "tools/cnesdata_delivery/lifecycle_contract.py": (
        "2f689fcd2e0c41525ba7e002e45289071af01e1c5c56b9333f23d6fb8424e63f"
    ),
    "tools/cnesdata_delivery/lifecycle_runtime.py": (
        "edf014783e83833fc801ad9107f1739229d5b0460f246d65552bad5fd4760873"
    ),
}
REPOSITORY = "VINIClUS/CnesData"
APPLICATION_CONTEXTS = {"promotion": "promotion", "mode-readiness-drill": "readiness"}
APPLICATION_CALLERS = {
    purpose: {"repository": REPOSITORY, "workflow_path": ".github/workflows/" + workflow,
              "ref": "refs/heads/main", "environment": "production"}
    for purpose, workflow in (("promotion", "promote-production.yml"),
                              ("mode-readiness-drill", "readiness-production.yml"))
}
CALLER_BINDING_FIELDS = (
    "application_purpose context_kind repository workflow_path ref environment source_sha "
    "run_id run_attempt producer_workflow_sha producer_source_closure_sha256"
)
REQUEST_FIELDS = (
    "schema_version repository source_sha step payloads application_purpose caller_binding_sha256"
)
STATIC_BINDING_FIELDS = (
    "release_id run_id run_attempt generation product_source_sha shared_source_sha "
    "static_plan_sha256"
)
STATIC_PLAN_SCHEMA = "cnesdata.static-plan/v1"
STATIC_PLAN_FIELDS = (
    "schema bucket distribution_arn inventory_sha256 candidate_keys rollback_versions "
    "retire_versions multipart_keys"
)
STATIC_ENTRYPOINTS = ("index.html", "theme-init.js")
PRODUCT_STATIC_PROMOTION_FIELDS = (
    "mode application_promotion binding candidate_keys rollback_versions retire_versions "
    "multipart_keys inventory_sha256 enabled_until"
)


class Refused(ValueError):
    """Recusa do consumidor com a mesma mensagem fechada do original."""


def require(condition: Any, message: str) -> None:
    if not condition:
        raise Refused(message)


def closed(value: Any, fields: str) -> Any:
    require(type(value) is dict and set(value) == set(fields.split()), "closed object fields")
    return value


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest(value: Any, length: int = 64) -> Any:
    require(type(value) is str and re.fullmatch(f"[0-9a-f]{{{length}}}", value), "digest")
    return value


def integer(value: Any, minimum: int = 1, maximum: int = 2**53 - 1) -> Any:
    require(type(value) is int and minimum <= value <= maximum, "integer range")
    return value


def canonical(value: Any) -> bytes:
    def visit(item: Any) -> str:
        if item is None or type(item) is bool:
            return json.dumps(item)
        if type(item) is str:
            return json.dumps(item, ensure_ascii=False)
        if type(item) is int:
            return str(item)
        if type(item) is list:
            return "[" + ",".join(visit(child) for child in item) + "]"
        require(type(item) is dict and all(type(key) is str for key in item), "JSON object key")
        return "{" + ",".join(f"{visit(key)}:{visit(item[key])}" for key in sorted(item)) + "}"

    return (visit(value) + "\n").encode("utf-8")


def loads(data: bytes, *, limit: int, require_canonical: bool = True) -> Any:
    require(type(data) is bytes and len(data) <= limit and not data.startswith(b"\xef\xbb\xbf"),
            "JSON size/BOM")
    value = json.loads(data.decode("utf-8"), parse_float=lambda token: require(False, "number"),
                       parse_constant=lambda token: require(False, "nonfinite JSON"))
    require(not require_canonical or canonical(value) == data, "noncanonical JSON bytes")
    return value


def application_caller_binding(
    evidence: dict[str, Any], producer: dict[str, str]
) -> dict[str, Any]:
    closed(evidence, CALLER_BINDING_FIELDS + " event fragment_version")
    purpose = evidence["application_purpose"]
    require(purpose in APPLICATION_CONTEXTS
            and evidence["context_kind"] == APPLICATION_CONTEXTS[purpose],
            "application purpose/context")
    require(type(evidence["fragment_version"]) is int and evidence["fragment_version"] == 4
            and evidence["event"] == "workflow_dispatch",
            "application original manual schema4 caller")
    require(all(evidence[key] == value for key, value in APPLICATION_CALLERS[purpose].items()),
            "application exact purpose caller")
    require(evidence["producer_workflow_sha"] == digest(producer["workflow_sha"], 40)
            and evidence["producer_source_closure_sha256"]
            == digest(producer["source_closure_sha256"]), "application original producer closure")
    digest(evidence["source_sha"], 40)
    for name in ("run_id", "run_attempt"):
        integer(evidence[name])
    return {key: evidence[key] for key in CALLER_BINDING_FIELDS.split()}


def authentic_caller(binding: dict[str, Any], producer: dict[str, str]) -> dict[str, Any]:
    return application_caller_binding({**binding, "event": "workflow_dispatch",
                                       "fragment_version": 4}, producer)


def admit_request(raw: bytes, raw_sha256: str, governance: dict[str, Any], caller: dict[str, Any]
                  ) -> dict[str, Any]:
    require(sha(raw) == digest(raw_sha256), "request bytes")
    request = loads(raw, limit=65536)
    closed(request, REQUEST_FIELDS)
    require(type(request["schema_version"]) is int and request["schema_version"] == 1
            and request["repository"] == REPOSITORY
            and request["source_sha"] == governance["source_sha"], "lifecycle request")
    step: Any = governance["lifecycle_steps"].get(request["step"])
    require(type(step) is dict and step["role"] == governance["role"]
            and step["purpose"] == governance["purpose"], "governed step")
    payloads: Any = request["payloads"]
    jobs: Any = step["input_jobs"]
    require(type(payloads) is dict, "complete payload inventory")
    require(set(payloads) == set(jobs), "complete payload inventory")
    for name, named in payloads.items():
        closed(named, "step")
        require(named["step"] == jobs[name]["step"], "payload producer step")
    require(request["application_purpose"] == caller["application_purpose"]
            and request["caller_binding_sha256"] == sha(canonical(caller)),
            "original requested application caller")
    return request


def static_session_binding(binding: dict[str, Any]) -> dict[str, Any]:
    closed(binding, STATIC_BINDING_FIELDS)
    require(type(binding["release_id"]) is str
            and re.fullmatch(r"[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?", binding["release_id"]),
            "static session release id")
    for key in ("run_id", "run_attempt", "generation"):
        integer(binding[key])
    for key in ("product_source_sha", "shared_source_sha"):
        digest(binding[key], 40)
    digest(binding["static_plan_sha256"])
    return binding


def _static_key(key: Any) -> Any:
    require(type(key) is str and len(key) <= 1024
            and re.fullmatch(r"assets/[A-Za-z0-9._/-]+|index\.html|theme-init\.js", key)
            is not None and all(part not in {"", ".", ".."} for part in key.split("/")),
            "static key")
    return key


def _static_refs(refs: Any) -> set[tuple[str, str]]:
    require(type(refs) is list, "static version refs")
    for ref in refs:
        closed(ref, "key version_id")
        _static_key(ref["key"])
        require(type(ref["version_id"]) is str and ref["version_id"] != "null"
                and re.fullmatch(r"[A-Za-z0-9._~+/=-]+", ref["version_id"]) is not None,
                "static version id")
    require(len({(ref["key"], ref["version_id"]) for ref in refs}) == len(refs),
            "static version refs distinct")
    return {(ref["key"], ref["version_id"]) for ref in refs}


def static_plan(raw: bytes, *, bucket: str) -> dict[str, Any]:
    plan = loads(raw, limit=1024**2)
    closed(plan, STATIC_PLAN_FIELDS)
    require(plan["schema"] == STATIC_PLAN_SCHEMA and plan["bucket"] == bucket, "static plan bucket")
    require(type(plan["distribution_arn"]) is str and re.fullmatch(
        r"arn:aws:cloudfront::[0-9]{12}:distribution/[A-Z0-9]+", plan["distribution_arn"]
    ) is not None, "static plan distribution")
    digest(plan["inventory_sha256"])
    for field, minimum in (("candidate_keys", 1), ("multipart_keys", 0)):
        keys = plan[field]
        require(type(keys) is list and len(keys) >= minimum and all(map(_static_key, keys))
                and keys == sorted(set(keys)), "static keys sorted distinct")
    rollback = _static_refs(plan["rollback_versions"])
    retire = _static_refs(plan["retire_versions"])
    require([ref["key"] for ref in plan["rollback_versions"]] in ([], list(STATIC_ENTRYPOINTS)),
            "static plan rollback pair")
    require(not rollback & retire, "static plan retire/rollback overlap")
    return plan


def _bootstrap_static_promotion(value: dict[str, Any]) -> dict[str, Any]:
    require(value["binding"].get("static_plan_sha256") is None, "static bootstrap plan binding")
    static_session_binding(value["binding"] | {"static_plan_sha256": "0" * 64})
    require(value["inventory_sha256"] is None, "static bootstrap inventory binding")
    require(all(value[key] == [] for key in (
        "candidate_keys", "rollback_versions", "retire_versions", "multipart_keys")),
        "static bootstrap lists")
    return value


def product_static_promotion(
    value: dict[str, Any], plan_raw: bytes | None, bucket: str
) -> dict[str, Any]:
    closed(value, PRODUCT_STATIC_PROMOTION_FIELDS)
    require(value["mode"] in {"observer-bootstrap", "operational"}, "static mode")
    if value["mode"] == "observer-bootstrap":
        require(plan_raw is None, "static bootstrap carries no plan")
        return _bootstrap_static_promotion(value)
    if plan_raw is None:
        raise Refused("static operational plan")
    plan = static_plan(plan_raw, bucket=bucket)
    static_session_binding(value["binding"])
    require(value["binding"]["static_plan_sha256"] == sha(plan_raw), "static plan binding")
    require(value["inventory_sha256"] == plan["inventory_sha256"], "static inventory binding")
    require(all(value[key] == plan[key] for key in (
        "candidate_keys", "rollback_versions", "retire_versions", "multipart_keys")),
        "static plan fields")
    return value


def _product_context_bytes(value: Any) -> bytes:
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False) + "\n").encode("utf-8")
    require(len(raw) <= 131072, "mode context full request bound")
    return raw


def decode_mode_context(inputs_raw: bytes) -> tuple[dict[str, Any], bytes]:
    inputs: Any = loads(inputs_raw, limit=131072, require_canonical=False)
    require(_product_context_bytes(inputs) == inputs_raw and type(inputs) is dict,
            "mode context original request bytes")
    mode: Any = inputs.get("mode_canary")
    require(type(mode) is dict, "mode context original request bytes")
    wrapper = mode.get("context_manifest")
    closed(wrapper, "canonical_utf8 sha256")
    require(type(wrapper["canonical_utf8"]) is str, "mode context UTF-8 text")
    raw = wrapper["canonical_utf8"].encode("utf-8")
    require(sha(raw) == digest(wrapper["sha256"]), "mode context inner bytes digest")
    manifest: Any = loads(raw, limit=131072, require_canonical=False)
    require(type(manifest) is dict and _product_context_bytes(manifest) == raw,
            "mode context canonical inner bytes")
    return manifest, raw
