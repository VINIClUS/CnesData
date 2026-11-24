"""Insumos sintéticos dos -record de modo: activation, close-drain, mode-input e request ABI3."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.promotion import _adapter_abi as abi
from scripts.promotion import promote_api
from scripts.promotion.lifecycle_inputs import mode_canary_input
from scripts.promotion.mode_canary_contract import parse_context_manifest
from scripts.promotion.promote_ready import machine_arn
from tests.production.delivery.test_adapter_handlers import API, SHA
from tests.production.delivery.test_phase_b import WINDOWS
from tests.production.delivery.test_promote_close import ACCOUNT, OWNER, canonical, sha
from tests.production.delivery.test_promote_close import _activation as activation_bytes

NOW = datetime(2026, 11, 13, 12, 5, tzinfo=UTC)
FIXED = "2026-11-13T12:35:00Z"
CONTEXT_DEADLINE = "2026-11-13T13:00:00Z"
OWNER_SHA = OWNER["owner_sha256"]
ACTIVATION = activation_bytes()
CANARY = "c" * 32
TENANT = f"prod-canary-{CANARY}"
FIXTURES = Path(__file__).resolve().parents[3] / "tests/production/fixtures/mode_canary"
REVISIONS: dict[str, str] = json.loads(ACTIVATION)["revisions"]
RECORD_OPS = ("unit-record", "recovery-record", "audit-record")


def runs(mode: str, owner: str = OWNER_SHA) -> list[str]:
    prefixes = ["unit"] if mode == "unit" else ["recovery-a", "recovery-b"]
    return [f"{prefix}-{owner[:16]}" for prefix in prefixes]


def fixture_manifest() -> bytes:
    return (FIXTURES / "fixture-manifest.json").read_bytes()


def close_drain(**common: Any) -> bytes:
    values = {"repository": "VINIClUS/CnesData", "release_id": "r" + "1" * 31,
              "source_sha": SHA, "candidate_sha256": "b" * 64, "canary_id": CANARY,
              "activation_sha256": sha(ACTIVATION), "promotion_id": "r" + "1" * 31,
              "promotion_generation": 7, "caller": {}} | common
    return canonical({
        "version": 2, "kind": "close-drain", "common": values, "fence": {},
        "close_confirmed_at": "2026-11-13T12:00:00Z", "drain_observed_at": "2026-11-13T12:00:05Z",
        "frozen_membership_sha256": "9" * 64, "members": [], "proofs": [], "permit": None,
        "input_expirations": [], "fixed_deadline": FIXED, "decision": None, "windows": WINDOWS,
    })


def resources(**scope: Any) -> dict[str, Any]:
    return {
        "table_arn": f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/cnesdata-prod-control-plane",
        "index_arn": f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/cnesdata-prod-control-plane/"
                     "index/gsi5",
        "data_bucket_arn": "arn:aws:s3:::cnesdata-prod-data",
        "audit_bucket_arn": "arn:aws:s3:::cnesdata-prod-audit",
        "cluster_arn": f"arn:aws:ecs:us-east-2:{ACCOUNT}:cluster/cnesdata-prod",
        "machine_arn": machine_arn(ACCOUNT),
        "execution_role_arn": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-exec",
        "log_group_arn": f"arn:aws:logs:us-east-2:{ACCOUNT}:log-group:cnesdata-prod",
        "revisions": dict(REVISIONS),
        "task_role_arns": {k: f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-{k}"
                           for k in ("audit", "recovery", "unit")},
        "network": {"public_subnet_ids": ["subnet-1"], "security_group_id": "sg-1"},
        "schedule_arns": {k: f"arn:aws:scheduler:us-east-2:{ACCOUNT}:schedule/p-{k}/p-{k}"
                          for k in ("audit", "recovery")},
        "alarm_arns": {k: f"arn:aws:cloudwatch:us-east-2:{ACCOUNT}:alarm:p-{k}"
                       for k in ("audit_outbox_backlog", "audit_repeated_failure",
                                 "recovery_repeated_failure")},
        "scope": {"audit_object_keys": ["a-1"], "output_object_keys": ["o-1"],
                  "raw_object_keys": ["r-1"], "run_ids": runs("unit") + runs("recovery"),
                  "tenant_id": TENANT} | scope,
    }


def mode_input(owner: dict[str, Any] = OWNER, scope: dict[str, Any] | None = None,
               **manifest: Any) -> bytes:
    values = resources(**(scope or {}))
    fields = {
        "schema_version": 2, "context_kind": "promotion",
        "owner": {key: owner[key] for key in ("repository", "run_id", "run_attempt", "generation",
                                              "owner_sha256")},
        "product_source_sha": SHA, "product_source_closure_sha256": "3" * 64,
        "shared_source_sha": "b" * 40, "shared_source_closure_sha256": "4" * 64,
        "release_id": "r" + "1" * 31, "activation_sha256": sha(ACTIVATION),
        "resources_sha256": sha(canonical(values)), "fixture_manifest_sha256":
        sha(fixture_manifest()), "scenario_plan_sha256": "7" * 64,
        "predecessor_terminal_sha256": None, "admitted_at": "2026-11-13T11:50:00Z",
        "deadline": CONTEXT_DEADLINE,
    } | manifest
    text = canonical(fields).decode()
    wrapper = parse_context_manifest({"canonical_utf8": text, "sha256": sha(text.encode())})
    principal = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-application"
    return canonical(mode_canary_input(wrapper, values, principal, "2026-11-13T14:00:00Z"))


def record_inputs(mode: str, **changes: bytes) -> dict[str, bytes]:
    inputs = {"activation": ACTIVATION, "close-drain": close_drain(),
              "fixture-manifest": fixture_manifest(), "mode-input": mode_input()}
    if mode == "audit":
        inputs["mode-seed"] = seed_manifest()
    return inputs | changes


def seed_manifest(owner: str = OWNER_SHA, tenant: str = TENANT, **changes: Any) -> bytes:
    return canonical({
        "schema": "cnesdata.mode-canary-seed/v1", "owner_sha256": owner, "tenant_id": tenant,
        "run_ids": runs("recovery", owner), "fixture_manifest_sha256": sha(fixture_manifest()),
        "items": [], "objects": [],
    } | changes)


def request(name: str, inputs: dict[str, bytes], owner: dict[str, Any] = OWNER,
            deadline: datetime | None = None) -> dict[str, Any]:
    epoch = int(NOW.timestamp())
    end = deadline or NOW + timedelta(seconds=120)
    stage = API | {"stage": abi.MODE_OPERATIONS[name][1], "close": epoch - 120,
                   "fixed_deadline": epoch + 1800, "admitted_at": epoch - 5,
                   "deadline": int(end.timestamp())}
    return {"abi_version": 3, "operation": name, "repository": owner["repository"],
            "source_sha": SHA, "owner": owner, "checkpoint_sha256": API["checkpoint_sha256"],
            "activation_sha256": sha(ACTIVATION), "stage": stage,
            "inputs": {key: {"sha256": sha(raw), "size": len(raw)}
                       for key, raw in sorted(inputs.items())}}


class PureContext:
    """Context que recusa qualquer chamada: só a observação final do journal é admitida."""

    def __init__(self, inputs: dict[str, bytes]) -> None:
        self.inputs = inputs
        self.observed: list[tuple[str, bytes]] = []

    def read(self, name: str) -> bytes:
        return self.inputs[name]

    def guard(self) -> None:
        raise AssertionError("guard")

    def observe(self, kind: str, raw: bytes) -> str:
        self.observed.append((kind, raw))
        return sha(raw)

    def call(self, role: str, service: str, operation: str, parameters: dict[str, Any]) -> Any:
        raise AssertionError(f"call {role} {service} {operation}")

    def evidence_snapshot(self) -> dict[str, Any]:
        return {"call_receipts_sha256": "e" * 64, "pending": [], "unknown": []}


def execute(mp: Any, name: str, inputs: dict[str, bytes], **options: Any) -> dict[str, Any]:
    now = options.pop("now", NOW)
    mp.setattr(promote_api, "CLOCK", lambda: now)
    context = PureContext(inputs)
    result = promote_api.execute_shared_operation(name, request(name, inputs, **options), context)
    [(kind, raw)] = context.observed
    return {"kind": kind, "raw": raw, "result": result}
