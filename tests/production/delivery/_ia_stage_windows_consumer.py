"""Espelho das checagens de janelas governadas do consumidor do infra-ansible, fixado por commit."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

# infra-ansible 54b381c3 (branch do P36) tools/cnesdata_delivery/{lifecycle_contract,host_gate}.py.
IA_PIN = "54b381c3dec4469a3bcd55a3e758291814646b07"
IA_MIRRORED_SOURCES = {
    "tools/cnesdata_delivery/lifecycle_contract.py": (
        "69e91402201cc5632e1cc7f134e0d70f162837003eba428ca9e097b87c13687c"
    ),
    "tools/cnesdata_delivery/host_gate.py": (
        "6fff0ff74c34e8cc02d3d3d3f3736354d61579be959314e46cbdf2da4bf2c99a"
    ),
}
ADAPTER_ABI_VERSION = 3
ADAPTER_STAGE_FIELDS = (
    "close fixed_deadline input_deadline stage admitted_at deadline decision preparation_deadline "
    "sequence checkpoint_sha256 windows"
)
STAGES = {"phase-b": (360, 720), "api": (180, 360), "static": (180, 360), "unit": (120, 360),
          "phase-c-recovery": (240, 660), "phase-c-audit": (240, 660), "final": (60, 180)}
FINAL_RESERVE = 420
STAGE_SUM_CEILING = 1830
CLOSE_DRAIN_FIELDS = (
    "version kind common fence close_confirmed_at drain_observed_at frozen_membership_sha256 "
    "members proofs permit input_expirations fixed_deadline decision windows"
)
CLOSE_EXPIRATION_KINDS = frozenset({"envelope", "artifact", "platform", "cost", "credential",
                                    "activation", "plan"})
READY_STAGE_FIELDS = "name admitted_at deadline later_seconds"


class Refused(ValueError):
    """Recusa do consumidor com a mesma mensagem do original."""


def require(condition: Any, message: str) -> None:
    if not condition:
        raise Refused(message)


def closed(value: Any, fields: str) -> Any:
    require(type(value) is dict and set(value) == set(fields.split()), "closed object fields")
    return value


def digest(value: Any) -> Any:
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value), "digest")
    return value


def instant(value: Any) -> float:
    require(type(value) is str and re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", value),
            "UTC timestamp")
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).timestamp()


def loads(raw: bytes) -> Any:
    value = json.loads(raw.decode("utf-8"))
    canonical = (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
                 + "\n").encode("utf-8")
    require(canonical == raw, "canonical JSON")
    return value


def _window_keys(value: Any) -> str:
    return ",".join(map(str, value)) if type(value) is dict else type(value).__name__


def governed_windows(value: Any) -> Any:
    closed(value, "stages fixed_deadline_seconds")
    stages, fixed = value["stages"], value["fixed_deadline_seconds"]
    require(type(stages) is dict and set(stages) == set(STAGES),
            "stage_windows keys=" + _window_keys(stages))
    for name, (low, high) in STAGES.items():
        seconds = stages[name]
        require(type(seconds) is int and low <= seconds <= high,
                f"stage window stage={name} value={seconds} min={low} max={high}")
    total = sum(stages.values())
    require(total <= STAGE_SUM_CEILING, f"stage windows sum={total} ceiling={STAGE_SUM_CEILING}")
    require(type(fixed) is int and fixed == total + FINAL_RESERVE,
            f"fixed_deadline_seconds value={fixed} expected={total + FINAL_RESERVE}")
    return value


def later_seconds(windows: dict[str, Any], stage: str) -> int:
    order = list(STAGES)
    return sum(windows["stages"][name] for name in order[order.index(stage) + 1:])


def fixed_deadline(close: int, input_deadline: int, windows: dict[str, Any]) -> int:
    return min(close + windows["fixed_deadline_seconds"], input_deadline)


def adapter_stage(stage: Any, window: dict[str, Any]) -> Any:
    closed(stage, ADAPTER_STAGE_FIELDS)
    require(stage == {key: window[key] for key in ADAPTER_STAGE_FIELDS.split()},
            "adapter original stage")
    governed_windows(stage["windows"])
    return stage


def close_window(previous: dict[str, Any], raw: bytes, *, infra_bound: int,
                 now: int) -> dict[str, Any]:
    value = closed(loads(raw), CLOSE_DRAIN_FIELDS)
    require(type(value["version"]) is int and value["version"] == 2
            and value["kind"] == "close-drain", "close-drain observation version/kind")
    require(value["windows"] == governed_windows(previous["windows"]),
            "close-drain windows differ from keeper windows")
    require(value["permit"] is None and value["decision"] is None, "original idle permit/decision")
    expirations: list[Any] = value["input_expirations"]
    require(type(expirations) is list and expirations, "original input expirations")
    keys: list[tuple[Any, Any]] = []
    for expiry in expirations:
        closed(expiry, "kind object_sha256 expires_at")
        require(expiry["kind"] in CLOSE_EXPIRATION_KINDS, "original input expiration kind")
        keys.append((expiry["kind"], digest(expiry["object_sha256"])))
    require(keys == sorted(set(keys)), "unique original input expirations")
    close = int(instant(value["close_confirmed_at"]))
    input_deadline = int(min(instant(expiry["expires_at"]) for expiry in expirations))
    fixed = fixed_deadline(close, input_deadline, previous["windows"])
    require(instant(value["fixed_deadline"]) == fixed, "original fixed deadline")
    require(type(infra_bound) is int and input_deadline <= infra_bound,
            "infra-known authenticated expiration precedes input deadline")
    require(previous["stage"] == "application-prepare" and previous["close"] is None
            and previous["decision"] is None and previous["admitted_at"] <= close <= now
            and close < previous["preparation_deadline"], "close inside the preparation stage")
    return previous | {"sequence": previous["sequence"] + 1, "close": close,
                       "fixed_deadline": fixed, "input_deadline": input_deadline,
                       "stage": "phase-b", "admitted_at": close,
                       "deadline": min(fixed, close + previous["windows"]["stages"]["phase-b"])}


def api_window(previous: dict[str, Any], *, now: int) -> dict[str, Any]:
    admitted = int(now)
    require(previous["stage"] == "phase-b" and previous["close"] is not None
            and previous["decision"] is None and previous["admitted_at"] <= admitted,
            "API follows the closed phase-b stage")
    return previous | {"sequence": previous["sequence"] + 1, "stage": "api",
                       "admitted_at": admitted,
                       "deadline": min(previous["fixed_deadline"],
                                       admitted + previous["windows"]["stages"]["api"])}


def closed_ready_api_stage(close: dict[str, Any], ready: dict[str, Any]) -> dict[str, Any]:
    windows = governed_windows(close["windows"])
    observed, t0 = instant(ready["observed_at"]), instant(close["close_confirmed_at"])
    fixed = min(t0 + windows["fixed_deadline_seconds"],
                *(instant(expiry["expires_at"]) for expiry in close["input_expirations"]))
    require(instant(close["fixed_deadline"]) == fixed, "original fixed deadline")
    stage = closed(ready["stage"], READY_STAGE_FIELDS)
    start, deadline = instant(stage["admitted_at"]), instant(stage["deadline"])
    require(start == observed and start < deadline <= fixed, "original stage deadline")
    api, later = windows["stages"]["api"], later_seconds(windows, "api")
    require(ready["decision"] is None and stage["name"] == "api"
            and stage["later_seconds"] == later and deadline <= start + api
            and fixed - start >= api + later + FINAL_RESERVE, "original API admission budget")
    return stage
