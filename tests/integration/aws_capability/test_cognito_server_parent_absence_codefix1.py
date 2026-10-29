"""Pai ausente conserva autoridade do filho para recarga e janitor reais."""

import hashlib
import json
import os
import stat
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from tests.integration.aws_capability._cleanup import Cleanup
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_contracts import ApiError, Reply, ServicePorts
from tests.integration.aws_capability._testing import ProcessBoundary
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    Outcome,
    Policy,
    Ports,
    RequestBudget,
    RequestContext,
    RunSpec,
)
from tests.integration.aws_capability.lifecycle import janitor
from tests.integration.aws_capability.services import AwsServices

ROOT = {
    "service": "cognito",
    "resource_id": "us-east-2_Abc123",
    "name": "cnesdata-cap-a1b2c3d4",
}
CHILD = {
    "service": "cognito",
    "resource_id": (
        '{"child_id":{"identifier":"https://api.cnesdata.com.br"},"kind":"cognito.server",'
        '"parent_id":"us-east-2_Abc123","root_id":"us-east-2_Abc123","run":"a1b2c3d4",'
        '"service":"cognito","v":1}'
    ),
    "name": "cnesdata-cap-a1b2c3d4",
}
REQUESTS = {
    ("sts", "get-caller-identity"): {},
    ("cognito-idp", "list-user-pools"): {"MaxResults": 60},
    ("dynamodb", "list-tables"): {"Limit": 100},
    ("s3api", "list-buckets"): {"MaxBuckets": 1000},
    ("iam", "list-roles"): {"MaxItems": 100, "PathPrefix": "/cnesdata-cap/"},
    ("cognito-idp", "describe-user-pool"): {"UserPoolId": "us-east-2_Abc123"},
}
REPLIES = {
    ("sts", "get-caller-identity"): {
        "UserId": "AROAEXAMPLE:session",
        "Account": "111111111111",
        "Arn": "arn:aws:sts::111111111111:assumed-role/CnesDataCapability/session",
    },
    ("cognito-idp", "list-user-pools"): {"UserPools": []},
    ("dynamodb", "list-tables"): {"TableNames": []},
    ("s3api", "list-buckets"): {"Buckets": []},
    ("iam", "list-roles"): {"Roles": [], "IsTruncated": False},
}


class MissingParentCommands:
    def __init__(self) -> None:
        self.attempts: list[dict[str, Any]] = []
        self.rejected: list[dict[str, Any]] = []

    def call(self, request: Request, context: RequestContext) -> Reply:
        entry = {
            "service": request.service,
            "operation": request.operation,
            "payload": deepcopy(dict(request.payload)),
            "lane": context.lane,
            "debit": None,
        }
        self.attempts.append(entry)
        context.budget.acquire(context.lane)
        entry["debit"] = context.budget.used
        key = (request.service, request.operation)
        if key not in REQUESTS:
            self.rejected.append(deepcopy(entry))
            raise AssertionError("recorder=unexpected_request")
        if dict(request.payload) != REQUESTS[key]:
            self.rejected.append(deepcopy(entry))
            raise AssertionError("recorder=unexpected_payload")
        if key == ("cognito-idp", "describe-user-pool"):
            error = ApiError(code="ResourceNotFoundException", details={}, origin="service")
            return Reply(False, {}, error)
        return Reply(True, deepcopy(REPLIES[key]), None)


def _seed(directory: Path, include_child: bool) -> bytes:
    data = {
        "schema_version": 3,
        "account": "111111111111",
        "region": "us-east-2",
        "run_id": "a1b2c3d4",
        "deadline": 1060,
        "phase": "cleanup_requested",
        "baseline": [],
        "created": [ROOT, CHILD] if include_child else [ROOT],
        "operation_state": "confirmed",
        "probe": "cognito-resource-server",
        "profile_version": 1,
        "limits": {
            "calls": 512,
            "pages": 8,
            "attempts": 2,
            "cleanup_seconds": 90,
            "cleanup_reserve": 256,
        },
    }
    raw = json.dumps(data, allow_nan=False).encode()
    directory.mkdir(mode=0o700)
    path = directory / "a1b2c3d4.json"
    with path.open("xb") as handle:
        path.chmod(0o600)
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return raw


def _spec(directory: Path) -> RunSpec:
    return RunSpec(
        "111111111111",
        "us-east-2",
        "a1b2c3d4",
        1060,
        directory,
        allow_governance_bypass=True,
        probe="cognito-resource-server",
    )


def _ports(spec: RunSpec, commands: MissingParentCommands) -> Ports:
    limits = PROFILES[spec.probe].limits
    services = AwsServices(spec, ServicePorts(commands, lambda: 1000))
    return Ports(
        services,
        ProcessBoundary([]),
        lambda: 1000,
        Policy(spec.account, frozenset({spec.region}), limits),
        RequestBudget(limits, spec.deadline, lambda: 1000),
    )


def _snapshot(journal: Journal) -> dict[str, Any]:
    raw = journal.path.read_bytes()
    return {
        "raw": raw.decode(),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "state": json.loads(raw),
        "mode": oct(stat.S_IMODE(journal.path.stat().st_mode)),
        "directory_mode": oct(stat.S_IMODE(journal.path.parent.stat().st_mode)),
    }


def _reload(spec: RunSpec) -> dict[str, str]:
    try:
        Journal(spec).load()
    except SafetyFailure as error:
        return {"status": "refused", "reason": error.reason}
    return {"status": "accepted"}


def _capture(
    journal: Journal, outcome: Outcome, ports: Ports, commands: MissingParentCommands
) -> dict[str, Any]:
    return {
        "outcome": asdict(outcome),
        "journal": _snapshot(journal),
        "attempts": deepcopy(commands.attempts),
        "rejected": deepcopy(commands.rejected),
        "budget": {"used": ports.budget.used, "work_used": ports.budget.work_used},
        "process_events": deepcopy(cast("ProcessBoundary", ports.processes).events),
    }


def _observe(spec: RunSpec) -> dict[str, Any]:
    commands = MissingParentCommands()
    ports = _ports(spec, commands)
    journal = Journal(spec)
    journal.load()
    initial = {
        "load": "accepted",
        "journal": _snapshot(journal),
        "attempts": deepcopy(commands.attempts),
        "budget_used": ports.budget.used,
    }
    first = _capture(journal, Cleanup(journal, ports).execute(), ports, commands)
    first["reload"] = _reload(spec)
    retry_commands = MissingParentCommands()
    retry_ports = _ports(spec, retry_commands)
    retry = _capture(Journal(spec), janitor(spec, retry_ports), retry_ports, retry_commands)
    retry["reload"] = _reload(spec)
    return {"initial": initial, "first": first, "retry": retry}


def _record(path: Path, observation: dict[str, Any]) -> None:
    path.write_text(json.dumps(observation, indent=2) + "\n")


def _assert_reads(observation: dict[str, Any]) -> None:
    assert observation["initial"]["load"] == "accepted"
    assert observation["initial"]["attempts"] == []
    assert observation["initial"]["budget_used"] == 0
    for stage in ("first", "retry"):
        row = observation[stage]
        assert row["rejected"] == [], "oracle=unexpected_command"
        assert row["process_events"] == []
        assert row["journal"]["mode"] == "0o600"
        assert row["journal"]["directory_mode"] == "0o700"
        assert row["budget"]["work_used"] == 0
        assert 0 <= row["budget"]["used"] < 512
        assert all(call["lane"] == "cleanup" for call in row["attempts"])
        assert all((call["service"], call["operation"]) in REQUESTS for call in row["attempts"])
        assert [call["debit"] for call in row["attempts"]] == list(
            range(1, row["budget"]["used"] + 1)
        )


def test_pai_ausente_preserva_autoridade_duravel_e_retry(tmp_path: Path) -> None:
    directory = tmp_path / "private"
    before = _seed(directory, True)
    observed = _observe(_spec(directory))
    _record(tmp_path / "observation.json", observed)
    _assert_reads(observed)
    assert observed["initial"]["journal"]["state"]["created"] == [ROOT, CHILD]
    assert observed["first"]["journal"]["raw"].encode() == before, "oracle=parent_authority_lost"
    assert observed["first"]["journal"]["state"]["created"] == [ROOT, CHILD]
    assert (observed["first"]["outcome"]["status"], observed["first"]["outcome"]["reason"]) == (
        "incomplete",
        "ownership_mismatch",
    )
    assert observed["first"]["reload"] == {"status": "accepted"}
    assert (observed["retry"]["outcome"]["status"], observed["retry"]["outcome"]["reason"]) == (
        "incomplete",
        "ownership_mismatch",
    )
    assert observed["retry"]["journal"]["raw"].encode() == before
    assert observed["retry"]["reload"] == {"status": "accepted"}
    assert observed["retry"]["budget"]["used"] > 0
    assert observed["retry"]["attempts"][0]["operation"] == "get-caller-identity"


def test_pai_ausente_root_only_completa_e_recarrega(tmp_path: Path) -> None:
    directory = tmp_path / "private"
    _seed(directory, False)
    observed = _observe(_spec(directory))
    _record(tmp_path / "observation.json", observed)
    _assert_reads(observed)
    assert observed["initial"]["journal"]["state"]["created"] == [ROOT]
    assert (observed["first"]["outcome"]["status"], observed["first"]["outcome"]["reason"]) == (
        "succeeded",
        "complete",
    )
    saved = observed["first"]["journal"]["state"]
    assert (saved["phase"], saved["operation_state"], saved["created"]) == (
        "complete",
        "confirmed",
        [],
    )
    assert {"pending_service", "pending_resource"}.isdisjoint(saved)
    assert observed["first"]["reload"] == {"status": "accepted"}
    assert (observed["retry"]["outcome"]["status"], observed["retry"]["outcome"]["reason"]) == (
        "succeeded",
        "complete",
    )
    assert observed["retry"]["journal"]["raw"] == observed["first"]["journal"]["raw"]
    assert observed["retry"]["reload"] == {"status": "accepted"}
    assert observed["retry"]["budget"]["used"] > 0
    assert observed["retry"]["attempts"][0]["operation"] == "get-caller-identity"
