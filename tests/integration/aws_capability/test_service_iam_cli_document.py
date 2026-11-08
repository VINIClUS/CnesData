"""Documento IAM no JSON CLI exige objeto e prova independente de posse fresca."""

from __future__ import annotations

import sys
import traceback
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pytest

from tests.integration.aws_capability._service_contracts import Reply, ServicePorts
from tests.integration.aws_capability.contracts import (
    Limits,
    Ownership,
    RequestBudget,
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)
from tests.integration.aws_capability.services import AwsServices

if TYPE_CHECKING:
    from pathlib import Path

    from tests.integration.aws_capability.cli import Request

CANARY = "CLI_PRIVATE_DOCUMENT"
DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CLI_PRIVATE_DOCUMENT",
            "Effect": "Allow",
            "Action": "dynamodb:ListTables",
            "Resource": "*",
        }
    ],
}
REPLIES: dict[str, dict[str, object]] = {
    "get-role": {
        "Role": {
            "RoleName": "cnesdata-cap-a1b2c3d4",
            "Path": "/cnesdata-cap/",
            "Arn": "arn:aws:iam::111111111111:role/cnesdata-cap/cnesdata-cap-a1b2c3d4",
        }
    },
    "list-role-tags": {
        "Tags": [
            {"Key": "Project", "Value": "CnesData"},
            {"Key": "Purpose", "Value": "aws-capability"},
            {"Key": "Run", "Value": "a1b2c3d4"},
            {"Key": "ExpiresAt", "Value": "1060"},
        ]
    },
}


@dataclass
class DocumentCommands:
    document: object = field(repr=False)
    requests: list[tuple[str, str, dict[str, object]]] = field(default_factory=list, init=False)
    debits: list[int] = field(default_factory=list, init=False)
    lanes: list[str] = field(default_factory=list, init=False)
    replies: list[Reply] = field(default_factory=list, init=False, repr=False)

    def call(self, request: Request, context: RequestContext) -> Reply:
        self.requests.append((request.service, request.operation, dict(request.payload)))
        context.budget.acquire(context.lane)
        self.debits.append(context.budget.used)
        self.lanes.append(context.lane)
        if request.service != "iam":
            raise AssertionError("unexpected_service")
        if request.operation == "get-role-policy":
            data = {
                "RoleName": "cnesdata-cap-a1b2c3d4",
                "PolicyName": "CLI_ReadPolicy",
                "PolicyDocument": deepcopy(self.document),
            }
        else:
            data = deepcopy(REPLIES[request.operation])
        reply = Reply(ok=True, data=data, error=None)
        self.replies.append(reply)
        return reply


@dataclass(frozen=True)
class DocumentLab:
    services: AwsServices
    resource: Resource
    context: RequestContext
    commands: DocumentCommands


@dataclass(frozen=True)
class Privacy:
    directory: Path
    caplog: pytest.LogCaptureFixture
    capsys: pytest.CaptureFixture[str]

    def check(self, lab: DocumentLab, public: object, error: BaseException | None) -> None:
        captured = self.capsys.readouterr()
        views = [
            self.caplog.text,
            captured.out,
            captured.err,
            repr(lab.services),
            repr(lab.services.ports),
            repr(public),
            *(repr(reply) for reply in lab.commands.replies),
        ]
        failure = error if error is not None else public
        if isinstance(failure, BaseException):
            views.extend(
                [
                    str(failure),
                    repr(failure.args),
                    repr(failure.__cause__),
                    repr(failure.__context__),
                    "".join(traceback.format_exception(failure)),
                ]
            )
        for path in self.directory.rglob("*"):
            if path.is_file():
                views.extend(
                    [str(path.relative_to(self.directory)), path.read_text(errors="replace")]
                )
        assert all(CANARY not in value for value in views)


def _lab(directory: Path, document: object) -> DocumentLab:
    budget = RequestBudget(Limits(), 1060, lambda: 1000)
    context = RequestContext(budget, "cleanup")
    commands = DocumentCommands(document)
    spec = RunSpec("111111111111", "us-east-2", "a1b2c3d4", 1060, directory)
    resource = Resource(
        "iam",
        '{"child_id":{"policy_name":"CLI_ReadPolicy"},"kind":"iam.inline-policy",'
        '"parent_id":"cnesdata-cap-a1b2c3d4","root_id":"cnesdata-cap-a1b2c3d4",'
        '"run":"a1b2c3d4","service":"iam","v":1}',
        "cnesdata-cap-a1b2c3d4",
    )
    return DocumentLab(
        AwsServices(spec, ServicePorts(commands, budget.now)), resource, context, commands
    )


def _assert_readbacks(lab: DocumentLab) -> None:
    assert lab.commands.requests == [
        ("iam", "get-role", {"RoleName": "cnesdata-cap-a1b2c3d4"}),
        ("iam", "list-role-tags", {"RoleName": "cnesdata-cap-a1b2c3d4", "MaxItems": 100}),
        (
            "iam",
            "get-role-policy",
            {"RoleName": "cnesdata-cap-a1b2c3d4", "PolicyName": "CLI_ReadPolicy"},
        ),
    ]
    assert lab.commands.debits == [1, 2, 3]
    assert lab.commands.lanes == ["cleanup", "cleanup", "cleanup"]
    assert lab.context.budget.used == 3
    assert lab.context.budget.work_used == 0
    assert len(lab.commands.replies) == 3


@pytest.mark.parametrize("method", ["lookup", "inspect"])
def test_documento_objeto_cli_confirma_policy_apos_posse_fresca(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    method: str,
) -> None:
    caplog.set_level("DEBUG")
    lab = _lab(tmp_path, DOCUMENT)
    privacy = Privacy(tmp_path, caplog, capsys)
    public: object = None
    try:
        public = getattr(lab.services, method)(lab.resource, lab.context)
    finally:
        _assert_readbacks(lab)
        error = sys.exception()
        privacy.check(lab, public, error)
    assert public == Ownership(
        account="111111111111",
        tags={
            "Project": "CnesData",
            "Purpose": "aws-capability",
            "Run": "a1b2c3d4",
            "ExpiresAt": "1060",
        },
        region="aws-global",
    )


def test_documento_encoded_nao_e_saida_json_cli_valida(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    caplog.set_level("DEBUG")
    lab = _lab(tmp_path, "%7B%22Statement%22%3A%22CLI_PRIVATE_DOCUMENT%22%7D")
    privacy = Privacy(tmp_path, caplog, capsys)
    public: object = None
    try:
        with pytest.raises(ServiceFailure, match=r"^service=describe_invalid$") as failure:
            public = lab.services.lookup(lab.resource, lab.context)
        public = failure.value
    finally:
        _assert_readbacks(lab)
        error = sys.exception()
        privacy.check(lab, public, error)
