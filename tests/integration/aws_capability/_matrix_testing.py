"""Trilha de chamadas dos probes de matriz: quem chamou o quê, com qual chave, e o resultado."""

from collections.abc import Mapping
from dataclasses import dataclass

from tests.integration.aws_capability._probe_testing import ProbeLab
from tests.integration.aws_capability._service_contracts import Reply
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import RequestContext

DDB_FIELDS = {"Key", "Item", "ExpressionAttributeValues", "IndexName", "KeyConditionExpression"}
S3_FIELDS = {"Key", "Prefix", "VersionId", "ObjectLockRetainUntilDate", "Retention"}


@dataclass(frozen=True)
class Call:
    who: str
    request: Request
    ok: bool
    code: str

    @property
    def operation(self) -> str:
        return self.request.operation

    @property
    def payload(self) -> Mapping[str, object]:
        return self.request.payload


def record(lab: ProbeLab) -> list[Call]:
    calls: list[Call] = []
    original = lab.aws.dispatch

    def traced(request: Request, context: RequestContext, who: str) -> Reply:
        reply = original(request, context, who)
        code = "ok" if reply.ok or reply.error is None else reply.error.code
        calls.append(Call(who, request, reply.ok, code))
        return reply

    lab.aws.dispatch = traced  # type: ignore[method-assign]
    return calls


def dims(call: Call) -> dict[str, object]:
    payload, operation = call.payload, call.operation
    if call.request.service == "dynamodb":
        if operation == "query":
            return {
                "index": payload.get("IndexName", "table"),
                "key": payload["ExpressionAttributeValues"][":pk"]["S"],  # type: ignore[index]
            }
        item = payload.get("Key") or payload.get("Item")
        return {"key": item["pk"]["S"]} if isinstance(item, Mapping) else {}
    if call.request.service == "s3api":
        return {"key": payload.get("Key", payload.get("Prefix"))}
    return {}


def shape(call: Call) -> dict[str, object]:
    ignored = DDB_FIELDS | S3_FIELDS
    return {key: value for key, value in call.payload.items() if key not in ignored}


def single_flip(control: Call, denial: Call) -> bool:
    return (
        control.who == denial.who
        and control.operation == denial.operation
        and shape(control) == shape(denial)
        and sum(dims(control).get(k) != v for k, v in dims(denial).items()) == 1
        and set(dims(control)) == set(dims(denial))
    )


def earlier_controls(calls: list[Call], index: int) -> list[Call]:
    return [call for call in calls[:index] if call.ok]


def covered(calls: list[Call], index: int) -> bool:
    denial = calls[index]
    return any(single_flip(control, denial) for control in earlier_controls(calls, index))
