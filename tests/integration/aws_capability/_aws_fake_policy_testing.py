"""Avaliador de policies inline da fronteira: ação, recurso e condição no mesmo statement."""

from collections.abc import Callable, Mapping, Sequence
from fnmatch import fnmatchcase

from tests.integration.aws_capability._aws_fake_sts_testing import name_for
from tests.integration.aws_capability._aws_fake_types_testing import OPERATOR, FakeRole

Keys = Mapping[str, Sequence[str]]


def listed(value: object) -> list[str]:
    return [value] if isinstance(value, str) else [str(item) for item in value]  # type: ignore[attr-defined]


def _like(values: Sequence[str], patterns: list[str]) -> bool:
    return all(any(fnmatchcase(value, pattern) for pattern in patterns) for value in values)


def _holds(operator: str, expected: object, values: Sequence[str]) -> bool:
    if operator == "Null":
        return (not values) if expected == "true" else bool(values)
    patterns = listed(expected)
    if operator == "ForAllValues:StringEquals":
        return all(value in patterns for value in values)
    if operator == "ForAllValues:StringLike":
        return _like(values, patterns)
    if operator == "StringLike":
        return bool(values) and _like(values, patterns)
    raise AssertionError(f"fake=unsupported_condition operator={operator}")


def _conditions_hold(condition: Mapping[str, Mapping[str, object]], keys: Keys) -> bool:
    return all(
        _holds(operator, expected, keys.get(key, ()))
        for operator, body in condition.items()
        for key, expected in body.items()
    )


def statement_permits(
    statement: Mapping[str, object], action: str, resource: str, keys: Keys
) -> bool:
    return (
        statement["Effect"] == "Allow"
        and action in listed(statement["Action"])
        and any(fnmatchcase(resource, pattern) for pattern in listed(statement["Resource"]))
        and _conditions_hold(statement.get("Condition", {}), keys)  # type: ignore[arg-type]
    )


class PolicyHandlers:
    roles: dict[str, FakeRole]
    counter: int
    allowed: Callable[[str, str], bool]

    def permits(self, who: str, action: str, resource: str, keys: Keys | None = None) -> bool:
        if not self.allowed(who, action):
            return False
        if who == OPERATOR:
            return True
        role = self.roles[name_for(who)]
        return any(
            statement_permits(statement, action, resource, keys or {})
            for _, effective, document in role.policies.values()
            if self.counter >= effective
            for statement in document["Statement"]  # type: ignore[index]
        )
