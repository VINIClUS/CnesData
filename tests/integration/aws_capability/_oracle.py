"""Oráculos explícitos, sem importar algoritmos do harness."""

from tests.integration.aws_capability.contracts import Outcome, Page


class Trace(list[tuple[object, ...]]):
    epoch: int = 0
    consumed: int = -1
    expected_name: str = "cnesdata-cap-a1b2c3d4"

    def begin(self) -> None:
        self.clear()
        self.epoch += 1


def assert_complete(outcome: Outcome, events: Trace) -> None:
    assert events.epoch != events.consumed, "oracle=stale_execution"
    assert outcome.status == "succeeded"
    assert outcome.reason == "complete"
    assert outcome.remaining == ()
    assert outcome.evidence.get("inventory_complete") is True
    assert outcome.evidence.get("counts") == {"cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0}
    assert_fresh_inventory(events)


def assert_fresh_inventory(events: Trace) -> None:
    assert events.epoch != events.consumed, "oracle=stale_execution"
    last_delete = max((i for i, event in enumerate(events) if event[0] == "delete"), default=-1)
    final = events[last_delete + 1 :]
    for service in ("cognito", "dynamodb", "s3", "iam"):
        assert_pages(final, service, events.expected_name)
    events.consumed = events.epoch


def assert_pages(events: list[tuple[object, ...]], service: str, name: str) -> None:
    rows = [event for event in events if event[0] == "inventory_result" and event[1] == service]
    assert rows, "oracle=missing_response"
    assert not any(event[:2] == ("inventory_error", service) for event in events)
    cursor = None
    seen: set[str] = set()
    for row in rows:
        assert row[2] == cursor, "oracle=cursor_not_consumed"
        page = row[3]
        assert isinstance(page, Page)
        assert all(resource.name != name for resource in page.resources), "oracle=residual"
        assert page.cursor != ""
        assert page.cursor is None or page.cursor not in seen
        if page.cursor is not None:
            seen.add(page.cursor)
        cursor = page.cursor
        if cursor is None:
            seen.clear()
    assert cursor is None, "oracle=unterminated_inventory"


def assert_probe_order(events: list[tuple[object, ...]], spec: object) -> None:
    created = [(i, event[1]) for i, event in enumerate(events) if event[0] == "created"]
    probes = [(i, event) for i, event in enumerate(events) if event[0] == "probe"]
    deletes = [i for i, event in enumerate(events) if event[0] == "delete"]
    assert len(created) == 4
    assert len(probes) == 1
    index, probe = probes[0]
    assert probe[1] == tuple(resource for _, resource in created)
    assert probe[2] == spec
    assert max(i for i, _ in created) < index
    assert deletes
    assert index < min(deletes)


def assert_no_mutation(events: list[tuple[object, ...]]) -> None:
    assert not any(event[0] in {"create", "probe", "delete"} for event in events)
