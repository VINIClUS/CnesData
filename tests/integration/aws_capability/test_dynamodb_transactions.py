"""Transações exigem cardinalidade literal, leitura forte e recusa remota limitada."""

import copy
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

import pytest

from tests.integration.aws_capability._dynamodb_transactions_testing import (
    TransactionLab,
    fixture_steps,
    private_surface,
    queue,
    trace,
    tx_lab,
)
from tests.integration.aws_capability._service_contracts import ApiError, Reply
from tests.integration.aws_capability._service_ddb_prepare_testing import transactions_evidence
from tests.integration.aws_capability._service_read_testing import CANARY
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import BudgetExhausted, Resource, ServiceFailure

__all__ = ["private_surface", "tx_lab"]

ROOT = Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4")
TARGET = {"pk": {"S": "cap#a1b2c3d4#txdup"}, "sk": {"S": "target"}}
SIBLING = {"pk": {"S": "cap#a1b2c3d4#txdup"}, "sk": {"S": "sibling"}}
REJECTION = "probe=ddb_transactions_rejection_invalid"
READBACK = "probe=ddb_transactions_readback_invalid"
RESPONSE = "probe=ddb_transactions_response_invalid"
QUERY = "probe=ddb_transactions_query_invalid"


def _items(count: int, group: str) -> list[Any]:
    return [
        {"pk": {"S": f"cap#a1b2c3d4#{group}"}, "sk": {"S": f"item#{i:03d}"}, "version": {"N": "1"}}
        for i in range(count)
    ]


def _literal_writes(steps: Sequence[Any]) -> None:
    for index, count in ((0, 100), (3, 101)):
        request = steps[index]["request"]
        assert request[:3] == ["dynamodb", "transact-write-items", "us-east-2"]
        items = _items(count, f"tx{count}")
        keys = {(item["pk"]["S"], item["sk"]["S"]) for item in items}
        assert len(items) == len(keys) == count
        assert all(len(json.dumps(item).encode("utf-8")) < 1024 for item in items)
        assert request[3] == {
            "TransactItems": [{"Put": {"TableName": ROOT.name, "Item": item}} for item in items],
            "ClientRequestToken": f"a1b2c3d4-tx{count}",
        }
    assert steps[4]["request"] == [
        "dynamodb",
        "transact-write-items",
        "us-east-2",
        {
            "TransactItems": [
                {"Put": {"TableName": ROOT.name, "Item": {**TARGET, "version": {"N": "1"}}}},
                {
                    "ConditionCheck": {
                        "TableName": ROOT.name,
                        "Key": TARGET,
                        "ConditionExpression": "attribute_not_exists(#pk)",
                        "ExpressionAttributeNames": {"#pk": "pk"},
                    }
                },
                {"Put": {"TableName": ROOT.name, "Item": {**SIBLING, "version": {"N": "1"}}}},
            ],
            "ClientRequestToken": "a1b2c3d4-txdup",
        },
    ]
    assert TARGET != SIBLING
    for key in (TARGET, SIBLING):
        assert len(json.dumps({**key, "version": {"N": "1"}}).encode("utf-8")) < 1024


def _literal_queries(steps: Sequence[Any]) -> None:
    for index, group in ((1, "tx100"), (2, "tx100"), (5, "txdup")):
        payload: dict[str, Any] = {
            "TableName": ROOT.name,
            "KeyConditionExpression": "#pk=:pk",
            "ExpressionAttributeNames": {"#pk": "pk"},
            "ExpressionAttributeValues": {":pk": {"S": f"cap#a1b2c3d4#{group}"}},
            "ConsistentRead": True,
            "Limit": 60,
        }
        if index == 2:
            payload["ExclusiveStartKey"] = {
                "pk": {"S": "cap#a1b2c3d4#tx100"},
                "sk": {"S": "item#059"},
            }
        request = ["dynamodb", "query", "us-east-2", payload]
        actual = steps[index]["request"]
        assert json.dumps(actual, sort_keys=True) == json.dumps(request, sort_keys=True)


def _full_readback(items: Any, expected: list[Any]) -> bool:
    if not isinstance(items, list) or len(items) != len(expected):
        return False
    if not all(isinstance(item, dict) for item in items):
        return False
    keys = [
        json.dumps({key: item.get(key) for key in ("pk", "sk")}, sort_keys=True) for item in items
    ]
    return len(set(keys)) == len(keys) and sorted(
        json.dumps(item, sort_keys=True) for item in items
    ) == sorted(json.dumps(item, sort_keys=True) for item in expected)


def _duplicate_accepted(reply: Reply) -> bool:
    error = reply.error
    if reply.ok or error is None or error.origin != "service":
        return False
    if error.code == "ValidationException":
        return True
    if error.code != "TransactionCanceledException":
        return False
    reasons = error.details.get("CancellationReasons")
    if not isinstance(reasons, list) or len(reasons) != 3:
        return False
    if not all(
        isinstance(reason, Mapping) and type(reason.get("Code")) is str for reason in reasons
    ):
        return False
    return [reason["Code"] for reason in reasons] in [
        ["ValidationError", "None", "None"],
        ["None", "ValidationError", "None"],
        ["ValidationError", "ValidationError", "None"],
    ]


def _remote(code: str, reasons: Any = ...) -> Any:
    error = {"Code": code, "Message": CANARY, "Private": CANARY}
    if reasons is not ...:
        error["CancellationReasons"] = reasons
    return {"exit": 254, "error": error}


def _canceled(codes: Sequence[str]) -> Any:
    return _remote("TransactionCanceledException", [{"Code": code} for code in codes])


def _duplicate_failures() -> list[Any]:
    canceled = "TransactionCanceledException"
    valid = [{"Code": "None"}, {"Code": "ValidationError"}, {"Code": "None"}]
    local = {**_remote(canceled, valid), "exit": 252}
    return [
        ("cancel_local", local),
        ("sem_motivos", _remote(canceled)),
        ("motivos_null", _remote(canceled, None)),
        ("motivos_objeto", _remote(canceled, {})),
        ("dois_motivos", _remote(canceled, valid[:2])),
        ("quatro_motivos", _remote(canceled, [*valid, {"Code": "None"}])),
        ("motivo_escalar", _remote(canceled, [valid[0], CANARY, valid[2]])),
        ("codigo_ausente", _remote(canceled, [valid[0], {}, valid[2]])),
        ("codigo_null", _remote(canceled, [valid[0], {"Code": None}, valid[2]])),
        ("codigo_desconhecido", _canceled(["None", CANARY, "None"])),
        ("todos_none", _canceled(["None", "None", "None"])),
        ("somente_sibling", _canceled(["None", "None", "ValidationError"])),
        ("condicao", _canceled(["None", "ConditionalCheckFailed", "None"])),
        ("misto_condicao", _canceled(["ValidationError", "ConditionalCheckFailed", "None"])),
        *[
            (code, _canceled(["ValidationError", code, "None"]))
            for code in (
                "ProvisionedThroughputExceeded",
                "ThrottlingError",
                "TransactionConflict",
                "ItemCollectionSizeLimitExceeded",
            )
        ],
    ]


def _data_reply(data: Any) -> Any:
    return {"exit": 0, "data": data}


def _bad_items(case: str) -> Any:
    items = _items(100, "tx100")[60:]
    if case == "99":
        items.pop()
    elif case == "101":
        items += _items(101, "tx100")[100:]
    elif case == "duplicado":
        items[-1] = _items(1, "tx100")[0]
    elif case == "estrangeiro":
        items[-1]["pk"] = {"S": CANARY}
    else:
        items[-1]["version"] = {"N": CANARY}
    return _data_reply({"Items": items, "Count": 100, "Private": CANARY})


def _read_failures() -> list[Any]:
    cursor = {"pk": {"S": "cap#a1b2c3d4#tx100"}, "sk": {"S": "item#059"}}
    return [
        *[
            (f"items_{name}", 1, _data_reply(data), 5, RESPONSE)
            for name, data in (
                ("ausente", {"Count": 100}),
                ("null", {"Items": None}),
                ("escalar", {"Items": CANARY}),
            )
        ],
        *[
            (f"itens_{case}", 2, _bad_items(case), 6, READBACK)
            for case in ("99", "101", "duplicado", "estrangeiro", "versao")
        ],
        *[
            (f"cursor_{name}", 1, _data_reply({"Items": [], "LastEvaluatedKey": key}), 5, QUERY)
            for name, key in (
                ("null", None),
                ("string", CANARY),
                ("estrangeiro", {**cursor, "pk": {"S": CANARY}}),
            )
        ],
        ("cursor_repetido", 2, _data_reply({"Items": [], "LastEvaluatedKey": cursor}), 6, QUERY),
        *[
            (f"zero_{name}", 5, _data_reply(data), 9, reason)
            for name, data, reason in (
                ("ausente", {"Count": 0}, RESPONSE),
                ("null", {"Items": None}, RESPONSE),
                ("escalar", {"Items": CANARY}, RESPONSE),
                (
                    "parcial",
                    {
                        "Items": [
                            {**TARGET, "version": {"N": "1"}},
                            {**SIBLING, "version": {"N": "1"}},
                        ]
                    },
                    READBACK,
                ),
            )
        ],
    ]


def _negative_cases() -> list[Any]:
    timeout = {"timeout": CANARY}
    denied = _remote("AccessDeniedException")
    local = {**_remote("ValidationException"), "exit": 252}
    return [
        ("escrita_remota", 0, denied, 4, "service=request_failed"),
        ("escrita_timeout", 0, timeout, 4, "service=request_failed"),
        ("pagina1_timeout", 1, timeout, 5, "service=request_failed"),
        ("pagina2_timeout", 2, timeout, 6, "service=request_failed"),
        *_read_failures(),
        *[
            (f"101_{name}", 3, reply, 7, REJECTION)
            for name, reply in (
                ("sucesso", _data_reply({})),
                ("local", local),
                ("codigo", denied),
                ("timeout", timeout),
            )
        ],
        *[
            (f"dup_{name}", 4, reply, 8, REJECTION)
            for name, reply in (
                ("sucesso", _data_reply({})),
                ("local", local),
                ("codigo", denied),
                ("timeout", timeout),
                *_duplicate_failures(),
            )
        ],
        ("zero_timeout", 5, timeout, 9, "service=request_failed"),
    ]


def _probe(lab: TransactionLab) -> BaseException:
    with pytest.raises((ServiceFailure, BudgetExhausted)) as error:
        lab.read.services.probe([ROOT], replace(lab.read.services.spec), lab.read.context)
    lab.privacy.check(error.value, lab.read)
    return error.value


@pytest.mark.parametrize(
    "case", ["nominal", "fim_vazio", "ordem", "validacao", "primeiro", "ambos"]
)
def test_nominal_exige_nove_chamadas_e_devolve_observacoes(
    tx_lab: TransactionLab, case: str
) -> None:
    steps = fixture_steps()
    if case == "fim_vazio":
        steps[2]["reply"]["data"]["LastEvaluatedKey"] = {}
        steps[5]["reply"]["data"]["LastEvaluatedKey"] = {}
    elif case == "ordem":
        steps[1]["reply"]["data"]["Items"].reverse()
        steps[2]["reply"]["data"]["Items"].reverse()
    elif case == "validacao":
        steps[4]["reply"] = _remote("ValidationException")
    elif case in {"primeiro", "ambos"}:
        middle = "ValidationError" if case == "ambos" else "None"
        steps[4]["reply"] = _canceled(["ValidationError", middle, "None"])
    queue(tx_lab, steps)
    lab = tx_lab.read
    evidence = lab.services.probe([ROOT], replace(lab.services.spec), lab.context)
    tx_lab.privacy.check_value(evidence, lab)
    trace(tx_lab, 9)
    expected = {
        "validacao": transactions_evidence("ValidationException", ()),
        "primeiro": transactions_evidence(reasons=("ValidationError", "None", "None")),
        "ambos": transactions_evidence(reasons=("ValidationError", "ValidationError", "None")),
    }.get(case, transactions_evidence())
    assert evidence == expected


@pytest.mark.parametrize("case", [pytest.param(row, id=row[0]) for row in _negative_cases()])
def test_recusa_sem_repetir_nem_vazar(tx_lab: TransactionLab, case: Any) -> None:
    _, index, reply, count, reason = case
    steps = fixture_steps()
    steps[index]["reply"] = copy.deepcopy(reply)
    queue(tx_lab, steps)
    error = _probe(tx_lab)
    trace(tx_lab, count)
    assert type(error) is ServiceFailure
    assert error.args == (reason,)


def test_oito_paginas_nao_terminais_esgotam_sem_nona_query(tx_lab: TransactionLab) -> None:
    steps = fixture_steps()
    pages = []
    previous = None
    for index in range(8):
        request = copy.deepcopy(steps[1]["request"])
        if previous is not None:
            request[3]["ExclusiveStartKey"] = previous
        key = {"pk": {"S": "cap#a1b2c3d4#tx100"}, "sk": {"S": f"item#{index:03d}"}}
        data = {"Items": [], "LastEvaluatedKey": key, "Private": CANARY}
        pages.append({"request": request, "reply": _data_reply(data)})
        previous = key
    queue(tx_lab, [steps[0], *pages, *steps[3:]])
    error = _probe(tx_lab)
    trace(tx_lab, 12)
    assert type(error) is ServiceFailure
    assert error.args == ("probe=ddb_transactions_pages_exhausted",)


@pytest.mark.parametrize(("precharge", "count"), [(90, 6), (88, 8)])
def test_reserva_impede_chamada_sem_debito(
    tx_lab: TransactionLab, precharge: int, count: int
) -> None:
    budget = tx_lab.original_budget
    for _ in range(precharge):
        budget.acquire("work")
    queue(tx_lab)
    error = _probe(tx_lab)
    trace(tx_lab, count, precharge)
    assert type(error) is BudgetExhausted
    assert error.args == ("budget=requests",)
    assert budget.used == budget.work_used == 96
    assert budget.limits.calls - budget.used == budget.limits.cleanup_reserve == 96


@pytest.mark.parametrize(("start", "count"), [(1057, 6), (1056, 8)])
def test_deadline_impede_chamada_sem_debito(
    tx_lab: TransactionLab, start: float, count: int
) -> None:
    tx_lab.clock.current = start
    tx_lab.clock.ticks = True
    queue(tx_lab)
    error = _probe(tx_lab)
    trace(tx_lab, count)
    assert type(error) is BudgetExhausted
    assert error.args == ("budget=deadline",)
    assert tx_lab.clock.current == 1060


def _oracle_controls(replies: Sequence[Reply]) -> None:
    for index, count in ((0, 99), (3, 100)):
        altered = fixture_steps()
        altered[index]["request"][3]["TransactItems"].pop()
        assert len(altered[index]["request"][3]["TransactItems"]) == count
        with pytest.raises(AssertionError):
            _literal_writes(altered)
    altered = fixture_steps()
    altered[0]["request"][3]["TransactItems"][-1] = altered[0]["request"][3]["TransactItems"][0]
    with pytest.raises(AssertionError):
        _literal_writes(altered)
    expected = _items(100, "tx100")
    assert _full_readback(list(reversed(expected)), expected)
    assert not _full_readback(expected[:99], expected)
    assert not _full_readback([*expected, expected[0]], expected)
    assert not _full_readback([*expected[:99], expected[0]], expected)
    for case in ("estrangeiro", "versao"):
        assert not _full_readback([*expected[:60], *_bad_items(case)["data"]["Items"]], expected)
    for _, raw in _duplicate_failures():
        error = raw["error"]
        origin = "service" if raw["exit"] == 254 else "local"
        assert not _duplicate_accepted(Reply(False, {}, ApiError(error["Code"], error, origin)))
    for raw in [
        _remote("ValidationException"),
        *[
            _canceled(codes)
            for codes in (
                ["ValidationError", "None", "None"],
                ["None", "ValidationError", "None"],
                ["ValidationError", "ValidationError", "None"],
            )
        ],
    ]:
        error = raw["error"]
        assert _duplicate_accepted(Reply(False, {}, ApiError(error["Code"], error, "service")))
    assert _duplicate_accepted(replies[7])
    assert not _duplicate_accepted(replace(replies[7], ok=True))
    assert not _duplicate_accepted(replace(replies[7], error=None))


def test_fronteira_real_e_oraculos_literais_independentes(tx_lab: TransactionLab) -> None:
    steps = fixture_steps()
    _literal_writes(steps)
    _literal_queries(steps)
    assert steps[0]["reply"] == {"exit": 0}
    queue(tx_lab)
    read = tx_lab.read
    replies = [
        read.services.ports.commands.call(Request(service, operation, payload), read.context)
        for service, operation, _, payload in tx_lab.expected
    ]
    assert [reply.ok for reply in replies] == [True] * 6 + [False, False, True]
    assert (replies[3].data, replies[3].error) == ({}, None)
    assert replies[6].error is not None
    assert (replies[6].error.origin, replies[6].error.code) == ("service", "ValidationException")
    error = replies[7].error
    assert error is not None
    assert (error.origin, error.code) == ("service", "TransactionCanceledException")
    reasons: Any = error.details["CancellationReasons"]
    assert [reason["Code"] for reason in reasons] == ["None", "ValidationError", "None"]
    items: Any = [replies[4].data["Items"], replies[5].data["Items"]]
    assert _full_readback([*items[0], *items[1]], _items(100, "tx100"))
    assert replies[4].data["LastEvaluatedKey"] == steps[2]["request"][3]["ExclusiveStartKey"]
    assert replies[8].data["Items"] == []
    assert CANARY not in repr(replies)
    _oracle_controls(replies)
    trace(tx_lab, 9)
