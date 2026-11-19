"""TTL registra presença e GSI esparso exige propagação limitada e devolve observações do perfil."""

import logging
from copy import deepcopy
from logging.handlers import BufferingHandler
from typing import Any

import pytest

from tests.integration.aws_capability._dynamodb_ttl_gsi_testing import (
    TtlGsiLab,
    acceptance_controls,
    assert_failure,
    assert_records,
    canonical,
    equal,
    fixture_steps,
    private_surface,
    queue,
    records,
    trace,
    ttl_lab,
)
from tests.integration.aws_capability._service_read_testing import CANARY, ROOT
from tests.integration.aws_capability.contracts import BudgetExhausted, ServiceFailure

__all__ = ["private_surface", "ttl_lab"]
E = {"pk": {"S": "cap#a1b2c3d4#ttl"}, "sk": {"S": "expired"}, "expires_at": {"N": "1999996400"}}
V = {"pk": {"S": "cap#a1b2c3d4#ttl"}, "sk": {"S": "current"}, "expires_at": {"N": "2000003600"}}
INDEXED = {
    "pk": {"S": "cap#a1b2c3d4#gsi"},
    "sk": {"S": "indexed"},
    "gsi6pk": {"S": "cap#a1b2c3d4#pending"},
    "gsi6sk": {"S": "item#001"},
}
S = {"pk": {"S": "cap#a1b2c3d4#gsi"}, "sk": {"S": "sparse"}}
BASE = {"pk": {"S": "cap#a1b2c3d4#gsi"}, "sk": {"S": "indexed"}}
CURSOR = {"pk": {"S": "cap#a1b2c3d4#ttl"}, "sk": {"S": "expired"}}
COMPLETED = "probe=completed"
GET = "probe=ddb_ttl_gsi observation=ttl_get expired_present=%s current_present=%s"
QUERY = "probe=ddb_ttl_gsi observation=ttl_query expired_present=%s current_present=%s"
SCAN = "probe=ddb_ttl_gsi observation=ttl_scan expired_present=%s current_present=%s"
INCLUDE = "probe=ddb_ttl_gsi observation=gsi_include status=%s polls=%d pages=%d"
REMOVE = "probe=ddb_ttl_gsi observation=gsi_remove status=%s polls=%d pages=%d"
REJECT = (
    "probe=ddb_ttl_gsi observation=gsi_consistent_read "
    "status=remote_rejected code=ValidationException"
)
NOMINAL = [
    (GET, (True, True)),
    (QUERY, (True, True)),
    (SCAN, (True, True)),
    (INCLUDE, ("observed", 1, 1)),
    (REMOVE, ("observed", 1, 1)),
    (REJECT, ()),
]


def _put(item: Any) -> Any:
    return [
        "dynamodb",
        "put-item",
        "us-east-2",
        {"TableName": "cnesdata-cap-a1b2c3d4", "Item": item},
    ]


def _get(key: Any) -> Any:
    return [
        "dynamodb",
        "get-item",
        "us-east-2",
        {"TableName": "cnesdata-cap-a1b2c3d4", "Key": key, "ConsistentRead": True},
    ]


def _requests() -> list[Any]:
    table = "cnesdata-cap-a1b2c3d4"
    base_query = {
        "TableName": table,
        "KeyConditionExpression": "#pk=:pk",
        "ExpressionAttributeNames": {"#pk": "pk"},
        "ExpressionAttributeValues": {":pk": {"S": "cap#a1b2c3d4#ttl"}},
        "ConsistentRead": True,
        "Limit": 2,
    }
    gsi_query = {
        "TableName": table,
        "IndexName": "gsi6",
        "KeyConditionExpression": "#gpk=:gpk",
        "ExpressionAttributeNames": {"#gpk": "gsi6pk"},
        "ExpressionAttributeValues": {":gpk": {"S": "cap#a1b2c3d4#pending"}},
        "Limit": 2,
    }

    return [
        _put(E),
        _put(V),
        _get(CURSOR),
        _get({"pk": {"S": "cap#a1b2c3d4#ttl"}, "sk": {"S": "current"}}),
        ["dynamodb", "query", "us-east-2", base_query],
        ["dynamodb", "scan", "us-east-2", {"TableName": table, "ConsistentRead": True, "Limit": 2}],
        _put(INDEXED),
        _put(S),
        _get(BASE),
        _get(S),
        ["dynamodb", "query", "us-east-2", gsi_query],
        [
            "dynamodb",
            "update-item",
            "us-east-2",
            {
                "TableName": table,
                "Key": BASE,
                "UpdateExpression": "REMOVE #gpk, #gsk",
                "ExpressionAttributeNames": {"#gpk": "gsi6pk", "#gsk": "gsi6sk"},
            },
        ],
        _get(BASE),
        ["dynamodb", "query", "us-east-2", deepcopy(gsi_query)],
        ["dynamodb", "query", "us-east-2", {**gsi_query, "ConsistentRead": True}],
    ]


def _steps() -> list[Any]:
    steps = fixture_steps()
    equal([step["request"] for step in steps], _requests())
    expected = [
        {},
        {},
        {"Item": E},
        {"Item": V},
        {"Items": [E, V], "Count": 0},
        {"Items": [E, V], "Count": 0},
        {},
        {},
        {"Item": INDEXED},
        {"Item": S},
        {"Items": [INDEXED]},
        {},
        {"Item": BASE},
        {"Items": []},
    ]
    for step, data in zip(steps[:14], expected, strict=True):
        equal(step["reply"], {"exit": 0, "data": {**data, "Private": CANARY}})
    equal(steps[14]["reply"], _remote("ValidationException"))
    assert all(len(canonical(item).encode()) < 1024 for item in (E, V, INDEXED, S))
    return steps


def _data(value: Any) -> Any:
    return {"exit": 0, "data": {**value, "Private": CANARY}}


def _remote(code: str) -> Any:
    return {"exit": 254, "error": {"Code": code, "Message": CANARY, "Private": CANARY}}


def _evidence(safe: list[Any]) -> dict[str, Any]:
    get, query, scan, include, remove = (row[1] for row in safe[:5])
    return {
        "ttl_expired_get_present": get[0],
        "ttl_expired_query_present": query[0],
        "ttl_expired_scan_present": scan[0],
        "ttl_current_present": True,
        "gsi_include_status": include[0],
        "gsi_include_polls": include[1],
        "gsi_remove_status": remove[0],
        "gsi_remove_polls": remove[1],
        "gsi_consistent_read_code": "ValidationException",
    }


def _run(lab: TtlGsiLab, steps: list[Any], expectation: Any) -> None:
    count, reason, safe, sleeps = expectation
    queue(lab, steps)
    if reason == COMPLETED:
        evidence = lab.read.services.probe([ROOT], lab.read.services.spec, lab.read.context)
        lab.privacy.check_value(evidence, lab.read)
        equal(evidence, _evidence(safe))
        trace(lab, count, sleeps)
        assert_records(lab.privacy.caplog, safe)
        return
    with pytest.raises((ServiceFailure, BudgetExhausted)) as error:
        lab.read.services.probe([ROOT], lab.read.services.spec, lab.read.context)
    lab.privacy.check(error.value, lab.read)
    trace(lab, count, sleeps)
    assert_failure(error.value, reason)
    assert_records(lab.privacy.caplog, safe)


@pytest.mark.parametrize(
    "case", ["todos", "ausente", "somente_get", "ordem", "inclusao", "remocao", "ambos"]
)
def test_registra_presenca_expirada_sem_exigir_remocao(ttl_lab: TtlGsiLab, case: str) -> None:
    steps, safe, sleeps = _steps(), deepcopy(NOMINAL), []
    if case in {"ausente", "somente_get"}:
        for index in (4, 5):
            steps[index]["reply"] = _data({"Items": [V], "Count": 2})
            safe[index - 3] = (safe[index - 3][0], (False, True))
        if case == "ausente":
            steps[2]["reply"] = _data({})
            safe[0] = (GET, (False, True))
    if case == "ordem":
        for index in (4, 5):
            steps[index]["reply"] = _data({"Items": [V, E], "LastEvaluatedKey": {}})
    for index, kind in ((13, "remocao"), (10, "inclusao")):
        if case in {kind, "ambos"}:
            pending = deepcopy(steps[index])
            pending["reply"] = _data({"Items": [INDEXED] if kind == "remocao" else []})
            steps.insert(index, pending)
            safe[4 if kind == "remocao" else 3] = (
                REMOVE if kind == "remocao" else INCLUDE,
                ("observed", 2, 2),
            )
            sleeps.append(1.0)
    _run(ttl_lab, steps, (len(steps) + 3, COMPLETED, safe, sleeps))
    assert ttl_lab.anchor_calls == 1


TTL_BAD = [
    ("expirado_chave", 2, {"Item": {**E, "sk": {"S": CANARY}}}),
    ("expirado_ttl", 2, {"Item": {**E, "expires_at": {"N": CANARY}}}),
    ("item_null", 2, {"Item": None}),
    ("atual_get", 3, {}),
    ("atual_query", 4, {"Items": [E]}),
    ("atual_scan", 5, {"Items": [E]}),
    ("duplicado", 4, {"Items": [E, V, V]}),
    ("estrangeiro", 5, {"Items": [V, {**E, "pk": {"S": CANARY}}]}),
    ("items_ausente", 4, {"Count": 2}),
    ("items_escalar", 5, {"Items": CANARY}),
]


@pytest.mark.parametrize("case", [pytest.param(row, id=row[0]) for row in TTL_BAD])
def test_ttl_recusa_imagem_invalida_sem_avancar(ttl_lab: TtlGsiLab, case: Any) -> None:
    _, index, data = case
    steps = _steps()
    steps[index]["reply"] = _data(data)
    safe = NOMINAL[: max(0, index - 3)]
    _run(ttl_lab, steps, (index + 4, "probe=ddb_ttl_response_invalid", safe, []))


@pytest.mark.parametrize(
    "case", ["query", "scan", "repetido", "estrangeiro", "null", "query_esgotada", "scan_esgotada"]
)
def test_paginacao_segue_cursor_exato_e_limita_duas_paginas(ttl_lab: TtlGsiLab, case: str) -> None:
    steps = _steps()
    index = 5 if case in {"scan", "estrangeiro", "scan_esgotada"} else 4
    first, second = deepcopy(steps[index]), deepcopy(steps[index])
    first["reply"] = _data({"Items": [], "LastEvaluatedKey": CURSOR, "Count": 2})
    second["request"][3]["ExclusiveStartKey"] = CURSOR
    reason, count, safe = COMPLETED, 19, NOMINAL
    if case in {"repetido", "query_esgotada", "scan_esgotada"}:
        cursor = CURSOR if case == "repetido" else V | {}
        cursor = {key: cursor[key] for key in ("pk", "sk")}
        second["reply"] = _data({"Items": [], "LastEvaluatedKey": cursor})
        reason = (
            "probe=ddb_ttl_cursor_invalid"
            if case == "repetido"
            else ("probe=ddb_ttl_pages_exhausted")
        )
        count, safe = index + 5, NOMINAL[: index - 3]
    if case in {"estrangeiro", "null"}:
        cursor = {**CURSOR, "pk": {"S": CANARY}} if case == "estrangeiro" else None
        first["reply"] = _data({"Items": [], "LastEvaluatedKey": cursor})
        reason, count, safe = "probe=ddb_ttl_cursor_invalid", index + 4, NOMINAL[: index - 3]
    steps[index : index + 1] = [first, second]
    _run(ttl_lab, steps, (count, reason, safe, []))


@pytest.mark.parametrize(
    "case",
    [
        "esparso",
        "estrangeiro",
        "duplicado",
        "drift",
        "inclusao_quatro",
        "remocao_quatro",
        "remove_chave",
        "base",
    ],
)
def test_gsi_esparso_observa_inclusao_e_remocao_com_lag_limitado(
    ttl_lab: TtlGsiLab, case: str
) -> None:
    steps, safe, sleeps = _steps(), NOMINAL[:3], []
    index, reason = 10, "probe=ddb_gsi_response_invalid"
    rows = {
        "esparso": [INDEXED, S],
        "estrangeiro": [INDEXED, {**INDEXED, "pk": {"S": CANARY}}],
        "duplicado": [INDEXED, INDEXED],
        "drift": [{**INDEXED, "gsi6sk": {"S": CANARY}}],
    }
    if case in rows:
        steps[index]["reply"] = _data({"Items": rows[case]})
    elif case in {"inclusao_quatro", "remocao_quatro"}:
        index = 10 if case == "inclusao_quatro" else 13
        poll = deepcopy(steps[index])
        poll["reply"] = _data({"Items": [] if index == 10 else [INDEXED]})
        steps[index : index + 1] = [deepcopy(poll) for _ in range(4)]
        reason = "probe=ddb_gsi_" + ("inclusion" if index == 10 else "removal") + "_unobserved"
        safe = NOMINAL[: 3 if index == 10 else 4] + [
            (INCLUDE if index == 10 else REMOVE, ("unobserved", 4, 4))
        ]
        sleeps, index = [1.0] * 3, index + 3
    else:
        index = 12 if case == "remove_chave" else 9
        steps[index]["reply"] = _data(
            {"Item": {**BASE, "gsi6pk": INDEXED["gsi6pk"]}} if case == "remove_chave" else {}
        )
        safe = NOMINAL[: 4 if index == 12 else 3]
    _run(ttl_lab, steps, (index + 4, reason, safe, sleeps))


@pytest.mark.parametrize("case", ["sucesso", "local", "denied", "timeout"])
def test_consistencia_forte_no_gsi_exige_rejeicao_remota(ttl_lab: TtlGsiLab, case: str) -> None:
    steps = _steps()
    replies = {
        "sucesso": _data({}),
        "local": {**_remote("ValidationException"), "exit": 252},
        "denied": _remote("AccessDeniedException"),
        "timeout": {"timeout": CANARY},
    }
    steps[14]["reply"] = replies[case]
    _run(ttl_lab, steps, (18, "probe=ddb_gsi_rejection_invalid", NOMINAL[:5], []))


TRANSPORT = [
    ("put", 0, _remote("AccessDeniedException")),
    ("get", 3, {"timeout": CANARY}),
    ("json", 4, {"exit": 0, "raw": CANARY}),
    ("inclusao", 10, {"timeout": CANARY}),
    ("remove", 11, {"timeout": CANARY}),
    ("remocao", 13, _remote("AccessDeniedException")),
]


@pytest.mark.parametrize("case", [pytest.param(row, id=row[0]) for row in TRANSPORT])
def test_transporte_para_sem_retry_e_preserva_registros(ttl_lab: TtlGsiLab, case: Any) -> None:
    _, index, reply = case
    steps = _steps()
    steps[index]["reply"] = reply
    safe = NOMINAL[: {0: 0, 3: 0, 4: 1, 10: 3, 11: 4, 13: 4}[index]]
    _run(ttl_lab, steps, (index + 4, "service=request_failed", safe, []))


@pytest.mark.parametrize(
    "case", ["saldo125", "saldo115", "saldo112", "apos_preparo", "pausa_inclusao", "pausa_remocao"]
)
def test_reserva_impede_chamada_sem_debito(ttl_lab: TtlGsiLab, case: str) -> None:
    steps, safe, sleeps = _steps(), [], []
    reason = "budget=requests" if case.startswith("saldo") else "budget=deadline"
    count = {
        "saldo125": 3,
        "saldo115": 13,
        "saldo112": 16,
        "apos_preparo": 3,
        "pausa_inclusao": 14,
        "pausa_remocao": 17,
    }[case]
    if case.startswith("saldo"):
        ttl_lab.start = int(case[5:])
        for _ in range(ttl_lab.start):
            ttl_lab.original.budget.acquire("work")
        safe = NOMINAL[: {3: 0, 13: 3, 16: 4}[count]]
    elif case == "apos_preparo":
        ttl_lab.advance_after = 3
    else:
        ttl_lab.current = 2000000059.75
        index = 10 if case == "pausa_inclusao" else 13
        steps[index]["reply"] = _data({"Items": [] if index == 10 else [INDEXED]})
        safe, sleeps = NOMINAL[: 3 if index == 10 else 4], [0.25]
    _run(ttl_lab, steps, (count, reason, safe, sleeps))


@pytest.mark.parametrize("case", ["query_invalida", "remove_falha", "captura"])
def test_registros_info_sao_observacoes_seguras_e_dependem_de_captura(
    ttl_lab: TtlGsiLab, case: str
) -> None:
    steps = _steps()
    if case == "query_invalida":
        steps[4]["reply"] = _data({"Count": 2})
        _run(ttl_lab, steps, (8, "probe=ddb_ttl_response_invalid", NOMINAL[:1], []))
    elif case == "remove_falha":
        steps[11]["reply"] = {"timeout": CANARY}
        _run(ttl_lab, steps, (15, "service=request_failed", NOMINAL[:4], []))
    else:
        handler = BufferingHandler(20)
        handler.setLevel(logging.WARNING)
        root, previous = logging.getLogger(), logging.getLogger().level
        root.addHandler(handler)
        try:
            _run(ttl_lab, steps, (18, COMPLETED, NOMINAL, []))
            assert len(records(ttl_lab.privacy.caplog)) == 6
            assert handler.buffer == []
        finally:
            root.removeHandler(handler)
            root.setLevel(previous)
            handler.close()


def test_oraculos_independentes_rejeitam_oito_mutacoes_finitas(
    ttl_lab: TtlGsiLab, record_property: Any
) -> None:
    steps = _steps()
    for index, key, value in [
        (4, "FilterExpression", "expires_at > :now"),
        (10, "ConsistentRead", True),
        (10, "IndexName", "gsi5"),
        (11, "UpdateExpression", "REMOVE #gpk"),
    ]:
        altered = deepcopy(steps)
        altered[index]["request"][3][key] = value
        with pytest.raises(AssertionError):
            equal([step["request"] for step in altered], _requests())
    for value in (1, 1.0):
        with pytest.raises(AssertionError):
            equal({"ConsistentRead": value}, {"ConsistentRead": True})
    record_property("shared_acceptance_controls", canonical(acceptance_controls(NOMINAL)))
    assert ttl_lab.read.process.calls == []


GSI_CURSOR_B = {
    "pk": {"S": "cap#a1b2c3d4#gsi"},
    "sk": {"S": "indexed#002"},
    "gsi6pk": {"S": "cap#a1b2c3d4#pending"},
    "gsi6sk": {"S": "item#002"},
}


def _two_pages(request: Any, final: Any, cursor: Any) -> list[Any]:
    second = deepcopy(request)
    second[3]["ExclusiveStartKey"] = deepcopy(cursor)
    return [
        {
            "request": deepcopy(request),
            "reply": _data({"Items": [], "Count": 2, "LastEvaluatedKey": deepcopy(cursor)}),
        },
        {"request": second, "reply": _data(final)},
    ]


def _maximum_pages(steps: list[Any]) -> list[Any]:
    ttl = [
        page
        for index in (4, 5)
        for page in _two_pages(steps[index]["request"], {"Items": [E, V]}, CURSOR)
    ]
    phases = []
    for removal in (False, True):
        pages = []
        for poll in range(4):
            present = poll < 3 if removal else poll == 3
            pages.extend(
                _two_pages(
                    steps[13 if removal else 10]["request"],
                    {"Items": [INDEXED] if present else []},
                    INDEXED,
                )
            )
        phases.append(pages)
    return [*steps[:4], *ttl, *steps[6:10], *phases[0], *steps[11:13], *phases[1], steps[14]]


@pytest.mark.parametrize(
    "case", ["maximo34", "inclusao_repetida", "inclusao_esgotada", "remocao_esgotada"]
)
def test_gsi_paginado_separa_paginas_polls_e_limites(ttl_lab: TtlGsiLab, case: str) -> None:
    steps = _steps()
    if case == "maximo34":
        safe = deepcopy(NOMINAL)
        safe[3:5] = [(INCLUDE, ("observed", 4, 8)), (REMOVE, ("observed", 4, 8))]
        _run(ttl_lab, _maximum_pages(steps), (34, COMPLETED, safe, [1.0] * 6))
        assert ttl_lab.anchor_calls == 1
    else:
        index = 13 if case == "remocao_esgotada" else 10
        cursor = INDEXED if case == "inclusao_repetida" else GSI_CURSOR_B
        pages = _two_pages(
            steps[index]["request"], {"Items": [], "LastEvaluatedKey": cursor}, INDEXED
        )
        steps[index : index + 1] = pages
        suffix = "cursor_invalid" if case == "inclusao_repetida" else "pages_exhausted"
        safe = NOMINAL[: 4 if index == 13 else 3]
        _run(ttl_lab, steps, (index + 5, "probe=ddb_gsi_" + suffix, safe, []))
