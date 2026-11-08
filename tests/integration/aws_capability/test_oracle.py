"""Controles positivos e mutações independentes do oráculo de inventário."""

from dataclasses import replace

import pytest

from tests.integration.aws_capability._oracle import (
    Trace,
    assert_complete,
    assert_no_mutation,
    assert_probe_order,
)
from tests.integration.aws_capability.contracts import Outcome, Page, Resource


def sample() -> tuple[Outcome, Trace]:
    outcome = Outcome(
        "succeeded",
        "complete",
        (),
        {
            "inventory_complete": True,
            "counts": {"cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0},
        },
    )
    events = Trace(
        [
            ("delete", "synthetic-owned", True),
            ("inventory_result", "cognito", None, Page()),
            ("inventory_result", "dynamodb", None, Page()),
            ("inventory_result", "s3", None, Page()),
            ("inventory_result", "iam", None, Page()),
        ]
    )
    return outcome, events


def test_controle_positivo_do_oraculo_aceita_evidencia_completa() -> None:
    outcome, events = sample()
    assert_complete(outcome, events)


@pytest.mark.parametrize("mutation", ["unknown", "residual", "incomplete", "count", "status"])
def test_oraculo_rejeita_resultado_mutante(mutation: str) -> None:
    outcome, events = sample()
    changes = {
        "unknown": {"remaining": None},
        "residual": {"remaining": (Resource("s3", "residual", "cnesdata-cap-a1b2c3d4"),)},
        "incomplete": {"evidence": {**outcome.evidence, "inventory_complete": False}},
        "count": {"evidence": {"inventory_complete": True, "counts": {"s3": 1}}},
        "status": {"status": "incomplete"},
    }
    with pytest.raises(AssertionError):
        assert_complete(replace(outcome, **changes[mutation]), events)


@pytest.mark.parametrize("mutation", ["missing_service", "stale_before_delete"])
def test_oraculo_rejeita_transcrito_mutante(mutation: str) -> None:
    outcome, events = sample()
    if mutation == "missing_service":
        events.pop()
    else:
        events.append(("delete", "late-resource", True))
    with pytest.raises(AssertionError):
        assert_complete(outcome, events)


def test_controle_positivo_sem_mutacao_aceita_apenas_leituras() -> None:
    assert_no_mutation(
        [("identity",), ("inventory_result", "iam", None, Page()), ("inspect", "foreign")]
    )


@pytest.mark.parametrize("operation", ["create", "probe", "delete"])
def test_oraculo_de_nao_mutacao_detecta_operacao_proibida(operation: str) -> None:
    with pytest.raises(AssertionError):
        assert_no_mutation([("identity",), (operation, "foreign")])


def test_oraculo_rejeita_transcrito_reutilizado_sem_nova_execucao() -> None:
    outcome, events = sample()
    assert_complete(outcome, events)
    with pytest.raises(AssertionError, match="stale_execution"):
        assert_complete(outcome, events)


@pytest.mark.parametrize(
    "mutation", ["initial", "terminal", "interrupted", "repeat", "error", "residual"]
)
def test_oraculo_rejeita_paginas_finais_invalidas(mutation: str) -> None:
    outcome, events = sample()
    changes = {
        "initial": [("inventory_result", "iam", "unconsumed", Page())],
        "terminal": [("inventory_result", "iam", None, Page(cursor="more"))],
        "interrupted": [
            ("inventory_result", "iam", None, Page(cursor="more")),
            ("inventory_result", "iam", None, Page()),
        ],
        "repeat": [
            ("inventory_result", "iam", None, Page(cursor="more")),
            ("inventory_result", "iam", "more", Page(cursor="more")),
        ],
        "error": [("inventory_error", "iam", None)],
        "residual": [
            (
                "inventory_result",
                "iam",
                None,
                Page((Resource("iam", "leftover", "cnesdata-cap-a1b2c3d4"),)),
            )
        ],
    }
    events[-1:] = changes[mutation]
    with pytest.raises(AssertionError):
        assert_complete(outcome, events)


def test_oraculo_aceita_paginacao_final_completa() -> None:
    outcome, events = sample()
    events[-1:] = [
        ("inventory_result", "iam", None, Page(cursor="more")),
        ("inventory_result", "iam", "more", Page()),
    ]
    assert_complete(outcome, events)


@pytest.mark.parametrize("foreign_resources", [False, True])
def test_oraculo_aceita_duas_varreduras_completas_com_mesmo_cursor(
    foreign_resources: bool,
) -> None:
    outcome, _ = sample()
    events = Trace()
    resources = (Resource("iam", "foreign", "cnesdata-cap-eeeeffff"),) if foreign_resources else ()
    for _ in range(2):
        for service in ("cognito", "dynamodb", "s3"):
            events.append(("inventory_result", service, None, Page()))
        events.append(("inventory_result", "iam", None, Page(resources, "same-cursor")))
        events.append(("inventory_result", "iam", "same-cursor", Page()))
    assert_complete(outcome, events)


def probe_trace() -> list[tuple[object, ...]]:
    resources = tuple(
        Resource(service, service, "run") for service in ("cognito", "dynamodb", "s3", "iam")
    )
    return [
        *(("created", resource) for resource in resources),
        ("probe", resources, "spec"),
        ("delete", "s3", True),
    ]


def test_oraculo_probe_aceita_referencias_e_ordem_corretas() -> None:
    assert_probe_order(probe_trace(), "spec")


@pytest.mark.parametrize("mutation", ["empty", "wrong", "early", "after_delete", "spec"])
def test_oraculo_probe_rejeita_referencias_ou_ordem_incorretas(mutation: str) -> None:
    events = probe_trace()
    if mutation == "empty":
        events[4] = ("probe", (), "spec")
    elif mutation == "wrong":
        events[4] = ("probe", (Resource("iam", "foreign", "run"),), "spec")
    elif mutation == "spec":
        events[4] = ("probe", events[4][1], "other")
    elif mutation == "early":
        events.insert(0, events.pop(4))
    else:
        events.append(events.pop(4))
    with pytest.raises(AssertionError):
        assert_probe_order(events, "spec")
