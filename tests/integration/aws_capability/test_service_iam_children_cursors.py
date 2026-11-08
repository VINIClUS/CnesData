"""Cursores IAM conservam escopo, transicoes e teto real de oito paginas."""

import json

import pytest

from tests.integration.aws_capability._inventory import pages
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import ReadLab, respond
from tests.integration.aws_capability.contracts import Limits, Page, ServiceFailure
from tests.integration.aws_capability.test_service_iam_children_lookup import (
    CURSORS,
    DATA,
    assert_failure,
    enqueue,
    read_lab,
    resource,
    trace,
)

__all__ = ["read_lab"]


@pytest.mark.parametrize("collection", ["policies", "attachments", "profiles"])
def test_duas_paginas_por_colecao_preservam_marker_e_refazem_posse(
    read_lab: ReadLab, collection: str
) -> None:
    enqueue(read_lab, "root", "tags", collection + "_first")
    cursor = None if collection == "policies" else CURSORS["cursors"][collection + "_start"]
    first = read_lab.services.dependents(resource("role"), cursor, read_lab.context)
    assert first == Page((), CURSORS["cursors"][collection + "_next"])
    assert_process(read_lab, trace("root", "tags", collection))
    enqueue(read_lab, "root", "tags", collection + "_last")
    second = read_lab.services.dependents(resource("role"), first.cursor, read_lab.context)
    next_cursor = {
        "policies": CURSORS["cursors"]["attachments_from_policies"],
        "attachments": CURSORS["cursors"]["profiles_from_attachments"],
        "profiles": None,
    }[collection]
    assert second == Page((), next_cursor)
    assert_process(
        read_lab, trace("root", "tags", collection, "root", "tags", collection + "_next")
    )


def test_tres_terminais_vazios_sem_flag_so_terminam_apos_profiles(read_lab: ReadLab) -> None:
    enqueue(
        read_lab,
        "root",
        "tags",
        "policies_empty",
        "root",
        "tags",
        "attachments_empty",
        "root",
        "tags",
        "profiles_empty",
    )
    first = read_lab.services.dependents(resource("role"), None, read_lab.context)
    assert first == Page((), CURSORS["cursors"]["attachments_start"])
    assert len(read_lab.process.calls) == 3
    second = read_lab.services.dependents(resource("role"), first.cursor, read_lab.context)
    assert second == Page((), CURSORS["cursors"]["profiles_from_empty_attachments"])
    assert len(read_lab.process.calls) == 6
    assert read_lab.services.dependents(resource("role"), second.cursor, read_lab.context) == Page()
    assert_process(
        read_lab,
        trace(
            "root", "tags", "policies", "root", "tags", "attachments", "root", "tags", "profiles"
        ),
    )


@pytest.mark.parametrize(
    "pagination",
    [
        {"IsTruncated": True},
        {"IsTruncated": True, "Marker": ""},
        {"IsTruncated": True, "Marker": 2},
        {"IsTruncated": "false"},
        {"IsTruncated": False, "Marker": "policy-C4-private"},
        {"Marker": "policy-C4-private"},
        {"IsTruncated": True, "Marker": None},
    ],
)
def test_paginacao_incoerente_apos_policy_valida_nao_emite_pagina(
    read_lab: ReadLab, pagination: dict
) -> None:
    enqueue(read_lab, "root", "tags")
    respond(read_lab, dict(DATA["replies"]["policies"], **pagination))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("role"), None, read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=inventory_invalid",)
    assert_process(read_lab, trace("root", "tags", "policies"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("v", True),
        ("v", 1.0),
        ("run", "foreign"),
        ("service", "s3"),
        ("parent", "OtherRole"),
        ("kind", "iam.roles"),
        ("markers", {"NextToken": "x"}),
        ("markers", {"Marker": None}),
        ("markers", {"Marker": ""}),
        ("seen", []),
        ("seen", ["bad"]),
        ("extra", "x"),
    ],
)
def test_cursor_fechado_invalido_recusa_antes_de_readback(
    read_lab: ReadLab, field: str, value: object
) -> None:
    cursor = json.loads(CURSORS["cursors"]["policies_next"])
    cursor[field] = value
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("role"), json.dumps(cursor), read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=cursor_invalid",)
    assert_process(read_lab, [])


def test_chave_duplicada_do_cursor_e_rejeitada_antes_de_io(read_lab: ReadLab) -> None:
    cursor = CURSORS["cursors"]["policies_next"].replace('"v":1', '"v":1,"v":1')
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("role"), cursor, read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=cursor_invalid",)
    assert_process(read_lab, [])


def test_marker_repetido_nao_e_pagina_terminal(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags", "policies_first", "root", "tags", "policies_first")
    first = read_lab.services.dependents(resource("role"), None, read_lab.context)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("role"), first.cursor, read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=cursor_cycle",)
    assert_process(read_lab, trace("root", "tags", "policies", "root", "tags", "policies_next"))


def test_ciclo_a_b_a_nao_perde_historico_de_markers(read_lab: ReadLab) -> None:
    enqueue(
        read_lab,
        "root",
        "tags",
        "policies_first",
        "root",
        "tags",
        "policies_b",
        "root",
        "tags",
        "policies_first",
    )
    first = read_lab.services.dependents(resource("role"), None, read_lab.context)
    second = read_lab.services.dependents(resource("role"), first.cursor, read_lab.context)
    assert second == Page((), CURSORS["cursors"]["policies_b"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("role"), second.cursor, read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=cursor_cycle",)
    assert_process(
        read_lab,
        trace(
            "root",
            "tags",
            "policies",
            "root",
            "tags",
            "policies_next",
            "root",
            "tags",
            "policies_b",
        ),
    )


def test_pages_real_interrompe_apos_oito_paginas_e_24_requests(read_lab: ReadLab) -> None:
    for reply in CURSORS["limit_replies"]:
        enqueue(read_lab, "root", "tags")
        respond(read_lab, reply)
    with pytest.raises(SafetyFailure) as error:
        pages(
            lambda cursor: read_lab.services.dependents(resource("role"), cursor, read_lab.context),
            8,
        )
    assert_failure(error.value, read_lab)
    assert error.value.reason == "inventory_incomplete"
    expected = []
    for call in CURSORS["limit_calls"]:
        expected.extend([*trace("root", "tags"), tuple(call)])
    assert_process(read_lab, expected)
    assert read_lab.context.budget.used == read_lab.context.budget.work_used == 24
    assert read_lab.context.budget.limits == Limits()


def test_folha_policy_recusa_cursor_antes_da_inspecao(read_lab: ReadLab) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("policy"), CURSORS["cursors"]["policies_next"], read_lab.context
        )
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=cursor_invalid",)
    assert_process(read_lab, [])


@pytest.mark.parametrize("collection", ["attachments", "profiles"])
def test_dependencia_externa_da_segunda_pagina_nao_e_descartada(
    read_lab: ReadLab, collection: str
) -> None:
    enqueue(
        read_lab,
        "root",
        "tags",
        collection + "_first",
        "root",
        "tags",
        "attachment" if collection == "attachments" else "profile",
    )
    first = read_lab.services.dependents(
        resource("role"), CURSORS["cursors"][collection + "_start"], read_lab.context
    )
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("role"), first.cursor, read_lab.context)
    assert_failure(error.value, read_lab)
    assert error.value.args == ("service=unexpected_external_dependency",)
    assert_process(
        read_lab, trace("root", "tags", collection, "root", "tags", collection + "_next")
    )


def test_policies_de_duas_paginas_preservam_os_dois_ids_literais(read_lab: ReadLab) -> None:
    enqueue(read_lab, "root", "tags")
    respond(
        read_lab, dict(DATA["replies"]["policies"], IsTruncated=True, Marker="policy-C4-private")
    )
    first = read_lab.services.dependents(resource("role"), None, read_lab.context)
    assert first == Page((resource("policy"),), CURSORS["cursors"]["policies_next"])
    enqueue(read_lab, "root", "tags")
    respond(read_lab, {"PolicyNames": ["OtherPolicy"], "IsTruncated": False})
    second = read_lab.services.dependents(resource("role"), first.cursor, read_lab.context)
    assert second == Page((resource("second"),), CURSORS["cursors"]["attachments_from_policies"])
    assert_process(read_lab, trace("root", "tags", "policies", "root", "tags", "policies_next"))
