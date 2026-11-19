"""Cursores fechados limitam paginas, transicoes e chamadas fisicas Cognito."""

import json

import pytest

from tests.integration.aws_capability._inventory import pages
from tests.integration.aws_capability._safety import SafetyFailure
from tests.integration.aws_capability._service_read_assertions import (
    FIXED_FLAGS,
    assert_environment,
    assert_process,
)
from tests.integration.aws_capability._service_read_testing import ReadLab
from tests.integration.aws_capability.contracts import BudgetExhausted, Limits, Page, ServiceFailure
from tests.integration.aws_capability.test_service_cognito_children_lookup import (
    CURSORS,
    DATA,
    assert_failure,
    enqueue,
    read_lab,
    remote_error,
    resource,
    trace,
)

__all__ = ["read_lab"]


def cursor(name: str | None) -> str | None:
    return None if name is None else CURSORS["cursors"][name]


def enqueue_page(lab: ReadLab, name: str) -> None:
    enqueue(lab, "root", "tags")
    reply = CURSORS["replies"].get(name, DATA["replies"].get(name))
    lab.process.outcomes.append((0, json.dumps(reply), ""))


def page_trace(name: str) -> list[tuple]:
    final = CURSORS["calls"].get(name, DATA["calls"].get(name))
    return [*trace("root", "tags"), tuple(final)]


@pytest.mark.parametrize("index", range(7))
def test_uma_chamada_consume_somente_a_colecao_corrente_com_cursor_literal(
    read_lab: ReadLab, index: int
) -> None:
    case = CURSORS["pages"][index]
    enqueue_page(read_lab, case["reply"])
    page = read_lab.services.dependents(resource("pool"), cursor(case["input"]), read_lab.context)
    assert page == Page(tuple(resource(name) for name in case["resources"]), cursor(case["output"]))
    assert_process(read_lab, page_trace(case["call"]))


def test_sete_paginas_reais_consumem_vinte_e_uma_chamadas_sem_listas_ocultas(
    read_lab: ReadLab,
) -> None:
    expected = []
    for case in CURSORS["pages"]:
        enqueue_page(read_lab, case["reply"])
        expected.extend(page_trace(case["call"]))
    found = pages(
        lambda value: read_lab.services.dependents(resource("pool"), value, read_lab.context), 8
    )
    assert found == [
        resource(name)
        for name in ("domain", "custom", "client", "client2", "server", "server2", "user", "user2")
    ]
    assert read_lab.context.budget.used == DATA["expected"]["seven_page_calls"] == 21
    assert read_lab.context.budget.limits == Limits()
    assert_process(read_lab, expected)


def test_colecoes_vazias_avancam_em_quatro_chamadas_independentes(read_lab: ReadLab) -> None:
    cases = [
        (None, "root", "domains", "clients_start"),
        ("clients_start", "clients_empty", "clients", "empty_servers"),
        ("empty_servers", "servers_empty", "servers", "empty_users"),
        ("empty_users", "users_empty", "users", None),
    ]
    expected = []
    for start, reply, call, end in cases:
        enqueue_page(read_lab, reply)
        assert read_lab.services.dependents(
            resource("pool"), cursor(start), read_lab.context
        ) == Page((), cursor(end))
        expected.extend(page_trace(call))
        assert_process(read_lab, expected)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("v", True),
        ("v", 1.0),
        ("v", 2),
        ("run", "e5f6a7b8"),
        ("service", "iam"),
        ("parent", None),
        ("parent", "us-east-2_foreign"),
        ("kind", "cognito.pools"),
        ("kind", "cognito.branding"),
        ("markers", {"PaginationToken": "client-A"}),
        ("markers", {"NextToken": ""}),
        ("markers", {"NextToken": None}),
        ("markers", {}),
        ("markers", {"NextToken": 1}),
        ("markers", {"NextToken": "client-A", "extra": "x"}),
        ("seen", []),
        ("seen", ["x"]),
        ("seen", "invalid"),
        ("seen", ["0" * 64]),
        ("seen", ["0" * 64] * 2),
        ("seen", ["0" * 64] * 9),
        (
            "seen",
            [
                "0" * 64,
                "1" * 64,
                "2" * 64,
                "3" * 64,
                "4" * 64,
                "5" * 64,
                "6" * 64,
                "7" * 64,
                "71deb72c7774478bc7d3867d047ba0414bed5aec5a302c04fe9f26d05c6e3dec",
            ],
        ),
        ("extra", "x"),
    ],
)
def test_cursor_de_filho_invalido_recusa_antes_de_qualquer_io(
    read_lab: ReadLab, field: str, value: object
) -> None:
    invalid = json.loads(CURSORS["cursors"]["clients_next"])
    invalid[field] = value
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("pool"), json.dumps(invalid), read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


@pytest.mark.parametrize("raw", ["{", "null", "[]", "true", "", '{"v":1,"v":1}'])
def test_cursor_json_malformado_duplicado_ou_tipo_invalido_recusa_sem_io(
    read_lab: ReadLab, raw: str
) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("pool"), raw, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


@pytest.mark.parametrize("field", ["v", "run", "service", "parent", "kind", "markers", "seen"])
def test_cursor_com_campo_ausente_recusa_sem_io(read_lab: ReadLab, field: str) -> None:
    invalid = json.loads(CURSORS["cursors"]["clients_next"])
    del invalid[field]
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("pool"), json.dumps(invalid), read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, [])


def test_cursor_repetido_na_resposta_nao_e_fim_da_colecao(read_lab: ReadLab) -> None:
    enqueue_page(read_lab, "clients_first")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("pool"), CURSORS["cursors"]["clients_next"], read_lab.context
        )
    assert error.value.args == ("service=cursor_cycle",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, page_trace("clients_next"))


def test_ciclo_a_b_a_nao_pode_reintroduzir_fingerprint_anterior(read_lab: ReadLab) -> None:
    enqueue_page(read_lab, "clients_first")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(
            resource("pool"), CURSORS["cursors"]["clients_b"], read_lab.context
        )
    assert error.value.args == ("service=cursor_cycle",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, page_trace("clients_b"))


def test_replay_do_mesmo_cursor_refaz_readbacks_e_pagina(read_lab: ReadLab) -> None:
    enqueue_page(read_lab, "clients_last")
    enqueue_page(read_lab, "clients_last")
    for _ in range(2):
        assert read_lab.services.dependents(
            resource("pool"), CURSORS["cursors"]["clients_next"], read_lab.context
        ) == Page((resource("client2"),), CURSORS["cursors"]["servers_start"])
    assert_process(read_lab, page_trace("clients_next") * 2)


def test_falha_na_segunda_pagina_nao_vira_fim_ou_retry(read_lab: ReadLab) -> None:
    enqueue_page(read_lab, "domains")
    enqueue(read_lab, "root", "tags")
    remote_error(read_lab, "NotAuthorizedException")
    with pytest.raises(ServiceFailure) as error:
        pages(
            lambda value: read_lab.services.dependents(resource("pool"), value, read_lab.context),
            8,
        )
    assert_failure(error.value, read_lab)
    assert_process(read_lab, page_trace("domains") + page_trace("clients"))


def test_oito_paginas_incompletas_esgotam_limite_real_sem_nona_lista(read_lab: ReadLab) -> None:
    enqueue_page(read_lab, "root")
    for token in (
        "client-A",
        "client-B",
        "client-C",
        "client-D",
        "client-E",
        "client-F",
        "client-G",
    ):
        enqueue(read_lab, "root", "tags")
        read_lab.process.outcomes.append(
            (0, json.dumps({"UserPoolClients": [], "NextToken": token}), "")
        )
    with pytest.raises(SafetyFailure) as error:
        pages(
            lambda value: read_lab.services.dependents(resource("pool"), value, read_lab.context),
            8,
        )
    assert error.value.reason == "inventory_incomplete"
    assert_failure(error.value, read_lab)
    expected = page_trace("domains") + page_trace("clients")
    for payload in (
        {"UserPoolId": "us-east-2_X9opaqueQ", "MaxResults": 60, "NextToken": "client-A"},
        {"UserPoolId": "us-east-2_X9opaqueQ", "MaxResults": 60, "NextToken": "client-B"},
        {"UserPoolId": "us-east-2_X9opaqueQ", "MaxResults": 60, "NextToken": "client-C"},
        {"UserPoolId": "us-east-2_X9opaqueQ", "MaxResults": 60, "NextToken": "client-D"},
        {"UserPoolId": "us-east-2_X9opaqueQ", "MaxResults": 60, "NextToken": "client-E"},
        {"UserPoolId": "us-east-2_X9opaqueQ", "MaxResults": 60, "NextToken": "client-F"},
    ):
        expected.extend(trace("root", "tags"))
        expected.append(("cognito-idp", "list-user-pool-clients", "us-east-2", payload))
    assert_process(read_lab, expected)
    assert read_lab.context.budget.limits == Limits()


@pytest.mark.parametrize("cut", [0, 1, 2, 3])
def test_deadline_interrompe_antes_do_proximo_readback_de_branding(
    read_lab: ReadLab, cut: int
) -> None:
    script = ["root", "tags", "client", "branding"]
    enqueue(read_lab, *script[:cut])
    read_lab.context.budget.now = lambda: 1060 if len(read_lab.process.calls) >= cut else 1000
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.dependents(resource("client"), None, read_lab.context)
    assert error.value.args == ("budget=deadline",)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, trace(*script[:cut]))
    assert read_lab.context.budget.limits == Limits()


@pytest.mark.parametrize("cut", [0, 1, 2, 3])
def test_reserva_real_interrompe_branding_sem_consumir_chamada_extra(
    read_lab: ReadLab, cut: int
) -> None:
    for _ in range(40 - cut):
        read_lab.context.budget.acquire("work")
    script = ["root", "tags", "client", "branding"]
    enqueue(read_lab, *script[:cut])
    with pytest.raises(BudgetExhausted) as error:
        read_lab.services.dependents(resource("client"), None, read_lab.context)
    assert error.value.args == ("budget=requests",)
    assert_failure(error.value, read_lab)
    assert read_lab.context.budget.used == read_lab.context.budget.work_used == 40
    assert read_lab.process.observed_debits == list(range(41 - cut, 41))
    assert len(read_lab.process.calls) == cut
    assert read_lab.process.outcomes == []
    for (argv, options), (_, operation, region, payload) in zip(
        read_lab.process.calls, trace(*script[:cut]), strict=True
    ):
        assert argv == [
            "aws",
            "cognito-idp",
            operation,
            "--region",
            region,
            *FIXED_FLAGS,
            "7",
            "--cli-read-timeout",
            "7",
        ]
        assert json.loads(options["input"]) == payload
        assert_environment(options["env"], region)
        assert options["timeout"] == 7
        assert options["shell"] is False
        assert options["capture_output"] is True
        assert options["text"] is True
        assert options["check"] is False
    assert read_lab.context.budget.limits == Limits()


@pytest.mark.parametrize(
    "collection_cursor_marker",
    [
        ("clients", "clients_start", "NextToken"),
        ("servers", "empty_servers", "NextToken"),
        ("users", "empty_users", "PaginationToken"),
    ],
)
@pytest.mark.parametrize("value", ["", None, 1, False, []])
def test_marker_de_resposta_invalido_nao_e_fim_da_colecao(
    read_lab: ReadLab, collection_cursor_marker: tuple[str, str, str], value: object
) -> None:
    collection, start, marker = collection_cursor_marker
    enqueue(read_lab, "root", "tags")
    reply = dict(DATA["replies"][collection + "_empty"])
    reply[marker] = value
    read_lab.process.outcomes.append((0, json.dumps(reply), ""))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.dependents(resource("pool"), cursor(start), read_lab.context)
    assert_failure(error.value, read_lab)
    assert_process(read_lab, page_trace(collection))


def test_marker_privado_valido_vai_so_ao_stdin_e_cursor_interno(read_lab: ReadLab) -> None:
    enqueue_page(read_lab, "clients_last")
    page = read_lab.services.dependents(resource("pool"), cursor("private_next"), read_lab.context)
    assert page == Page((resource("client2"),), cursor("private_servers"))
    assert_process(read_lab, page_trace("private_next"))
