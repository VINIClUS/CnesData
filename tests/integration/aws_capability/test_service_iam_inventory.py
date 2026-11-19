"""Inventário IAM valida paginação, cursor fechado e requests físicas."""

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import assert_process
from tests.integration.aws_capability._service_roots_testing import (
    ReadLab,
    load_fixture,
    make_read_lab,
)
from tests.integration.aws_capability.contracts import Page, Resource, ServiceFailure

FIXTURES = Path(__file__).parent / "fixtures" / "cli" / "r4"
LIST = ("iam", "list-roles", "us-east-1", {"MaxItems": 100, "PathPrefix": "/cnesdata-cap/"})
NEXT = (
    "iam",
    "list-roles",
    "us-east-1",
    {"MaxItems": 100, "PathPrefix": "/cnesdata-cap/", "Marker": "role-next"},
)


def data(name: str) -> dict:
    return load_fixture(FIXTURES / f"{name}.json")


def reply(lab: ReadLab, value: object) -> None:
    lab.process.outcomes.append((0, json.dumps(value), ""))


def cursor(name: str = "valid") -> str:
    return json.dumps(data("cursors")[name], sort_keys=True, separators=(",", ":"))


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[ReadLab]:
    lab = make_read_lab(monkeypatch, tmp_path, caplog, capsys)
    yield lab
    lab.check_private()


def test_inventario_iam_preserva_role_name_e_uma_pagina_por_chamada(read_lab: ReadLab) -> None:
    reply(read_lab, data("iam")["first"])
    first = read_lab.services.inventory("iam", None, read_lab.context)
    assert first.resources == (
        Resource("iam", "CapRole", "CapRole"),
        Resource("iam", "cnesdata-cap-a1b2c3d4-worker", "cnesdata-cap-a1b2c3d4-worker"),
    )
    assert first.cursor is not None
    assert json.loads(first.cursor) == data("cursors")["valid"]
    assert_process(read_lab, [LIST])


def test_continuacao_iam_envia_marker_somente_apos_truncamento(read_lab: ReadLab) -> None:
    reply(read_lab, data("iam")["first"])
    first = read_lab.services.inventory("iam", None, read_lab.context)
    reply(read_lab, data("iam")["second"])
    second = read_lab.services.inventory("iam", first.cursor, read_lab.context)
    assert second == Page((Resource("iam", "IndependentRole", "IndependentRole"),), None)
    assert_process(read_lab, [LIST, NEXT])


def test_pagina_vazia_truncada_nao_encerra_inventario(read_lab: ReadLab) -> None:
    reply(read_lab, data("iam")["empty_with_next"])
    first = read_lab.services.inventory("iam", None, read_lab.context)
    assert first.resources == ()
    assert first.cursor is not None
    assert json.loads(first.cursor) == data("cursors")["valid"]
    reply(read_lab, data("iam")["second"])
    assert read_lab.services.inventory("iam", first.cursor, read_lab.context).cursor is None
    assert_process(read_lab, [LIST, NEXT])


def test_reinicio_iam_sem_cursor_rele_sem_cache(read_lab: ReadLab) -> None:
    reply(read_lab, data("iam")["empty"])
    assert read_lab.services.inventory("iam", None, read_lab.context) == Page((), None)
    reply(read_lab, data("iam")["first"])
    assert read_lab.services.inventory("iam", None, read_lab.context).resources[0].name == "CapRole"
    assert_process(read_lab, [LIST, LIST])


@pytest.mark.parametrize("case", list(data("cursors")["invalid"]))
def test_cursor_iam_invalido_falha_antes_de_subprocesso(read_lab: ReadLab, case: str) -> None:
    value = json.dumps(data("cursors")["invalid"][case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("iam", value, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [])


@pytest.mark.parametrize("value", ["", "{", "[]", "null"])
def test_cursor_malformado_falha_antes_de_subprocesso(read_lab: ReadLab, value: str) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("iam", value, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    assert_process(read_lab, [])


@pytest.mark.parametrize("duplicate", ["envelope", "marker"])
def test_cursor_json_duplicado_e_rejeitado(read_lab: ReadLab, duplicate: str) -> None:
    value = (
        '{"run":"a1b2c3d4",' + cursor()[1:]
        if duplicate == "envelope"
        else cursor().replace('"Marker":"role-next"', '"Marker":"other","Marker":"role-next"')
    )
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("iam", value, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    assert_process(read_lab, [])


def test_marker_repetido_falha_ciclo_apos_uma_request(read_lab: ReadLab) -> None:
    reply(read_lab, data("iam")["repeat"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("iam", cursor(), read_lab.context)
    assert error.value.args == ("service=cursor_cycle",)
    assert_process(read_lab, [NEXT])


def test_ciclo_entre_paginas_preserva_historico_iam(read_lab: ReadLab) -> None:
    reply(read_lab, data("iam")["first"])
    first = read_lab.services.inventory("iam", None, read_lab.context)
    reply(read_lab, data("iam")["cycle_second"])
    second = read_lab.services.inventory("iam", first.cursor, read_lab.context)
    assert second.cursor is not None
    assert json.loads(second.cursor) == data("cursors")["cycle"]
    reply(read_lab, data("iam")["repeat"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("iam", second.cursor, read_lab.context)
    assert error.value.args == ("service=cursor_cycle",)
    assert_process(
        read_lab,
        [
            LIST,
            NEXT,
            (
                "iam",
                "list-roles",
                "us-east-1",
                {"MaxItems": 100, "PathPrefix": "/cnesdata-cap/", "Marker": "role-other"},
            ),
        ],
    )


@pytest.mark.parametrize(
    "case",
    [
        "missing_roles",
        "roles_map",
        "roles_null",
        "missing_name",
        "empty_name",
        "number_name",
        "wrong_path",
        "truncated_missing_marker",
        "truncated_empty_marker",
        "truncated_number_marker",
        "terminal_marker",
    ],
)
def test_resposta_iam_invalida_nao_vira_lista_vazia(read_lab: ReadLab, case: str) -> None:
    reply(read_lab, data("iam")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("iam", None, read_lab.context)
    assert error.value.args == ("service=inventory_invalid",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [LIST])


@pytest.mark.parametrize("case", ["remote_denied", "bad_json", "empty_stdout"])
def test_erro_de_listagem_iam_falha_sem_retry(read_lab: ReadLab, case: str) -> None:
    read_lab.process.outcomes.append(tuple(data("errors")[case]))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("iam", None, read_lab.context)
    assert error.value.args == ("service=request_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [LIST])


def test_listagem_negada_na_continuacao_nao_reinicia(read_lab: ReadLab) -> None:
    read_lab.process.outcomes.append(tuple(data("errors")["remote_denied"]))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("iam", cursor(), read_lab.context)
    assert error.value.args == ("service=request_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [NEXT])


@pytest.mark.parametrize("name", ["iam", "cursors", "errors"])
def test_fixture_iam_carrega_sem_chaves_duplicadas(name: str) -> None:
    assert isinstance(data(name), dict)


def test_loader_r4_rejeita_chave_duplicada_aninhada(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text('{"roles":{"RoleName":"a","RoleName":"b"}}')
    with pytest.raises(ValueError, match="duplicate_key="):
        load_fixture(path)
