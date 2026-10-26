"""Inventário DynamoDB paginado sem cache nem mistura de cursores."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import (
    assert_private,
    assert_process,
    assert_rejected,
)
from tests.integration.aws_capability._service_read_testing import (
    ReadLab,
    cursor_input,
    fixture_data,
    make_read_lab,
    respond,
    respond_error,
)
from tests.integration.aws_capability.contracts import Page, Resource, ServiceFailure


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


def test_inventario_consumiu_uma_pagina_aws_por_chamada(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("tables")["first"])
    first = read_lab.services.inventory("dynamodb", None, read_lab.context)
    assert first.resources == (
        Resource("dynamodb", "cnesdata-cap-a1b2c3d4", "cnesdata-cap-a1b2c3d4"),
        Resource("dynamodb", "legacy-table", "legacy-table"),
    )
    assert first.cursor is not None
    assert json.loads(first.cursor) == fixture_data("cursors")["valid"]
    assert_process(read_lab, [("dynamodb", "list-tables", "us-east-2", {"Limit": 100})])
    respond(read_lab, fixture_data("tables")["second"])
    second = read_lab.services.inventory("dynamodb", first.cursor, read_lab.context)
    assert second == Page((Resource("dynamodb", "production-table", "production-table"),), None)
    assert_process(
        read_lab,
        [
            ("dynamodb", "list-tables", "us-east-2", {"Limit": 100}),
            (
                "dynamodb",
                "list-tables",
                "us-east-2",
                {
                    "Limit": 100,
                    "ExclusiveStartTableName": "legacy-table",
                },
            ),
        ],
    )


def test_inventario_vazio_exige_resposta_remota_valida(read_lab: ReadLab) -> None:
    respond(read_lab, {"TableNames": []})
    assert read_lab.services.inventory("dynamodb", None, read_lab.context) == Page((), None)
    assert_process(read_lab, [("dynamodb", "list-tables", "us-east-2", {"Limit": 100})])


def test_reinicio_sem_cursor_rele_a_colecao_sem_cache(read_lab: ReadLab) -> None:
    respond(read_lab, {"TableNames": []})
    assert read_lab.services.inventory("dynamodb", None, read_lab.context) == Page((), None)
    respond(read_lab, {"TableNames": ["late-table"]})
    assert read_lab.services.inventory("dynamodb", None, read_lab.context) == Page(
        (Resource("dynamodb", "late-table", "late-table"),), None
    )
    assert_process(
        read_lab,
        [
            ("dynamodb", "list-tables", "us-east-2", {"Limit": 100}),
            ("dynamodb", "list-tables", "us-east-2", {"Limit": 100}),
        ],
    )


@pytest.mark.parametrize(
    "case",
    [
        "wrong_version",
        "boolean_version",
        "other_run",
        "other_service",
        "other_parent",
        "other_kind",
        "extra_field",
        "other_marker",
        "extra_marker",
        "empty_marker",
    ],
)
def test_cursor_de_outro_escopo_ou_schema_nao_inicia_processo(read_lab: ReadLab, case: str) -> None:
    cursor = json.dumps(fixture_data("cursors")["invalid"][case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", cursor, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [])


@pytest.mark.parametrize(
    "case", ["empty_markers", "empty_seen", "fingerprint_missing", "fingerprint_not_last"]
)
def test_cursor_sem_fingerprint_atual_nao_inicia_processo(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cursor = json.dumps(fixture_data("cursors")["invalid"][case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", cursor, read_lab.context)
    assert_rejected(error.value)
    assert error.value.args == ("service=cursor_invalid",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [])


def test_json_profundo_invalido_e_sanitizado_sem_io(
    read_lab: ReadLab,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cursor = "[" * 10_000 + "0" + "]" * 10_000
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", cursor, read_lab.context)
    assert_rejected(error.value)
    assert error.value.args == ("service=cursor_invalid",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [])


@pytest.mark.parametrize("cursor", ["", "{", '{"v":1,"v":1}'])
def test_cursor_malformado_nao_inicia_processo(read_lab: ReadLab, cursor: str) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", cursor, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [])


@pytest.mark.parametrize(
    "cursor",
    [
        '{"v":1,"v":1,"run":"a1b2c3d4","service":"dynamodb","parent":null,'
        '"kind":"dynamodb.tables","markers":{"ExclusiveStartTableName":"legacy-table"},'
        '"seen":["46305bf78be4f8cc569041ac15f0f85dbc5903c628324f1b76b407adde92ac84"]}',
        '{"v":1,"run":"a1b2c3d4","service":"dynamodb","parent":null,'
        '"kind":"dynamodb.tables","markers":{"ExclusiveStartTableName":"legacy-table",'
        '"ExclusiveStartTableName":"legacy-table"},'
        '"seen":["46305bf78be4f8cc569041ac15f0f85dbc5903c628324f1b76b407adde92ac84"]}',
    ],
    ids=["versao_duplicada", "marker_duplicado"],
)
def test_cursor_com_campo_duplicado_nao_inicia_processo(read_lab: ReadLab, cursor: str) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", cursor, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [])


def test_cursor_repetido_na_resposta_nao_vira_fim_da_colecao(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("tables")["repeat"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", cursor_input(), read_lab.context)
    assert_rejected(error.value)
    assert_process(
        read_lab,
        [
            (
                "dynamodb",
                "list-tables",
                "us-east-2",
                {
                    "Limit": 100,
                    "ExclusiveStartTableName": "legacy-table",
                },
            )
        ],
    )


def test_ciclo_de_markers_nao_renova_historico_do_cursor(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("tables")["first"])
    first = read_lab.services.inventory("dynamodb", None, read_lab.context)
    respond(read_lab, fixture_data("tables")["cycle_second"])
    second = read_lab.services.inventory("dynamodb", first.cursor, read_lab.context)
    respond(read_lab, fixture_data("tables")["repeat"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", second.cursor, read_lab.context)
    assert_rejected(error.value)
    assert_process(
        read_lab,
        [
            ("dynamodb", "list-tables", "us-east-2", {"Limit": 100}),
            (
                "dynamodb",
                "list-tables",
                "us-east-2",
                {
                    "Limit": 100,
                    "ExclusiveStartTableName": "legacy-table",
                },
            ),
            (
                "dynamodb",
                "list-tables",
                "us-east-2",
                {
                    "Limit": 100,
                    "ExclusiveStartTableName": "omega-table",
                },
            ),
        ],
    )


def test_segunda_pagina_negada_nao_e_resposta_vazia(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_denied")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", cursor_input(), read_lab.context)
    assert_rejected(error.value)
    assert_process(
        read_lab,
        [
            (
                "dynamodb",
                "list-tables",
                "us-east-2",
                {
                    "Limit": 100,
                    "ExclusiveStartTableName": "legacy-table",
                },
            )
        ],
    )


@pytest.mark.parametrize("case", ["missing_names", "bad_names", "empty_marker"])
def test_pagina_malformada_nao_e_colecao_vazia(read_lab: ReadLab, case: str) -> None:
    respond(read_lab, fixture_data("tables")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", None, read_lab.context)
    assert_rejected(error.value)
    assert_process(read_lab, [("dynamodb", "list-tables", "us-east-2", {"Limit": 100})])


@pytest.mark.parametrize(
    "case", ["marker_number", "marker_null", "marker_bool", "marker_list", "marker_object"]
)
def test_marker_com_tipo_invalido_nao_autoriza_proxima_pagina(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, fixture_data("tables")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("dynamodb", None, read_lab.context)
    assert_rejected(error.value)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [("dynamodb", "list-tables", "us-east-2", {"Limit": 100})])
