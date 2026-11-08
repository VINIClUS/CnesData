"""Inventário Cognito paginado e cursores fechados."""

import json
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_cognito_testing import (
    ROOT,
    ReadLab,
    cursor_input,
    fixture_data,
    load_fixture,
    make_read_lab,
    respond,
    respond_error,
)
from tests.integration.aws_capability._service_read_assertions import (
    assert_private,
    assert_process,
)
from tests.integration.aws_capability.contracts import Page, Resource, ServiceFailure


@pytest.fixture
def read_lab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> ReadLab:
    return make_read_lab(monkeypatch, tmp_path)


def test_inventario_cognito_consumiu_uma_pagina_aws_por_chamada(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["first"])
    first = read_lab.services.inventory("cognito", None, read_lab.context)
    assert first.resources == (
        ROOT,
        Resource("cognito", "us-east-2_legacy99", "legacy-pool"),
    )
    assert first.cursor is not None
    assert json.loads(first.cursor) == fixture_data("cursors")["valid"]
    assert_process(read_lab, [("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60})])


def test_segunda_pagina_cognito_envia_next_token_somente_depois_da_primeira(
    read_lab: ReadLab,
) -> None:
    respond(read_lab, fixture_data("cognito")["first"])
    first = read_lab.services.inventory("cognito", None, read_lab.context)
    respond(read_lab, fixture_data("cognito")["second"])
    second = read_lab.services.inventory("cognito", first.cursor, read_lab.context)
    assert second == Page((Resource("cognito", "us-east-2_production", "production-pool"),), None)
    assert_process(
        read_lab,
        [
            ("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60}),
            (
                "cognito-idp",
                "list-user-pools",
                "us-east-2",
                {"MaxResults": 60, "NextToken": "pool-next"},
            ),
        ],
    )


def test_pagina_vazia_com_next_token_continua_varredura(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["empty_with_next"])
    first = read_lab.services.inventory("cognito", None, read_lab.context)
    assert first.resources == ()
    assert first.cursor is not None
    respond(read_lab, fixture_data("cognito")["second"])
    second = read_lab.services.inventory("cognito", first.cursor, read_lab.context)
    assert second.resources == (Resource("cognito", "us-east-2_production", "production-pool"),)
    assert_process(
        read_lab,
        [
            ("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60}),
            (
                "cognito-idp",
                "list-user-pools",
                "us-east-2",
                {"MaxResults": 60, "NextToken": "pool-next"},
            ),
        ],
    )


def test_reinicio_sem_cursor_rele_a_colecao_sem_cache(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["empty_inventory"])
    assert read_lab.services.inventory("cognito", None, read_lab.context) == Page((), None)
    respond(read_lab, fixture_data("cognito")["first"])
    page = read_lab.services.inventory("cognito", None, read_lab.context)
    assert page.resources[0] == ROOT
    assert_process(
        read_lab,
        [
            ("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60}),
            ("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60}),
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
        "empty_token",
        "missing_token",
        "empty_seen",
        "fingerprint_missing",
        "fingerprint_not_last",
        "ddb_marker",
    ],
)
def test_cursor_invalido_nao_inicia_processo(read_lab: ReadLab, case: str) -> None:
    cursor = json.dumps(fixture_data("cursors")["invalid"][case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("cognito", cursor, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    assert_process(read_lab, [])


def test_cursor_repetido_na_resposta_nao_vira_fim_da_colecao(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["repeat"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("cognito", cursor_input(), read_lab.context)
    assert error.value.args == ("service=cursor_cycle",)
    assert_process(
        read_lab,
        [
            (
                "cognito-idp",
                "list-user-pools",
                "us-east-2",
                {"MaxResults": 60, "NextToken": "pool-next"},
            )
        ],
    )


@pytest.mark.parametrize("cursor", ["", "{", "[" * 2000 + "0" + "]" * 2000])
def test_cursor_malformado_e_sanitizado_sem_io(
    read_lab: ReadLab,
    cursor: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("cognito", cursor, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [])


def test_cursor_com_canary_nao_publica_dados_privados(read_lab: ReadLab) -> None:
    cursor = json.dumps(fixture_data("cursors")["invalid"]["extra_field"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("cognito", cursor, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    assert_private(error.value, read_lab, "")
    assert_process(read_lab, [])


def test_ciclo_de_next_tokens_nao_renova_historico(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("cognito")["first"])
    first = read_lab.services.inventory("cognito", None, read_lab.context)
    respond(read_lab, fixture_data("cognito")["cycle_second"])
    second = read_lab.services.inventory("cognito", first.cursor, read_lab.context)
    respond(read_lab, fixture_data("cognito")["repeat"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("cognito", second.cursor, read_lab.context)
    assert error.value.args == ("service=cursor_cycle",)
    assert_process(
        read_lab,
        [
            ("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60}),
            (
                "cognito-idp",
                "list-user-pools",
                "us-east-2",
                {"MaxResults": 60, "NextToken": "pool-next"},
            ),
            (
                "cognito-idp",
                "list-user-pools",
                "us-east-2",
                {"MaxResults": 60, "NextToken": "pool-other"},
            ),
        ],
    )


def test_resposta_malformada_nao_vira_colecao_vazia(
    read_lab: ReadLab,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cases = (
        "missing_pools",
        "bad_pools",
        "bad_pool_entry",
        "empty_id",
        "empty_name",
        "empty_token",
        "token_number",
        "token_null",
        "token_list",
        "extra_field",
    )
    expected = []
    for case in cases:
        respond(read_lab, fixture_data("cognito")[case])
        with pytest.raises(ServiceFailure) as error:
            read_lab.services.inventory("cognito", None, read_lab.context)
        assert error.value.args == ("service=inventory_invalid",)
        captured = capsys.readouterr()
        assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
        expected.append(("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60}))
        assert_process(read_lab, expected)


def test_loader_rejeita_chaves_duplicadas_em_copia(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"empty": {}, "empty": {}}')
    with pytest.raises(ValueError, match="duplicate_key=empty"):
        load_fixture(duplicate)


@pytest.mark.parametrize("failure", ["remote_denied", "bad_json", "empty_stdout", "empty_object"])
def test_falha_remota_ou_local_nao_vira_colecao_vazia(
    read_lab: ReadLab,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("cognito", None, read_lab.context)
    expected = (
        "service=inventory_invalid" if failure == "empty_object" else "service=request_failed"
    )
    assert error.value.args == (expected,)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [("cognito-idp", "list-user-pools", "us-east-2", {"MaxResults": 60})])
