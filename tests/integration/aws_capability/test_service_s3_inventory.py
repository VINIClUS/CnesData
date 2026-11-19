"""Inventário S3 exige requests literais e continuação fechada."""

import json
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_read_assertions import assert_private, assert_process
from tests.integration.aws_capability._service_roots_testing import (
    LIST,
    NEXT,
    ROOT,
    ReadLab,
    cursor_input,
    fixture_data,
    load_fixture,
    make_read_lab,
    respond,
    respond_error,
)
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Page,
    Resource,
    ServiceFailure,
)
from tests.integration.aws_capability.services import AwsCommands


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


def test_inventario_s3_le_uma_pagina_rica_sem_filtrar_regiao_ou_prefixo(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["first"])
    first = read_lab.services.inventory("s3", None, read_lab.context)
    assert first.resources == (ROOT, Resource("s3", "legacy-bucket", "legacy-bucket"))
    assert first.cursor is not None
    assert json.loads(first.cursor) == fixture_data("cursors")["valid"]
    assert_process(read_lab, [LIST])


def test_segunda_pagina_s3_envia_continuation_token_sem_outros_filtros(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["first"])
    first = read_lab.services.inventory("s3", None, read_lab.context)
    respond(read_lab, fixture_data("s3")["second"])
    second = read_lab.services.inventory("s3", first.cursor, read_lab.context)
    assert second == Page((Resource("s3", "production-bucket", "production-bucket"),), None)
    assert_process(read_lab, [LIST, NEXT])


def test_pagina_vazia_com_token_nao_encerra_inventario(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["empty_with_next"])
    first = read_lab.services.inventory("s3", None, read_lab.context)
    assert first.resources == ()
    assert first.cursor is not None
    respond(read_lab, fixture_data("s3")["second"])
    second = read_lab.services.inventory("s3", first.cursor, read_lab.context)
    assert second.resources == (Resource("s3", "production-bucket", "production-bucket"),)
    assert second.cursor is None
    assert_process(read_lab, [LIST, NEXT])


def test_reinicio_sem_cursor_rele_colecao_sem_cache(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["empty"])
    assert read_lab.services.inventory("s3", None, read_lab.context) == Page((), None)
    respond(read_lab, fixture_data("s3")["first"])
    first = read_lab.services.inventory("s3", None, read_lab.context)
    assert first.resources[0] == ROOT
    assert_process(read_lab, [LIST, LIST])


def test_inventario_usa_regiao_configurada_do_spec(read_lab: ReadLab) -> None:
    original = read_lab.services.ports.commands
    assert isinstance(original, AwsCommands)
    commands = replace(original, region="eu-west-1")
    read_lab.services = replace(
        read_lab.services,
        spec=replace(read_lab.services.spec, region="eu-west-1"),
        ports=replace(read_lab.services.ports, commands=commands),
    )
    respond(read_lab, fixture_data("s3")["empty"])
    assert read_lab.services.inventory("s3", None, read_lab.context) == Page((), None)
    assert_process(read_lab, [("s3api", "list-buckets", "eu-west-1", {"MaxBuckets": 1000})])


@pytest.mark.parametrize("case", list(fixture_data("cursors")["invalid"]))
def test_cursor_invalido_falha_antes_de_processo(read_lab: ReadLab, case: str) -> None:
    cursor = json.dumps(fixture_data("cursors")["invalid"][case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("s3", cursor, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [])


@pytest.mark.parametrize("cursor", ["", "{", "[]", "null", "[" * 2000 + "0" + "]" * 2000])
def test_cursor_malformado_falha_sem_io(read_lab: ReadLab, cursor: str) -> None:
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("s3", cursor, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [])


@pytest.mark.parametrize(
    "duplicate",
    ["envelope", "marker"],
    ids=["envelope", "markers"],
)
def test_cursor_com_chave_json_duplicada_e_rejeitado(read_lab: ReadLab, duplicate: str) -> None:
    cursor = (
        '{"run":"a1b2c3d4",' + cursor_input()[1:]
        if duplicate == "envelope"
        else cursor_input().replace(
            '"ContinuationToken":"bucket-next"',
            '"ContinuationToken":"other","ContinuationToken":"bucket-next"',
        )
    )
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("s3", cursor, read_lab.context)
    assert error.value.args == ("service=cursor_invalid",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [])


def test_token_repetido_nao_vira_fim_de_colecao(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["repeat"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("s3", cursor_input(), read_lab.context)
    assert error.value.args == ("service=cursor_cycle",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [NEXT])


def test_ciclo_entre_paginas_preserva_historico_inteiro(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["first"])
    first = read_lab.services.inventory("s3", None, read_lab.context)
    respond(read_lab, fixture_data("s3")["cycle_second"])
    second = read_lab.services.inventory("s3", first.cursor, read_lab.context)
    assert second.cursor is not None
    assert json.loads(second.cursor) == fixture_data("cursors")["cycle"]
    respond(read_lab, fixture_data("s3")["repeat"])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("s3", second.cursor, read_lab.context)
    assert error.value.args == ("service=cursor_cycle",)
    read_lab.check_private(error.value)
    assert_process(
        read_lab,
        [
            LIST,
            NEXT,
            (
                "s3api",
                "list-buckets",
                "us-east-2",
                {"MaxBuckets": 1000, "ContinuationToken": "bucket-other"},
            ),
        ],
    )


@pytest.mark.parametrize(
    "case",
    [
        "missing_buckets",
        "buckets_map",
        "buckets_null",
        "buckets_string",
        "bad_entry",
        "missing_name",
        "empty_name",
        "number_name",
        "null_name",
        "token_empty",
        "token_number",
        "token_null",
        "token_array",
    ],
)
def test_resposta_invalida_nao_vira_colecao_vazia(
    read_lab: ReadLab,
    case: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, fixture_data("s3")[case])
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("s3", None, read_lab.context)
    assert error.value.args == ("service=inventory_invalid",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [LIST])


@pytest.mark.parametrize("failure", ["remote_denied", "bad_json", "empty_stdout"])
def test_erro_de_listagem_falha_sem_retry(read_lab: ReadLab, failure: str) -> None:
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("s3", None, read_lab.context)
    assert error.value.args == ("service=request_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [LIST])


def test_falha_na_continuacao_nao_reinicia_colecao_ou_fabrica_fim(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_denied")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inventory("s3", cursor_input(), read_lab.context)
    assert error.value.args == ("service=request_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [NEXT])


def test_orcamento_esgotado_impede_subprocesso(read_lab: ReadLab) -> None:
    read_lab.context.budget.used = 80
    read_lab.context.budget.work_used = 40
    with pytest.raises(BudgetExhausted, match="budget=requests"):
        read_lab.services.inventory("s3", None, read_lab.context)
    assert read_lab.process.calls == []
    assert read_lab.process.observed_debits == []


@pytest.mark.parametrize("name", ["s3", "cursors", "errors"])
def test_todas_fixtures_carregam_sem_chaves_duplicadas(name: str) -> None:
    assert isinstance(fixture_data(name), dict)


@pytest.mark.parametrize(
    "payload",
    ['{"empty":{},"empty":{}}', '{"outer":{"Key":"a","Key":"b"}}'],
    ids=["raiz", "aninhado"],
)
def test_loader_rejeita_json_duplicado_em_copia(tmp_path: Path, payload: str) -> None:
    path = tmp_path / "duplicate.json"
    path.write_text(payload)
    with pytest.raises(ValueError, match="duplicate_key="):
        load_fixture(path)
