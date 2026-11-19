"""Contrato compartilhado dos stores de leads públicos."""

import sqlite3
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from runpy import run_path
from typing import Any
from uuid import UUID

import boto3
import pytest
from moto import mock_aws

from cnes_domain import public_leads as domain
from cnes_infra.public_leads import dynamodb as dynamodb_module
from cnes_infra.public_leads import sqlite as sqlite_module

PublicLead = domain.PublicLead
PublicLeadInput = domain.PublicLeadInput
PublicLeadConflict = domain.PublicLeadConflict
PublicLeadUnavailable = domain.PublicLeadUnavailable
SQLitePublicLeadStore = sqlite_module.SQLitePublicLeadStore
DynamoDBPublicLeadStore = dynamodb_module.DynamoDBPublicLeadStore

_TABLE_NAME = "public-leads-contract"
_FIELDS = {
    "name": "Pessoa Sintética",
    "email": "Pessoa.Sintetica+Leads@EXAMPLE.ORG",
    "interest": "contact",
    "organization": "Org UTF-8 — saúde",
    "municipality": "Cidade",
    "role": "Gestão",
    "message": "Linha 1\nLinha 2",
    "source_path": "/contato",
    "source_cta": "fale conosco",
    "privacy_notice_version": "1",
    "newsletter_opt_in": True,
}
_NOW = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
_UUIDS = (
    UUID("11111111-1111-4111-8111-111111111111"),
    UUID("22222222-2222-4222-8222-222222222222"),
    UUID("33333333-3333-4333-8333-333333333333"),
)


def _input(**changes: object) -> PublicLeadInput:
    return PublicLeadInput(**{**_FIELDS, **changes})


def _record(**changes: object) -> PublicLead:
    return PublicLead(
        lead_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        created_at=_NOW,
        **{**_FIELDS, **changes},
    )


def _create_table(client: Any) -> None:
    client.create_table(
        TableName=_TABLE_NAME,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )


@pytest.fixture(params=("sqlite", "dynamodb"))
def store(
    request: pytest.FixtureRequest,
    tmp_path: Any,
) -> Iterator[tuple[Any, list[str]]]:
    calls: list[str] = []
    ids = iter(_UUIDS)

    def clock() -> datetime:
        calls.append("clock")
        return _NOW

    def uuid_factory() -> UUID:
        calls.append("uuid")
        return next(ids)

    if request.param == "sqlite":
        adapter = SQLitePublicLeadStore(tmp_path / "leads.sqlite3", clock, uuid_factory)
        adapter.initialize()
        yield adapter, calls
        return
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        adapter = DynamoDBPublicLeadStore(client, _TABLE_NAME, clock, uuid_factory)
        yield adapter, calls


def test_create_preserva_os_onze_campos_e_a_identidade_global(store: Any) -> None:
    adapter, calls = store
    created = adapter.create(_input())
    expected = PublicLead(lead_id=_UUIDS[0], created_at=_NOW, **_FIELDS)
    assert created.lead_id == _UUIDS[0]
    assert created.created_at == _NOW
    assert calls.count("clock") == 1
    assert calls.count("uuid") == 1
    assert created == expected
    assert adapter.readback(created.lead_id) == expected


def test_create_preserva_duplicado_e_caixa_do_email(store: Any) -> None:
    adapter, _ = store
    first = adapter.create(_input())
    second = adapter.create(_input())
    assert first.lead_id != second.lead_id
    assert first.email == second.email == "Pessoa.Sintetica+Leads@EXAMPLE.ORG"
    assert adapter.readback(first.lead_id) == first
    assert adapter.readback(second.lead_id) == second


def test_import_preserva_id_e_created_at_sem_consultar_factories(store: Any) -> None:
    adapter, calls = store
    record = _record()
    calls.clear()
    assert adapter.import_record(record) is None
    assert calls == []
    assert adapter.readback(record.lead_id) == record
    assert calls == []


def test_import_conflito_nao_sobrescreve(store: Any) -> None:
    adapter, _ = store
    record = _record()
    adapter.import_record(record)
    conflicting = replace(record, name="Outro registro", newsletter_opt_in=False)
    with pytest.raises(PublicLeadConflict, match="code=leads_conflict"):
        adapter.import_record(conflicting)
    assert adapter.readback(record.lead_id) == record


def test_readback_ausente_retorna_none(store: Any) -> None:
    adapter, _ = store
    assert adapter.readback(UUID("99999999-9999-4999-8999-999999999999")) is None


def test_validacao_de_persistencia_ocorre_antes_dos_factories(store: Any) -> None:
    adapter, calls = store
    with pytest.raises(PublicLeadUnavailable, match="code=leads_unavailable"):
        adapter.create(_input(name=""))
    assert calls == []


def test_preserva_vazios_opcionais_e_booleano_real(store: Any) -> None:
    adapter, _ = store
    value = _input(
        organization="",
        municipality="",
        privacy_notice_version="",
        newsletter_opt_in=False,
    )
    created = adapter.create(value)
    assert adapter.readback(created.lead_id) == PublicLead(
        lead_id=created.lead_id,
        created_at=_NOW,
        **{
            **_FIELDS,
            "organization": "",
            "municipality": "",
            "privacy_notice_version": "",
            "newsletter_opt_in": False,
        },
    )


@pytest.fixture
def guarded_store(store: Any, monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Any, list[str]]:
    adapter, calls = store
    events: list[str] = []
    connect = sqlite3.connect

    def recorded_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        events.append("connect")
        return connect(*args, **kwargs)

    class ClientRecorder:
        def __init__(self, wrapped: Any) -> None:
            self.wrapped = wrapped

        def __getattr__(self, name: str) -> Any:
            method = getattr(self.wrapped, name)

            def recorded_call(*args: object, **kwargs: object) -> Any:
                events.append(name)
                return method(*args, **kwargs)

            return recorded_call

    if isinstance(adapter, SQLitePublicLeadStore):
        monkeypatch.setattr(sqlite3, "connect", recorded_connect)
    else:
        monkeypatch.setattr(adapter, "client", ClientRecorder(adapter.client))
    calls.clear()
    return adapter, calls, events


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("name", "N" * 121, id="name-max"),
        pytest.param("email", "ab", id="email-min"),
        pytest.param("email", "e" * 255, id="email-max"),
        pytest.param("interest", "other", id="interest-enum"),
        pytest.param("organization", "O" * 201, id="organization-max"),
        pytest.param("municipality", "M" * 121, id="municipality-max"),
        pytest.param("role", "R" * 121, id="role-max"),
        pytest.param("message", "X" * 2001, id="message-max"),
        pytest.param("source_path", "P" * 201, id="source-path-max"),
        pytest.param("source_cta", "C" * 51, id="source-cta-max"),
        pytest.param("privacy_notice_version", "v" * 11, id="privacy-max"),
        pytest.param("name", None, id="string-null"),
        pytest.param("email", 42, id="string-type"),
        pytest.param("newsletter_opt_in", 0, id="bool-zero"),
        pytest.param("newsletter_opt_in", 1, id="bool-one"),
        pytest.param("newsletter_opt_in", "false", id="bool-string"),
        pytest.param("newsletter_opt_in", None, id="bool-null"),
    ],
)
def test_create_rejeita_campo_tipado_antes_de_callbacks_e_io(
    guarded_store: Any,
    field: str,
    value: object,
) -> None:
    adapter, calls, events = guarded_store
    candidate = _input(**{field: value})
    with pytest.raises(PublicLeadUnavailable) as caught:
        adapter.create(candidate)
    assert caught.value.code == "leads_unavailable"
    assert str(caught.value) == "code=leads_unavailable"
    assert calls == []
    assert events == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("lead_id", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", id="uuid-type"),
        pytest.param("created_at", _NOW.replace(tzinfo=None), id="time-naive"),
        pytest.param("created_at", _NOW.astimezone(timezone(timedelta(hours=1))), id="time-offset"),
        pytest.param("created_at", "2026-01-02T03:04:05.123456+00:00", id="time-type"),
    ],
)
def test_import_rejeita_identidade_antes_de_factories_e_io(
    guarded_store: Any,
    field: str,
    value: Any,
) -> None:
    adapter, calls, events = guarded_store
    candidate = replace(_record(), **{field: value})
    with pytest.raises(PublicLeadUnavailable, match=r"^code=leads_unavailable$"):
        adapter.import_record(candidate)
    assert calls == []
    assert events == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("email", "e" * 255, id="email-max"),
        pytest.param("interest", "other", id="interest-enum"),
        pytest.param("newsletter_opt_in", 1, id="bool-type"),
    ],
)
def test_import_rejeita_campos_tipados_antes_de_factories_e_io(
    guarded_store: Any,
    field: str,
    value: Any,
) -> None:
    adapter, calls, events = guarded_store
    with pytest.raises(PublicLeadUnavailable, match=r"^code=leads_unavailable$"):
        adapter.import_record(replace(_record(), **{field: value}))
    assert calls == []
    assert events == []


@pytest.mark.parametrize(
    ("callback", "value"),
    [
        pytest.param("uuid_factory", str(_UUIDS[0]), id="uuid-type"),
        pytest.param("clock", _NOW.replace(tzinfo=None), id="time-naive"),
        pytest.param("clock", _NOW.astimezone(timezone(timedelta(hours=-1))), id="time-offset"),
        pytest.param("clock", None, id="time-type"),
    ],
)
def test_create_rejeita_resultado_de_factory_antes_de_io(
    guarded_store: Any,
    callback: str,
    value: Any,
) -> None:
    adapter, calls, events = guarded_store

    def invalid_callback() -> Any:
        calls.append(callback)
        return value

    setattr(adapter, callback, invalid_callback)
    with pytest.raises(PublicLeadUnavailable, match=r"^code=leads_unavailable$"):
        adapter.create(_input())
    assert calls.count(callback) == 1
    assert events == []


def test_persiste_todos_os_maximos_compativeis_com_interesse(store: Any) -> None:
    adapter, _ = store
    fixture = Path(__file__).resolve().parents[1] / "fixtures/public_leads_legacy_cases.py"
    maximum = run_path(str(fixture))["SYNTHETIC_CASES"]["S04"]
    created = adapter.create(PublicLeadInput(**maximum))
    expected = PublicLead(lead_id=_UUIDS[0], created_at=_NOW, **maximum)
    assert created == expected
    assert adapter.readback(_UUIDS[0]) == expected


@pytest.mark.parametrize("interest", ["early_access", "pilot", "contact"])
def test_persiste_cada_interesse_permitido(store: Any, interest: str) -> None:
    adapter, _ = store
    adapter.create(_input(interest=interest))
    expected = PublicLead(lead_id=_UUIDS[0], created_at=_NOW, **{**_FIELDS, "interest": interest})
    assert adapter.readback(_UUIDS[0]) == expected


def test_persiste_whitespace_e_email_sem_regex_http(store: Any) -> None:
    adapter, _ = store
    changes = {
        "name": "  Pessoa  ",
        "email": "x y",
        "organization": "  Org  ",
        "privacy_notice_version": "   ",
    }
    adapter.create(_input(**changes))
    expected = PublicLead(lead_id=_UUIDS[0], created_at=_NOW, **{**_FIELDS, **changes})
    assert adapter.readback(_UUIDS[0]) == expected


def test_create_e_import_preservam_utc_zero_customizado_e_microssegundos(store: Any) -> None:
    adapter, calls = store
    instant = _NOW.replace(tzinfo=timezone(timedelta(0), "custom-zero"))
    assert instant.tzinfo is not UTC

    def custom_clock() -> datetime:
        calls.append("clock")
        return instant

    adapter.clock = custom_clock
    created = adapter.create(_input())
    expected = PublicLead(lead_id=_UUIDS[0], created_at=instant, **_FIELDS)
    assert created == expected
    assert adapter.readback(_UUIDS[0]) == expected
    assert calls.count("uuid") == 1
    assert calls.count("clock") == 1
    imported = replace(_record(), created_at=instant)
    calls.clear()
    adapter.import_record(imported)
    observed = adapter.readback(imported.lead_id)
    assert observed == imported
    assert observed.created_at.microsecond == 123456
    assert observed.created_at.utcoffset() == timedelta(0)
    assert calls == []


def test_replay_identico_de_import_conflita_sem_overwrite_ou_factories(store: Any) -> None:
    adapter, calls = store
    record = _record()
    adapter.import_record(record)
    calls.clear()
    with pytest.raises(PublicLeadConflict, match=r"^code=leads_conflict$"):
        adapter.import_record(record)
    assert adapter.readback(record.lead_id) == record
    assert calls == []
