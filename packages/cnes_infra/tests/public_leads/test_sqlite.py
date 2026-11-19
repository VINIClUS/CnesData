"""Persistência SQLite real do contrato de leads públicos."""

import sqlite3
import traceback
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest

from cnes_domain import public_leads as domain
from cnes_infra.public_leads import sqlite as sqlite_module

PublicLead = domain.PublicLead
PublicLeadInput = domain.PublicLeadInput
PublicLeadUnavailable = domain.PublicLeadUnavailable
SQLitePublicLeadStore = sqlite_module.SQLitePublicLeadStore

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
_FIELD_NAMES = tuple(_FIELDS)
_NOW = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
_LEAD_ID = UUID("11111111-1111-4111-8111-111111111111")


def _input(**changes: object) -> PublicLeadInput:
    return PublicLeadInput(**{**_FIELDS, **changes})


def _record(**changes: object) -> PublicLead:
    return PublicLead(
        lead_id=_LEAD_ID,
        created_at=_NOW,
        **{**_FIELDS, **changes},
    )


def _store(
    path: Any,
    calls: list[str] | None = None,
    clock_values: tuple[datetime, ...] = (_NOW,),
) -> Any:
    events = calls if calls is not None else []
    clocks = iter(clock_values)

    def clock() -> datetime:
        events.append("clock")
        return next(clocks)

    def uuid_factory() -> UUID:
        events.append("uuid")
        return _LEAD_ID

    store = SQLitePublicLeadStore(path, clock, uuid_factory)
    store.initialize()
    return store


@pytest.fixture
def sqlite_store(tmp_path: Any) -> tuple[Any, list[str], Any]:
    path = tmp_path / "public-leads.sqlite3"
    calls: list[str] = []
    return _store(path, calls), calls, path


def test_initialize_idempotente_cria_apenas_tabela_global(sqlite_store: Any) -> None:
    store, _, path = sqlite_store
    store.initialize()
    with sqlite3.connect(path) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        columns = connection.execute("PRAGMA table_info(public_leads)").fetchall()
    assert tuple(row[0] for row in tables) == ("public_leads",)
    assert tuple(row[1] for row in columns) == ("id", "created_at", *_FIELD_NAMES)


def test_create_persiste_campos_com_select_cru(sqlite_store: Any) -> None:
    store, calls, path = sqlite_store
    created = store.create(_input())
    assert created.lead_id == _LEAD_ID
    assert created.created_at == _NOW
    assert calls.count("clock") == 1
    assert calls.count("uuid") == 1
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT id, created_at, name, email, interest, organization, municipality, "
            "role, message, source_path, source_cta, privacy_notice_version, "
            "newsletter_opt_in FROM public_leads WHERE id = ?",
            (str(_LEAD_ID),),
        ).fetchone()
    assert row == (
        str(created.lead_id),
        _NOW.isoformat(),
        _FIELDS["name"],
        _FIELDS["email"],
        _FIELDS["interest"],
        _FIELDS["organization"],
        _FIELDS["municipality"],
        _FIELDS["role"],
        _FIELDS["message"],
        _FIELDS["source_path"],
        _FIELDS["source_cta"],
        _FIELDS["privacy_notice_version"],
        1,
    )


def test_colisao_de_uuid_no_create_nao_sobrescreve(sqlite_store: Any) -> None:
    _, calls, path = sqlite_store
    calls.clear()
    store = _store(path, calls, (_NOW, _NOW + timedelta(microseconds=1)))
    first = store.create(_input())
    with sqlite3.connect(path) as connection:
        before = connection.execute(
            "SELECT id, created_at, name, email, interest, organization, municipality, role, "
            "message, "
            "source_path, source_cta, privacy_notice_version, newsletter_opt_in "
            "FROM public_leads WHERE id = ?",
            (str(_LEAD_ID),),
        ).fetchone()
    with pytest.raises(PublicLeadUnavailable, match="code=leads_unavailable"):
        store.create(_input(name="Segundo registro"))
    with sqlite3.connect(path) as connection:
        after = connection.execute(
            "SELECT id, created_at, name, email, interest, organization, municipality, role, "
            "message, "
            "source_path, source_cta, privacy_notice_version, newsletter_opt_in "
            "FROM public_leads WHERE id = ?",
            (str(_LEAD_ID),),
        ).fetchone()
    assert calls.count("clock") == 2
    assert calls.count("uuid") == 2
    assert before == after
    assert store.readback(_LEAD_ID) == first


def test_import_invalido_nao_acessa_sqlite_antes_da_validacao(
    sqlite_store: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, calls, path = sqlite_store
    events: list[object] = []
    original_connect = sqlite3.connect

    def guarded_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        events.append((args, kwargs))
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", guarded_connect)
    calls.clear()
    with pytest.raises(PublicLeadUnavailable, match="code=leads_unavailable"):
        store.import_record(_record(name=""))
    assert calls == []
    assert events == []
    monkeypatch.undo()
    with sqlite3.connect(path) as connection:
        count = connection.execute("SELECT count(*) FROM public_leads").fetchone()
    assert count == (0,)


def test_readback_reabre_conexao_e_rele_estado_alterado(sqlite_store: Any) -> None:
    store, _, path = sqlite_store
    created = store.create(_input())
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE public_leads SET organization = ? WHERE id = ?",
            ("mudado no banco", str(created.lead_id)),
        )
        connection.commit()
    reopened = _store(path)
    assert reopened.readback(created.lead_id) == replace(
        created,
        organization="mudado no banco",
    )


def test_schema_nao_tem_tenant_ttl_unicidade_email_ou_indice_pii(sqlite_store: Any) -> None:
    _, _, path = sqlite_store
    with sqlite3.connect(path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(public_leads)")}
        indexes = tuple(connection.execute("PRAGMA index_list(public_leads)").fetchall())
    assert "tenant_id" not in columns
    assert "expires_at" not in columns
    assert all("email" not in str(index).lower() for index in indexes)


def test_create_invalido_faz_rollback_e_expoe_erro_neutro(sqlite_store: Any) -> None:
    store, calls, path = sqlite_store
    with pytest.raises(PublicLeadUnavailable, match="code=leads_unavailable"):
        store.create(_input(name=""))
    assert calls == []
    with sqlite3.connect(path) as connection:
        count = connection.execute("SELECT count(*) FROM public_leads").fetchone()
    assert count == (0,)


def test_readback_de_tabela_indisponivel_nao_vaza_backend(sqlite_store: Any) -> None:
    store, _, path = sqlite_store
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE public_leads")
        connection.commit()
    with pytest.raises(PublicLeadUnavailable) as caught:
        store.readback(_LEAD_ID)
    assert str(caught.value) == "code=leads_unavailable"
    assert "public_leads" not in str(caught.value)


_RAW_INSERT = (
    "INSERT INTO public_leads (id, created_at, name, email, interest, organization, "
    "municipality, role, message, source_path, source_cta, privacy_notice_version, "
    "newsletter_opt_in) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
_RAW_MAXIMUM = (
    "11111111-1111-4111-8111-111111111111",
    "2026-01-02T03:04:05.123456+00:00",
    "N" * 120,
    "e" * 254,
    "early_access",
    "O" * 200,
    "M" * 120,
    "R" * 120,
    "X" * 2000,
    "P" * 200,
    "C" * 50,
    "v" * 10,
    1,
)


@pytest.mark.parametrize(
    ("index", "value"),
    [
        pytest.param(2, "", id="name-min"),
        pytest.param(2, "N" * 121, id="name-max"),
        pytest.param(3, "ab", id="email-min"),
        pytest.param(3, "e" * 255, id="email-max"),
        pytest.param(4, "other", id="interest-enum"),
        pytest.param(5, "O" * 201, id="organization-max"),
        pytest.param(6, "M" * 121, id="municipality-max"),
        pytest.param(7, "R" * 121, id="role-max"),
        pytest.param(8, "X" * 2001, id="message-max"),
        pytest.param(9, "P" * 201, id="source-path-max"),
        pytest.param(10, "C" * 51, id="source-cta-max"),
        pytest.param(11, "v" * 11, id="privacy-max"),
        pytest.param(12, 2, id="bool-check"),
    ],
)
def test_schema_rejeita_check_em_insert_sql_cru(sqlite_store: Any, index: int, value: Any) -> None:
    _, _, path = sqlite_store
    row = list(_RAW_MAXIMUM)
    row[index] = value
    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(_RAW_INSERT, row)
        assert connection.execute("SELECT count(*) FROM public_leads").fetchone() == (0,)


@pytest.mark.parametrize("index", range(13), ids=("id", "created_at", *_FIELD_NAMES))
def test_schema_rejeita_null_em_cada_coluna_sql(sqlite_store: Any, index: int) -> None:
    _, _, path = sqlite_store
    row: list[Any] = list(_RAW_MAXIMUM)
    row[index] = None
    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(_RAW_INSERT, row)
        assert connection.execute("SELECT count(*) FROM public_leads").fetchone() == (0,)


@pytest.mark.parametrize("maximum", [True, False], ids=["maximos", "minimos"])
def test_schema_aceita_fronteiras_por_insert_sql_cru(sqlite_store: Any, maximum: bool) -> None:
    store, _, path = sqlite_store
    store.initialize()
    row = (
        _RAW_MAXIMUM
        if maximum
        else (
            "11111111-1111-4111-8111-111111111111",
            "2026-01-02T03:04:05.123456+00:00",
            "N",
            "x y",
            "contact",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            0,
        )
    )
    with sqlite3.connect(path) as connection:
        connection.execute(_RAW_INSERT, row)
        connection.commit()
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT * FROM public_leads").fetchone() == row


class _ObservedSQL:
    def __init__(self, wrapped: Any, owner: sqlite3.Connection, state: Any) -> None:
        object.__setattr__(self, "wrapped", wrapped)
        object.__setattr__(self, "owner", owner)
        object.__setattr__(self, "state", state)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self.wrapped, name, value)

    def __enter__(self) -> Any:
        self.wrapped.__enter__()
        return self

    def __exit__(self, *args: Any) -> Any:
        return self.wrapped.__exit__(*args)

    def __getattr__(self, name: str) -> Any:
        method = getattr(self.wrapped, name)
        if name not in {"execute", "executemany", "cursor"}:
            return method

        def observed(*args: Any, **kwargs: Any) -> Any:
            result = method(*args, **kwargs)
            if name == "cursor":
                return _ObservedSQL(result, self.owner, self.state)
            if args[0].lstrip().upper().startswith(("INSERT", "REPLACE")):
                count = self.owner.execute("SELECT count(*) FROM public_leads").fetchone()[0]
                self.state[1].append(count)
                if self.state[0]:
                    raise sqlite3.OperationalError("synthetic-write private@example.org")
            return result

        return observed


def _observe_connections(monkeypatch: pytest.MonkeyPatch, fail: bool) -> Any:
    connect = sqlite3.connect
    connections: list[sqlite3.Connection] = []
    writes: list[int] = []

    def observed_connect(*args: Any, **kwargs: Any) -> Any:
        connection = connect(*args, **kwargs)
        connections.append(connection)
        return _ObservedSQL(connection, connection, (fail, writes))

    monkeypatch.setattr(sqlite3, "connect", observed_connect)
    return connect, connections, writes


@pytest.mark.parametrize("fail", [False, True], ids=["commit", "rollback"])
def test_write_real_fecha_conexao_e_falha_pos_dml_reverte_transacao(
    sqlite_store: Any,
    monkeypatch: pytest.MonkeyPatch,
    fail: bool,
) -> None:
    store, _, path = sqlite_store
    connect, connections, writes = _observe_connections(monkeypatch, fail)
    if fail:
        with pytest.raises(PublicLeadUnavailable) as caught:
            store.create(_input())
        assert caught.value.code == "leads_unavailable"
        assert str(caught.value) == "code=leads_unavailable"
        formatted = "".join(traceback.format_exception(caught.value))
        assert "private@example.org" not in formatted
        assert "synthetic-write" not in formatted
    else:
        store.create(_input())
        assert store.readback(_LEAD_ID) == _record()
    with connect(path) as fresh:
        assert fresh.execute("SELECT count(*) FROM public_leads").fetchone() == (int(not fail),)
    assert writes == [1]
    assert connections
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")


@pytest.mark.parametrize("operation", ["initialize", "create"])
def test_initialize_e_write_indisponiveis_suprimem_detalhe_de_backend(
    sqlite_store: Any,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    store, _, _ = sqlite_store
    events: list[str] = []

    def unavailable(*args: Any, **kwargs: Any) -> Any:
        events.append("connect")
        raise sqlite3.OperationalError("synthetic-sql private@example.org")

    monkeypatch.setattr(sqlite3, "connect", unavailable)
    with pytest.raises(PublicLeadUnavailable) as caught:
        store.initialize() if operation == "initialize" else store.create(_input())
    assert events == ["connect"]
    assert caught.value.code == "leads_unavailable"
    assert str(caught.value) == "code=leads_unavailable"
    formatted = "".join(traceback.format_exception(caught.value))
    assert "synthetic-sql" not in formatted
    assert "private@example.org" not in formatted
    assert "OperationalError" not in formatted


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("not-a-time", id="time-invalid"),
        pytest.param("2026-01-02T03:04:05.123456", id="time-naive"),
        pytest.param("2026-01-02T03:04:05.123456+01:00", id="time-offset"),
    ],
)
def test_readback_rejeita_timestamp_corrompido_em_sql_cru(sqlite_store: Any, value: str) -> None:
    store, _, path = sqlite_store
    store.create(_input())
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE public_leads SET created_at = ? WHERE id = ?", (value, str(_LEAD_ID))
        )
        connection.commit()
    with pytest.raises(PublicLeadUnavailable, match=r"^code=leads_unavailable$"):
        store.readback(_LEAD_ID)


class _MisdirectedSQL(_ObservedSQL):
    def __getattr__(self, name: str) -> Any:
        method = getattr(self.wrapped, name)
        if name not in {"execute", "cursor"}:
            return method

        def redirected(*args: Any, **kwargs: Any) -> Any:
            if name == "cursor":
                return _MisdirectedSQL(method(*args, **kwargs), self.owner, self.state)
            if len(args) > 1:
                source, destination = self.state
                parameters = args[1]
                if isinstance(parameters, dict):
                    parameters = {
                        key: destination if value == source else value
                        for key, value in parameters.items()
                    }
                else:
                    parameters = tuple(
                        destination if value == source else value for value in parameters
                    )
                args = (args[0], parameters, *args[2:])
            return method(*args, **kwargs)

        return redirected


def test_readback_rejeita_uuid_valido_divergente_na_fronteira_sql(
    sqlite_store: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, _, path = sqlite_store
    store.create(_input())
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE public_leads SET id = ? WHERE id = ?",
            ("22222222-2222-4222-8222-222222222222", str(_LEAD_ID)),
        )
        connection.commit()
    connect = sqlite3.connect

    def misdirected_connect(*args: Any, **kwargs: Any) -> Any:
        connection = connect(*args, **kwargs)
        return _MisdirectedSQL(
            connection, connection, (str(_LEAD_ID), "22222222-2222-4222-8222-222222222222")
        )

    monkeypatch.setattr(sqlite3, "connect", misdirected_connect)
    with pytest.raises(PublicLeadUnavailable, match=r"^code=leads_unavailable$"):
        store.readback(_LEAD_ID)
