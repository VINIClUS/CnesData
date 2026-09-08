"""Testes do transporte FTP dos arquivos PF do DATASUS."""

from __future__ import annotations

from dataclasses import dataclass
from ftplib import error_perm, error_proto, error_reply, error_temp
from pathlib import Path
from traceback import format_exception
from typing import Any

import pytest

from cnes_domain.pipeline.circuit_breaker import CircuitBreaker
from cnes_infra.ingestion import datasus_cnes_transport as transport_module
from cnes_infra.ingestion.datasus_cnes_transport import (
    DatasusCnesFtpTransport,
    DatasusCnesRequest,
)

_PATH = "/dissemin/publicos/CNES/200508_/Dados/PF/PFSP2601.dbc"
_DBC_HEADER = (
    b"\x03\x00\x00\x00\x00\x00\x00\x00\x21\x05\xe6\x00" + b"\x00" * 1300 + b"\x0d"
)
_LAYOUT = (
    ("CNES", "C", 7, 0),
    ("CODUFMUN", "C", 6, 0),
    ("REGSAUDE", "C", 4, 0),
    ("MICR_REG", "C", 6, 0),
    ("DISTRSAN", "C", 4, 0),
    ("DISTRADM", "C", 4, 0),
    ("TPGESTAO", "C", 1, 0),
    ("PF_PJ", "C", 1, 0),
    ("CPF_CNPJ", "C", 14, 0),
    ("NIV_DEP", "C", 1, 0),
    ("CNPJ_MAN", "C", 14, 0),
    ("ESFERA_A", "C", 2, 0),
    ("ATIVIDAD", "C", 2, 0),
    ("RETENCAO", "C", 2, 0),
    ("NATUREZA", "C", 2, 0),
    ("CLIENTEL", "C", 2, 0),
    ("TP_UNID", "C", 2, 0),
    ("TURNO_AT", "C", 2, 0),
    ("NIV_HIER", "C", 2, 0),
    ("TERCEIRO", "C", 1, 0),
    ("CPF_PROF", "C", 11, 0),
    ("CPFUNICO", "C", 1, 0),
    ("CBO", "C", 6, 0),
    ("CBOUNICO", "C", 6, 0),
    ("NOMEPROF", "C", 60, 0),
    ("CNS_PROF", "C", 15, 0),
    ("CONSELHO", "C", 2, 0),
    ("REGISTRO", "C", 13, 0),
    ("VINCULAC", "C", 6, 0),
    ("VINCUL_C", "C", 1, 0),
    ("VINCUL_A", "C", 1, 0),
    ("VINCUL_N", "C", 1, 0),
    ("PROF_SUS", "C", 1, 0),
    ("PROFNSUS", "C", 1, 0),
    ("HORAOUTR", "N", 3, 0),
    ("HORAHOSP", "N", 3, 0),
    ("HORA_AMB", "N", 3, 0),
    ("COMPETEN", "C", 6, 0),
    ("UFMUNRES", "C", 6, 0),
    ("NAT_JUR", "C", 4, 0),
)


@dataclass(frozen=True)
class _Field:
    name: str
    type: str
    length: int
    decimal_count: int


class _Rows:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = iter(rows)
        self.closed = False

    def __iter__(self) -> _Rows:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._rows)

    def close(self) -> None:
        self.closed = True


class _ExplodingRows(_Rows):
    def __next__(self) -> dict[str, object]:
        raise RuntimeError("broken-dbf")


class _Table:
    def __init__(self, rows: list[dict[str, object]], layout: tuple = _LAYOUT) -> None:
        self.fields = tuple(_Field(*field) for field in layout)
        self.loaded = False
        self._rows = rows
        self.iterators: list[_Rows] = []

    def __iter__(self) -> _Rows:
        rows = _Rows(self._rows)
        self.iterators.append(rows)
        return rows


class _BrokenTable(_Table):
    def __init__(self, break_on_iteration: int) -> None:
        super().__init__([_record()])
        self._break_on_iteration = break_on_iteration

    def __iter__(self) -> _Rows:
        if len(self.iterators) + 1 == self._break_on_iteration:
            raise RuntimeError("broken-dbf")
        return super().__iter__()


class _BrokenRowsTable(_Table):
    def __init__(self, break_on_iteration: int) -> None:
        super().__init__([_record()])
        self._break_on_iteration = break_on_iteration

    def __iter__(self) -> _Rows:
        if len(self.iterators) + 1 == self._break_on_iteration:
            rows = _ExplodingRows([])
            self.iterators.append(rows)
            return rows
        return super().__iter__()


class _Ftp:
    def __init__(self, payload: bytes = _DBC_HEADER) -> None:
        self.payload = payload
        self.calls: list[tuple[str, object]] = []
        self.sizes: list[object] = [len(payload), len(payload)]
        self.mdtms: list[object] = ["213 20260202010101", "213 20260202010101"]
        self.retrieval_error: Exception | None = None
        self.quit_called = False
        self.close_called = False

    def connect(self, host: str, timeout: int) -> None:
        self.calls.append(("connect", (host, timeout)))

    def login(self) -> None:
        self.calls.append(("login", None))

    def voidcmd(self, command: str) -> None:
        self.calls.append(("voidcmd", command))

    def size(self, path: str) -> object:
        self.calls.append(("size", path))
        return self.sizes.pop(0)

    def sendcmd(self, command: str) -> object:
        self.calls.append(("sendcmd", command))
        return self.mdtms.pop(0)

    def retrbinary(self, command: str, callback: Any, blocksize: int) -> None:
        self.calls.append(("retrbinary", (command, blocksize)))
        if self.retrieval_error:
            raise self.retrieval_error
        for start in range(0, len(self.payload), 2):
            callback(self.payload[start : start + 2])

    def quit(self) -> None:
        self.quit_called = True

    def close(self) -> None:
        self.close_called = True


def _request(**updates: str) -> DatasusCnesRequest:
    values = {
        "tenant_id": "354130",
        "competencia": "2026-01",
        "file_subtype": "CNES_VINCULO",
        "snapshot_id": "snapshot-1",
        "agent_id": "system-datasus",
        "agent_version": "1.2.3",
    }
    return DatasusCnesRequest(**(values | updates))


def _record(municipio: str = "354130", competencia: str = "202601") -> dict[str, object]:
    return {"CODUFMUN": municipio, "COMPETEN": competencia, "CPF_PROF": "90000000001"}


def _install_table(monkeypatch: pytest.MonkeyPatch, table: _Table) -> list[tuple[str, str]]:
    conversions: list[tuple[str, str]] = []

    def decompress(source: str, destination: str) -> None:
        conversions.append((source, destination))
        row_count = len(getattr(table, "_rows", ()))
        header = bytearray(_DBC_HEADER)
        header[4:8] = row_count.to_bytes(4, "little")
        record = b" " + b"\x00" * 229
        eof = b"\x1a" if row_count % 2 == 0 else b""
        Path(destination).write_bytes(header + record * row_count + eof)

    monkeypatch.setattr(transport_module, "decompress", decompress)
    monkeypatch.setattr(transport_module, "DBF", lambda *args, **kwargs: table)
    return conversions


def _error_code(action: Any) -> tuple[str, bool]:
    with pytest.raises(Exception) as captured:
        action()
    return captured.value.code, captured.value.retryable


def _ftp_error(error_type: type[Exception], message: str) -> Exception:
    return error_type(message)


def test_resolve_uf_e_yymm_em_url_unica(monkeypatch: pytest.MonkeyPatch):
    ftp = _Ftp()
    table = _Table([_record(), _record("330455")])
    conversions = _install_table(monkeypatch, table)
    adapter = DatasusCnesFtpTransport(lambda: ftp, CircuitBreaker(base_delay=0))

    assert list(adapter.fetch(_request())) == [_record()]
    assert ftp.calls.index(("voidcmd", "TYPE I")) < ftp.calls.index(("size", _PATH))
    assert ("retrbinary", (f"RETR {_PATH}", 64 * 1024)) in ftp.calls
    assert {value for operation, value in ftp.calls if operation == "size"} == {_PATH}
    assert conversions[0][0].endswith("PFSP2601.dbc")
    assert ftp.quit_called is True


@pytest.mark.parametrize(
    ("updates", "code"),
    [
        ({"tenant_id": "35413"}, "request_invalid"),
        ({"tenant_id": "999999"}, "request_invalid"),
        ({"tenant_id": "35\u0661\u0662\u0663\u0664"}, "request_invalid"),
        ({"competencia": "2026-13"}, "request_invalid"),
        ({"competencia": "\uff12\uff10\uff12\uff16-01"}, "request_invalid"),
        ({"file_subtype": "CNES_ESTABELECIMENTO"}, "request_invalid"),
        ({"snapshot_id": ""}, "request_invalid"),
        ({"snapshot_id": " "}, "request_invalid"),
        ({"agent_id": "edge"}, "request_invalid"),
        ({"agent_version": ""}, "request_invalid"),
        ({"agent_version": " "}, "request_invalid"),
        ({"tenant_id": None}, "request_invalid"),
    ],
)
def test_rejeita_request_invalido_sem_abrir_ftp(updates: dict[str, str], code: str):
    opened = False

    def factory() -> _Ftp:
        nonlocal opened
        opened = True
        return _Ftp()

    error = _error_code(lambda: list(DatasusCnesFtpTransport(factory).fetch(_request(**updates))))

    assert error == (code, False)
    assert opened is False


def test_ftp_550_retorna_source_not_published_sem_fallback(monkeypatch: pytest.MonkeyPatch):
    ftp = _Ftp()
    ftp.retrieval_error = _ftp_error(error_perm, "550 file unavailable")
    _install_table(monkeypatch, _Table([]))

    error = _error_code(lambda: list(DatasusCnesFtpTransport(lambda: ftp).fetch(_request())))

    assert error == ("source_not_published", True)
    retrievals = [call for call in ftp.calls if call[0] == "retrbinary"]
    assert retrievals == [("retrbinary", (f"RETR {_PATH}", 64 * 1024))]
    assert ftp.close_called is True


def test_falha_temporaria_e_retryable_e_limpa_ftp(monkeypatch: pytest.MonkeyPatch):
    ftp = _Ftp()
    ftp.retrieval_error = _ftp_error(error_temp, "421 unavailable")
    _install_table(monkeypatch, _Table([]))

    error = _error_code(lambda: list(DatasusCnesFtpTransport(lambda: ftp).fetch(_request())))

    assert error == ("source_unavailable", True)
    assert ftp.close_called is True


@pytest.mark.parametrize("error_type", [EOFError, error_reply, error_proto])
def test_falha_de_resposta_ftp_e_retryable(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
):
    ftp = _Ftp()
    _install_table(monkeypatch, _Table([]))

    def fail_connect(*args: object, **kwargs: object) -> None:
        raise _ftp_error(error_type, "sensitive-12345678901")

    ftp.connect = fail_connect

    with pytest.raises(Exception) as captured:
        list(DatasusCnesFtpTransport(lambda: ftp).fetch(_request()))

    assert (captured.value.code, captured.value.retryable) == ("source_unavailable", True)
    assert "sensitive-12345678901" not in "".join(format_exception(captured.value))


def test_circuito_aberto_falha_sem_abrir_ftp():
    breaker = CircuitBreaker(failure_threshold=1, base_delay=0)
    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("offline")))
    opened = False

    def factory() -> _Ftp:
        nonlocal opened
        opened = True
        return _Ftp()

    error = _error_code(
        lambda: list(DatasusCnesFtpTransport(factory, breaker).fetch(_request()))
    )

    assert error == ("circuit_open", True)
    assert opened is False


@pytest.mark.parametrize(
    "case",
    [
        ([3, 4], ["213 20260202010101"] * 2, b"dbc", "source_changed"),
        ([3, 3], ["213 20260202010101", "213 20260202010102"], b"dbc", "source_changed"),
        ([None], ["213 20260202010101"], b"dbc", "metadata_invalid"),
        ([3], ["invalid"], b"dbc", "metadata_invalid"),
        ([4, 4], ["213 20260202010101"] * 2, b"dbc", "size_mismatch"),
    ],
)
def test_rejeita_metadados_instaveis_ou_invalidos(
    monkeypatch: pytest.MonkeyPatch,
    case: tuple[list[object], list[object], bytes, str],
):
    sizes, mdtms, payload, code = case
    ftp = _Ftp(payload)
    ftp.sizes = sizes
    ftp.mdtms = mdtms
    _install_table(monkeypatch, _Table([]))

    error = _error_code(lambda: list(DatasusCnesFtpTransport(lambda: ftp).fetch(_request())))

    assert error == (code, True)
    assert ftp.close_called is True


@pytest.mark.parametrize(("failure", "code"), [("dbc", "dbc_invalid"), ("dbf", "dbf_invalid")])
def test_rejeita_dbc_ou_dbf_invalido(
    monkeypatch: pytest.MonkeyPatch, failure: str, code: str
):
    ftp = _Ftp()

    def fail(*args: object, **kwargs: object) -> None:
        raise ValueError("sensitive-data-must-not-leak")

    converter = fail if failure == "dbc" else lambda _s, d: Path(d).write_bytes(
        _DBC_HEADER + b"\x1a"
    )
    reader = fail if failure == "dbf" else lambda *_args, **_kwargs: None
    monkeypatch.setattr(transport_module, "decompress", converter)
    monkeypatch.setattr(transport_module, "DBF", reader)

    with pytest.raises(Exception) as captured:
        list(DatasusCnesFtpTransport(lambda: ftp).fetch(_request()))

    assert (captured.value.code, captured.value.retryable) == (code, False)
    assert "sensitive-data-must-not-leak" not in "".join(format_exception(captured.value))


def test_rejeita_layout_dbf_divergente(monkeypatch: pytest.MonkeyPatch):
    layout = _LAYOUT[:-1] + (("CAMPO_ERRADO", "C", 4, 0),)
    _install_table(monkeypatch, _Table([_record()], layout))

    error = _error_code(lambda: list(DatasusCnesFtpTransport(_Ftp).fetch(_request())))

    assert error == ("schema_invalid", False)


def test_valida_competencia_antes_da_ausencia_municipal(monkeypatch: pytest.MonkeyPatch):
    _install_table(monkeypatch, _Table([_record("330455", "202512")]))
    adapter = DatasusCnesFtpTransport(_Ftp)

    error = _error_code(lambda: list(adapter.fetch(_request())))

    assert error == ("competencia_invalid", False)


def test_ausencia_municipal_e_source_not_published(monkeypatch: pytest.MonkeyPatch):
    _install_table(monkeypatch, _Table([_record("330455")]))

    error = _error_code(lambda: list(DatasusCnesFtpTransport(_Ftp).fetch(_request())))

    assert error == ("source_not_published", True)


def test_leitura_incremental_fecha_iteradores_e_temporarios_no_cancelamento(
    monkeypatch: pytest.MonkeyPatch,
):
    table = _Table([_record(), _record()])
    conversions = _install_table(monkeypatch, table)
    rows = DatasusCnesFtpTransport(_Ftp).fetch(_request())

    assert next(rows) == _record()
    assert len(table.iterators) == 2
    assert table.iterators[0].closed is True
    temporary_root = Path(conversions[0][0]).parent
    rows.close()

    assert all(iterator.closed for iterator in table.iterators)
    assert temporary_root.exists() is False


def test_factory_padrao_cria_ftp(monkeypatch: pytest.MonkeyPatch):
    ftp = _Ftp()
    table = _Table([_record()])
    _install_table(monkeypatch, table)
    monkeypatch.setattr(transport_module, "FTP", lambda: ftp)

    assert list(DatasusCnesFtpTransport().fetch(_request())) == [_record()]


def test_falha_ao_criar_ftp_e_retryable(monkeypatch: pytest.MonkeyPatch):
    _install_table(monkeypatch, _Table([]))

    def factory() -> _Ftp:
        raise OSError("offline")

    error = _error_code(lambda: list(DatasusCnesFtpTransport(factory).fetch(_request())))

    assert error == ("source_unavailable", True)


def test_limpa_ftp_quando_quit_falha(monkeypatch: pytest.MonkeyPatch):
    ftp = _Ftp()

    def broken_quit() -> None:
        ftp.quit_called = True
        raise OSError("offline")

    ftp.quit = broken_quit
    _install_table(monkeypatch, _Table([_record()]))

    assert list(DatasusCnesFtpTransport(lambda: ftp).fetch(_request())) == [_record()]
    assert ftp.quit_called is True
    assert ftp.close_called is True


def test_rejeita_layout_dbf_ilegivel(monkeypatch: pytest.MonkeyPatch):
    class BrokenLayout:
        @property
        def fields(self):
            raise RuntimeError("broken-dbf")

    _install_table(monkeypatch, BrokenLayout())

    error = _error_code(lambda: list(DatasusCnesFtpTransport(_Ftp).fetch(_request())))

    assert error == ("dbf_invalid", False)


@pytest.mark.parametrize("iteration", [1, 2])
def test_rejeita_iteracao_dbf_ilegivel(monkeypatch: pytest.MonkeyPatch, iteration: int):
    _install_table(monkeypatch, _BrokenTable(iteration))

    error = _error_code(lambda: list(DatasusCnesFtpTransport(_Ftp).fetch(_request())))

    assert error == ("dbf_invalid", False)


@pytest.mark.parametrize("iteration", [1, 2])
def test_rejeita_leitura_dbf_ilegivel(monkeypatch: pytest.MonkeyPatch, iteration: int):
    _install_table(monkeypatch, _BrokenRowsTable(iteration))

    error = _error_code(lambda: list(DatasusCnesFtpTransport(_Ftp).fetch(_request())))

    assert error == ("dbf_invalid", False)


def test_rejeita_campo_dbf_invalido_com_falha_final(monkeypatch: pytest.MonkeyPatch):
    _install_table(monkeypatch, _Table([_record(competencia=202601)]))

    error = _error_code(lambda: list(DatasusCnesFtpTransport(_Ftp).fetch(_request())))

    assert error == ("field_invalid", False)


@pytest.mark.parametrize("field", ["COMPETEN", "CODUFMUN"])
def test_rejeita_campo_dbf_ausente_com_falha_final(
    monkeypatch: pytest.MonkeyPatch, field: str
):
    row = _record()
    row.pop(field)
    _install_table(monkeypatch, _Table([row]))

    error = _error_code(lambda: list(DatasusCnesFtpTransport(_Ftp).fetch(_request())))

    assert error == ("field_invalid", False)
