"""Transporte FTP para arquivos PF do CNES nacional."""

from __future__ import annotations

from dataclasses import dataclass
from ftplib import FTP, error_perm
from ftplib import Error as FtpError
from hashlib import sha256
from pathlib import Path
from re import fullmatch
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Never, Protocol

from datasus_dbc import decompress
from dbfread import DBF, FieldParser

from cnes_domain.pipeline.circuit_breaker import CircuitBreaker, CircuitBreakerAberto

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

_HOST = "ftp.datasus.gov.br"
_ROOT = "/dissemin/publicos/CNES/200508_/Dados/PF"
_SUBTYPE = "CNES_VINCULO"
_AGENT = "system-datasus"
_BLOCK_SIZE = 64 * 1024
_TIMEOUT = 30
_UF_BY_CODE = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}
_PF_LAYOUT = (
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
_DBF_HEADER_SIZE = 33 + len(_PF_LAYOUT) * 32
_DBF_RECORD_SIZE = 1 + sum(field[2] for field in _PF_LAYOUT)
_DBF_VERSION = 3


class DatasusCnesError(Exception):
    def __init__(self, code: str, retryable: bool) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(f"code={code} retryable={str(retryable).lower()}")


@dataclass(frozen=True, slots=True)
class DatasusCnesRequest:
    tenant_id: str
    competencia: str
    file_subtype: str
    snapshot_id: str
    agent_id: str
    agent_version: str


class DatasusCnesTransportPort(Protocol):
    def fetch(
        self, request: DatasusCnesRequest
    ) -> Iterator[Mapping[str, object]]: ...  # pragma: no cover


class _Closable(Protocol):
    def close(self) -> None: ...  # pragma: no cover


class _PfFieldParser(FieldParser):
    def parseN(self, field: object, data: bytes) -> object:  # noqa: N802
        if b"*" in data:
            return data
        return super().parseN(field, data)


@dataclass(frozen=True, slots=True)
class _Metadata:
    size: int
    modified: str


@dataclass(frozen=True, slots=True)
class _Download:
    size: int
    digest: str


@dataclass(frozen=True, slots=True)
class _DbfPhysical:
    file_size: int
    version: int
    records: int
    header_size: int
    record_size: int
    descriptor_end: bytes
    last_byte: bytes


def _default_ftp_factory() -> FTP:
    return FTP()  # noqa: S321


def _raise(code: str, retryable: bool) -> Never:
    raise DatasusCnesError(code, retryable) from None


def _validate_request(request: DatasusCnesRequest) -> None:
    values = (
        request.tenant_id,
        request.competencia,
        request.file_subtype,
        request.snapshot_id,
        request.agent_id,
        request.agent_version,
    )
    if not all(isinstance(value, str) for value in values):
        _raise("request_invalid", False)
    valid_tenant = bool(fullmatch(r"[0-9]{6}", request.tenant_id))
    valid_competencia = bool(
        fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", request.competencia)
    )
    valid_snapshot = bool(
        fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", request.snapshot_id)
    )
    valid_identity = valid_snapshot and bool(request.agent_version.strip())
    if not valid_tenant or request.tenant_id[:2] not in _UF_BY_CODE:
        _raise("request_invalid", False)
    if not valid_competencia or not valid_identity:
        _raise("request_invalid", False)
    if request.file_subtype != _SUBTYPE or request.agent_id != _AGENT:
        _raise("request_invalid", False)


def _close(resource: _Closable) -> None:
    resource.close()


class DatasusCnesFtpTransport:
    def __init__(
        self,
        ftp_factory: Callable[[], FTP] | None = None,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self._ftp_factory = ftp_factory or _default_ftp_factory
        self._breaker = circuit_breaker or CircuitBreaker(
            failure_threshold=3, service_name="DATASUS_FTP"
        )

    def fetch(self, request: DatasusCnesRequest) -> Iterator[Mapping[str, object]]:
        _validate_request(request)
        uf = _UF_BY_CODE[request.tenant_id[:2]]
        yymm = request.competencia[2:4] + request.competencia[5:7]
        path = f"{_ROOT}/PF{uf}{yymm}.dbc"
        return self._iterate(request, path)

    def _iterate(
        self, request: DatasusCnesRequest, remote_path: str
    ) -> Iterator[Mapping[str, object]]:
        with TemporaryDirectory(prefix="datasus-cnes-") as temporary:
            dbc_path = Path(temporary) / Path(remote_path).name
            dbf_path = dbc_path.with_suffix(".dbf")
            self._download_protected(remote_path, dbc_path)
            self._decompress(dbc_path, dbf_path)
            _validate_dbf_file(dbf_path)
            table = self._open_dbf(dbf_path)
            self._validate_layout(table)
            self._validate_competencia(table, request.competencia.replace("-", ""))
            yield from self._municipal_rows(table, request.tenant_id)

    def _download_protected(self, remote_path: str, destination: Path) -> None:
        try:
            published = self._breaker.call(self._download, remote_path, destination)
        except CircuitBreakerAberto:
            _raise("circuit_open", True)
        if not published:
            _raise("source_not_published", True)

    def _download(self, remote_path: str, destination: Path) -> bool:
        ftp: FTP | None = None
        try:
            ftp = self._ftp_factory()
            ftp.connect(_HOST, timeout=_TIMEOUT)
            ftp.login()
            ftp.voidcmd("TYPE I")
            before = self._metadata(ftp, remote_path)
            downloaded = self._retrieve(ftp, remote_path, destination)
            after = self._metadata(ftp, remote_path)
            if before != after:
                _raise("source_changed", True)
            if downloaded.size != before.size:
                _raise("size_mismatch", True)
        except DatasusCnesError:
            raise
        except error_perm as error:
            if str(error).startswith("550"):
                return False
            _raise("source_unavailable", True)
        except (FtpError, OSError, EOFError):
            _raise("source_unavailable", True)
        finally:
            self._close_ftp(ftp)
        return True

    @staticmethod
    def _metadata(ftp: FTP, remote_path: str) -> _Metadata:
        try:
            size = ftp.size(remote_path)
        except ValueError:
            _raise("metadata_invalid", True)
        modified = ftp.sendcmd(f"MDTM {remote_path}")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            _raise("metadata_invalid", True)
        if not isinstance(modified, str) or not fullmatch(r"213 [0-9]{14}", modified):
            _raise("metadata_invalid", True)
        return _Metadata(size, modified)

    @staticmethod
    def _retrieve(ftp: FTP, remote_path: str, destination: Path) -> _Download:
        digest = sha256()
        size = 0
        with destination.open("wb") as stream:

            def write(chunk: bytes) -> None:
                nonlocal size
                stream.write(chunk)
                digest.update(chunk)
                size += len(chunk)

            ftp.retrbinary(f"RETR {remote_path}", write, blocksize=_BLOCK_SIZE)
        return _Download(size, digest.hexdigest())

    @staticmethod
    def _close_ftp(ftp: FTP | None) -> None:
        if ftp is None:
            return
        try:
            ftp.quit()
        except Exception:
            pass
        try:
            _close(ftp)
        except Exception:
            pass

    @staticmethod
    def _decompress(dbc_path: Path, dbf_path: Path) -> None:
        _validate_dbc_file(dbc_path)
        try:
            decompress(str(dbc_path), str(dbf_path))
        except Exception:
            _raise("dbc_invalid", False)

    @staticmethod
    def _open_dbf(dbf_path: Path) -> object:
        try:
            return DBF(
                str(dbf_path),
                load=False,
                encoding="latin-1",
                parserclass=_PfFieldParser,
            )
        except Exception:
            _raise("dbf_invalid", False)

    @staticmethod
    def _validate_layout(table: object) -> None:
        try:
            layout = tuple(
                (field.name, field.type, field.length, field.decimal_count)
                for field in table.fields
            )
        except Exception:
            _raise("dbf_invalid", False)
        if layout != _PF_LAYOUT:
            _raise("schema_invalid", False)

    @staticmethod
    def _validate_competencia(table: object, expected: str) -> None:
        iterator = _table_iterator(table)
        try:
            for row in iterator:
                if _row_text(row, "COMPETEN") != expected:
                    _raise("competencia_invalid", False)
        except DatasusCnesError:
            raise
        except Exception:
            _raise("dbf_invalid", False)
        finally:
            _close(iterator)

    @staticmethod
    def _municipal_rows(table: object, tenant_id: str) -> Iterator[Mapping[str, object]]:
        iterator = _table_iterator(table)
        found = False
        try:
            for row in iterator:
                if _row_text(row, "CODUFMUN") == tenant_id:
                    found = True
                    yield row
        except DatasusCnesError:
            raise
        except Exception:
            _raise("dbf_invalid", False)
        finally:
            _close(iterator)
        if not found:
            _raise("source_not_published", True)


def _row_text(row: Mapping[str, object], field: str) -> str:
    try:
        value = row[field]
    except (KeyError, TypeError):
        _raise("field_invalid", False)
    if not isinstance(value, str):
        _raise("field_invalid", False)
    return value.strip()


def _table_iterator(table: object) -> Iterator[Mapping[str, object]]:
    try:
        return iter(table)
    except Exception:
        _raise("dbf_invalid", False)


def _inspect_dbf(path: Path, code: str) -> _DbfPhysical:
    try:
        file_size = path.stat().st_size
        with path.open("rb") as stream:
            raw = stream.read(12)
            if len(raw) != 12:
                _raise(code, False)
            header_size = int.from_bytes(raw[8:10], "little")
            descriptor_end = b""
            if 0 < header_size <= file_size:
                stream.seek(header_size - 1)
                descriptor_end = stream.read(1)
            stream.seek(-1, 2)
            last_byte = stream.read(1)
    except DatasusCnesError:
        raise
    except OSError:
        _raise(code, False)
    return _DbfPhysical(
        file_size,
        raw[0],
        int.from_bytes(raw[4:8], "little"),
        header_size,
        int.from_bytes(raw[10:12], "little"),
        descriptor_end,
        last_byte,
    )


def _valid_shape(physical: _DbfPhysical) -> bool:
    return (
        physical.version == _DBF_VERSION
        and physical.header_size == _DBF_HEADER_SIZE
        and physical.record_size == _DBF_RECORD_SIZE
        and physical.descriptor_end == b"\x0d"
    )


def _validate_dbc_file(path: Path) -> None:
    physical = _inspect_dbf(path, "dbc_invalid")
    if not _valid_shape(physical):
        _raise("dbc_invalid", False)


def _validate_dbf_file(path: Path) -> None:
    physical = _inspect_dbf(path, "dbf_invalid")
    if not _valid_shape(physical):
        _raise("dbf_invalid", False)
    expected = physical.header_size + physical.records * physical.record_size
    if physical.file_size == expected:
        return
    if physical.file_size == expected + 1 and physical.last_byte == b"\x1a":
        return
    _raise("dbf_invalid", False)
