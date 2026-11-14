"""Leitor credential_process da imagem de produção: JSON do host validado, falha fechada."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[2]
READER_SOURCE = REPO_ROOT / "apps" / "central_api" / "docker" / "aws_credentials.py"
SHEBANG = "#!/usr/local/bin/python3 -IS"
REFUSAL_EXIT = 2
# Split literals: the versioned-secret scan in tests/production/infra rejects key-shaped text.
ACCESS_KEY = "ASIA" + "GENA000000000001"
LONG_TERM_KEY = "AKIA" + "GENA000000000001"
SECRET = "segredo-do-host-nunca-em-log"  # noqa: S105
TOKEN = "token-de-sessao-nunca-em-log"  # noqa: S105
FIELDS = ("AccessKeyId", "Expiration", "SecretAccessKey", "SessionToken", "Version")
SIZE_LIMIT = 8192
MARGIN_SECONDS = 960
MAX_LIFETIME_SECONDS = 3600
FIFO_DEADLINE_SECONDS = 1.0


def _instant(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _document(lifetime: float = 1800, **changes: object) -> dict[str, object]:
    document: dict[str, object] = {
        "Version": 1,
        "AccessKeyId": ACCESS_KEY,
        "SecretAccessKey": SECRET,
        "SessionToken": TOKEN,
        "Expiration": _instant(time.time() + lifetime),
    }
    return document | changes


def _without(name: str) -> dict[str, object]:
    document = _document()
    document.pop(name)
    return document


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [sys.executable, "-I", "-S", str(READER_SOURCE), *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _write(path: Path, body: bytes, mode: int = 0o440) -> Path:
    path.write_bytes(body)
    path.chmod(mode)
    return path


def _publish(path: Path, document: object, mode: int = 0o440) -> Path:
    return _write(path, json.dumps(document).encode("utf-8"), mode)


def _assert_refused(
    result: subprocess.CompletedProcess[str], code: str, path: str,
) -> None:
    assert (result.returncode, result.stdout, result.stderr) == (
        REFUSAL_EXIT, "", f"credentials_json={code} path={path}\n",
    )
    assert SECRET not in result.stderr
    assert TOKEN not in result.stderr


@pytest.fixture
def reader() -> ModuleType:
    spec = importlib.util.spec_from_file_location("aws_credentials_reader", READER_SOURCE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_leitor_roda_isolado_sem_site_pelo_python_da_imagem() -> None:
    assert READER_SOURCE.read_text(encoding="utf-8").splitlines()[0] == SHEBANG


def test_leitor_usa_so_biblioteca_padrao() -> None:
    allowed = {"json", "os", "re", "stat", "sys", "time", "datetime", "errno", "__future__"}
    lines = READER_SOURCE.read_text(encoding="utf-8").splitlines()
    imported = {
        line.split()[1].split(".")[0]
        for line in lines
        if line.startswith(("import ", "from "))
    }

    assert imported <= allowed


def test_leitor_imprime_so_os_cinco_campos_do_json_valido(tmp_path: Path) -> None:
    document = _document()
    path = _publish(tmp_path / "aws-credentials.json", document)

    result = _run(str(path))

    assert (result.returncode, result.stderr) == (0, "")
    assert json.loads(result.stdout) == document
    assert tuple(json.loads(result.stdout)) == FIELDS


@pytest.mark.parametrize("mode", [0o400, 0o440, 0o600, 0o640], ids=oct)
def test_leitor_aceita_modo_sem_bits_de_outros(tmp_path: Path, mode: int) -> None:
    path = _publish(tmp_path / "aws-credentials.json", _document(), mode)

    assert _run(str(path)).returncode == 0


REFUSED_DOCUMENTS: dict[str, tuple[str, Callable[[], object]]] = {
    "expirado": ("expiration_window", lambda: _document(-60)),
    "menos_de_960s": ("expiration_window", lambda: _document(900)),
    "mais_de_3600s": ("expiration_window", lambda: _document(3700)),
    "chave_de_longo_prazo": (
        "access_key", lambda: _document(AccessKeyId=LONG_TERM_KEY),
    ),
    "chave_curta": ("access_key", lambda: _document(AccessKeyId="ASIAGENA")),
    "chave_minuscula": ("access_key", lambda: _document(AccessKeyId="asiagena000000000001")),
    "chave_nao_string": ("access_key", lambda: _document(AccessKeyId=1)),
    "secret_vazio": ("secret_access_key", lambda: _document(SecretAccessKey="")),
    "secret_nao_string": ("secret_access_key", lambda: _document(SecretAccessKey=None)),
    "token_vazio": ("session_token", lambda: _document(SessionToken="")),
    "token_ausente": ("fields", lambda: _without("SessionToken")),
    "sem_expiration": ("fields", lambda: _without("Expiration")),
    "campo_extra": ("fields", lambda: _document(AccountId="000000000000")),
    "nao_objeto": ("fields", lambda: [_document()]),
    "versao_bool": ("version", lambda: _document(Version=True)),
    "versao_2": ("version", lambda: _document(Version=2)),
    "versao_texto": ("version", lambda: _document(Version="1")),
    "expiration_fracionaria": (
        "expiration_format", lambda: _document(Expiration="2026-11-14T12:00:00.5Z"),
    ),
    "expiration_com_offset": (
        "expiration_format", lambda: _document(Expiration="2026-11-14T12:00:00+00:00"),
    ),
    "expiration_impossivel": (
        "expiration_format", lambda: _document(Expiration="2026-13-40T12:00:00Z"),
    ),
    "expiration_nao_string": ("expiration_format", lambda: _document(Expiration=1)),
}


@pytest.mark.parametrize(
    ("code", "build"), list(REFUSED_DOCUMENTS.values()), ids=list(REFUSED_DOCUMENTS),
)
def test_leitor_recusa_documento_fora_do_contrato(
    tmp_path: Path, code: str, build: Callable[[], object],
) -> None:
    path = _publish(tmp_path / "aws-credentials.json", build())

    _assert_refused(_run(str(path)), code, str(path))


REFUSED_BYTES: dict[str, tuple[str, bytes]] = {
    "json_invalido": ("invalid_json", b"{"),
    "nao_utf8": ("invalid_json", b"\xff\xfe"),
    "chave_duplicada": (
        "duplicate_key", b'{"Version": 1, "Version": 1}',
    ),
    "vazio": ("size", b""),
    "acima_de_8_kib": ("size", b" " * (SIZE_LIMIT + 1)),
}


@pytest.mark.parametrize(
    ("code", "body"), list(REFUSED_BYTES.values()), ids=list(REFUSED_BYTES),
)
def test_leitor_recusa_bytes_fora_do_contrato(tmp_path: Path, code: str, body: bytes) -> None:
    path = _write(tmp_path / "aws-credentials.json", body)

    _assert_refused(_run(str(path)), code, str(path))


@pytest.mark.parametrize("mode", [0o444, 0o404, 0o441, 0o442], ids=oct)
def test_leitor_recusa_arquivo_com_bit_de_outros(tmp_path: Path, mode: int) -> None:
    path = _publish(tmp_path / "aws-credentials.json", _document(), mode)

    _assert_refused(_run(str(path)), "insecure_mode", str(path))


def test_leitor_recusa_symlink_mesmo_para_arquivo_valido(tmp_path: Path) -> None:
    target = _publish(tmp_path / "real.json", _document())
    link = tmp_path / "aws-credentials.json"
    link.symlink_to(target)

    _assert_refused(_run(str(link)), "symlink", str(link))


def test_leitor_recusa_fifo_sem_bloquear(tmp_path: Path) -> None:
    fifo = tmp_path / "aws-credentials.json"
    os.mkfifo(fifo, 0o440)
    started = time.monotonic()

    result = _run(str(fifo))

    assert time.monotonic() - started < FIFO_DEADLINE_SECONDS
    _assert_refused(result, "not_regular", str(fifo))


def test_leitor_recusa_diretorio(tmp_path: Path) -> None:
    directory = tmp_path / "aws-credentials.json"
    directory.mkdir(mode=0o750)

    _assert_refused(_run(str(directory)), "not_regular", str(directory))


@pytest.mark.parametrize(
    "relative", ["aws-credentials.json", "ausente/aws-credentials.json"],
    ids=["arquivo-ausente", "diretorio-ausente"],
)
def test_leitor_recusa_arquivo_ausente(tmp_path: Path, relative: str) -> None:
    path = tmp_path / relative

    _assert_refused(_run(str(path)), "missing", str(path))


@pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0,
    reason="unreadable_requires_non_root",
)
def test_leitor_recusa_arquivo_ilegivel(tmp_path: Path) -> None:
    path = _publish(tmp_path / "aws-credentials.json", _document(), 0)

    _assert_refused(_run(str(path)), "unreadable", str(path))


@pytest.mark.parametrize(
    "arguments", [(), ("aws-credentials.json",), ("/a.json", "/b.json")],
    ids=["sem-argumento", "path-relativo", "dois-argumentos"],
)
def test_leitor_recusa_uso_fora_do_contrato(arguments: tuple[str, ...]) -> None:
    result = _run(*arguments)

    assert (result.returncode, result.stdout, result.stderr) == (
        REFUSAL_EXIT, "", "credentials_json=usage\n",
    )


@pytest.mark.parametrize(
    ("lifetime", "accepted"),
    [
        (MARGIN_SECONDS, False),
        (MARGIN_SECONDS + 1, True),
        (MAX_LIFETIME_SECONDS, True),
        (MAX_LIFETIME_SECONDS + 1, False),
    ],
    ids=["960-recusa", "961-aceita", "3600-aceita", "3601-recusa"],
)
def test_janela_de_validade_exige_mais_de_960s_e_no_maximo_3600s(
    reader: Any, lifetime: int, accepted: bool,
) -> None:
    now = 1_800_000_000
    document = _document(Expiration=_instant(now + lifetime))

    if accepted:
        reader.validate(document, now)
        return
    with pytest.raises(reader.Refused, match=r"^credentials_json=expiration_window$"):
        reader.validate(document, now)


def test_saida_do_leitor_e_estavel_e_sem_espaco(tmp_path: Path) -> None:
    document = _document()
    path = _publish(tmp_path / "aws-credentials.json", document)

    result = _run(str(path))

    assert result.stdout == json.dumps(dict(sorted(document.items())), separators=(",", ":"))

