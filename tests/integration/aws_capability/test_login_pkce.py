"""PKCE, URLs de authorize, senha sintética e arquivo de handoff."""

import re
import stat
from pathlib import Path

import pytest

from tests.integration.aws_capability._login_handoff import (
    handoff_path,
    new_password,
    remove_handoff,
    write_handoff,
)
from tests.integration.aws_capability._login_pkce import (
    Endpoint,
    authorize_url,
    challenge_of,
    new_authorize,
)
from tests.integration.aws_capability._login_ports import CALLBACK_URL, LISTEN_PORT, LOGOUT_URL
from tests.integration.aws_capability.contracts import RunSpec

API = "https://api.cnesdata.com.br"
ENDPOINT = Endpoint("https://dom.auth.us-east-2.amazoncognito.com", "CLIENT_CANARY")


def test_desafio_s256_confere_com_o_vetor_da_rfc_7636() -> None:
    verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    assert challenge_of(verifier) == "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"


def test_cada_authorize_tem_state_e_verifier_proprios() -> None:
    pairs = [new_authorize(API) for _ in range(4)]
    assert len({p.state for p in pairs}) == 4
    assert len({p.verifier for p in pairs}) == 4
    assert all(re.fullmatch(r"[A-Za-z0-9_-]{43,128}", p.verifier) for p in pairs)


def test_url_com_resource_leva_escopos_completos_e_s256() -> None:
    auth = new_authorize(API)
    url = authorize_url(ENDPOINT, auth)
    assert url.startswith(ENDPOINT.origin + "/oauth2/authorize?")
    query = dict(part.split("=", 1) for part in url.split("?", 1)[1].split("&"))
    assert query["response_type"] == "code"
    assert query["code_challenge_method"] == "S256"
    assert query["code_challenge"] == challenge_of(auth.verifier)
    assert query["state"] == auth.state
    assert query["resource"] == "https%3A%2F%2Fapi.cnesdata.com.br"
    assert query["redirect_uri"] == "http%3A%2F%2Flocalhost%3A53682%2Fcallback"
    scopes = query["scope"].split("+")
    api_scope = "https%3A%2F%2Fapi.cnesdata.com.br%2Fapi.access"
    assert scopes == ["openid", "email", "profile", api_scope]


def test_url_sem_resource_omite_o_parametro() -> None:
    url = authorize_url(ENDPOINT, new_authorize(None))
    assert "resource=" not in url


def test_callback_e_logout_usam_a_porta_fixa_compartilhada() -> None:
    assert LISTEN_PORT == 53682
    assert f"http://localhost:{LISTEN_PORT}/callback" == CALLBACK_URL
    assert f"http://localhost:{LISTEN_PORT}/" == LOGOUT_URL


def test_repr_nao_vaza_state_nem_verifier_nem_client() -> None:
    auth = new_authorize(API)
    text = repr(auth) + repr(ENDPOINT)
    assert auth.state not in text
    assert auth.verifier not in text
    assert "CLIENT_CANARY" not in text


def test_senha_sintetica_atende_a_politica_do_pool() -> None:
    for _ in range(50):
        password = new_password()
        assert len(password) >= 14
        assert re.search(r"[a-z]", password)
        assert re.search(r"[A-Z]", password)
        assert re.search(r"[0-9]", password)
        assert re.search(r"[^A-Za-z0-9]", password)
        assert not re.search(r"[\s=]", password)


def _spec(directory: Path) -> RunSpec:
    directory.mkdir(mode=0o700)
    return RunSpec("111111111111", "us-east-2", "a1b2c3d4", 2000.0, directory)


def test_handoff_privado_atomico_com_formato_fixo(tmp_path: Path) -> None:
    spec = _spec(tmp_path / "private")
    write_handoff(spec, "https://x/authorize?a=b", "u@example.invalid", "PASS-CANARY-1a!")
    path = handoff_path(spec)
    assert path == spec.journal_dir / "a1b2c3d4.login"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.read_text() == (
        "url=https://x/authorize?a=b\nusername=u@example.invalid\npassword=PASS-CANARY-1a!\n"
    )
    assert [p.name for p in spec.journal_dir.iterdir()] == ["a1b2c3d4.login"]


def test_remove_handoff_e_idempotente(tmp_path: Path) -> None:
    spec = _spec(tmp_path / "private")
    remove_handoff(spec)
    write_handoff(spec, "u", "n", "p")
    remove_handoff(spec)
    remove_handoff(spec)
    assert not handoff_path(spec).exists()


def test_remove_handoff_nao_segue_symlink(tmp_path: Path) -> None:
    spec = _spec(tmp_path / "private")
    alvo = tmp_path / "alvo"
    alvo.write_text("mantido")
    handoff_path(spec).symlink_to(alvo)
    remove_handoff(spec)
    assert alvo.read_text() == "mantido"
    assert not handoff_path(spec).is_symlink()


def test_arquivo_pre_existente_e_substituido_com_modo_privado(tmp_path: Path) -> None:
    spec = _spec(tmp_path / "private")
    handoff_path(spec).write_text("antigo")
    handoff_path(spec).chmod(0o644)
    write_handoff(spec, "u", "n", "p")
    assert stat.S_IMODE(handoff_path(spec).stat().st_mode) == 0o600
    assert "antigo" not in handoff_path(spec).read_text()


@pytest.mark.parametrize("field", ["url", "username", "password"])
def test_handoff_recusa_quebra_de_linha(tmp_path: Path, field: str) -> None:
    spec = _spec(tmp_path / "private")
    values = {"url": "u", "username": "n", "password": "p", field: "a\nb"}
    with pytest.raises(ValueError, match="handoff=invalid_field"):
        write_handoff(spec, values["url"], values["username"], values["password"])


def test_remove_handoff_ignora_run_id_fora_do_formato(tmp_path: Path) -> None:
    spec = _spec(tmp_path / "private")
    alvo = tmp_path / "private" / "x.login"
    alvo.write_text("mantido")
    hostil = RunSpec("111111111111", "us-east-2", "../private/x", 2000.0, spec.journal_dir)
    remove_handoff(hostil)
    assert alvo.read_text() == "mantido"
