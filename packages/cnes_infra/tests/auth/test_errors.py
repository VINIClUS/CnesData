"""OAuthError: RFC 6749 §5.2 body shape."""
import pytest

from cnes_infra.auth.errors import OAuthError


def test_oauth_error_armazena_code_e_status_padrao():
    err = OAuthError("invalid_grant")
    assert err.code == "invalid_grant"
    assert err.status_code == 400
    assert err.description is None
    assert err.extra == {}


def test_oauth_error_aceita_description_e_status_customizado():
    err = OAuthError(
        "invalid_token",
        description="access_token_consumido",
        status_code=401,
    )
    assert err.description == "access_token_consumido"
    assert err.status_code == 401


def test_oauth_error_aceita_extra_dict():
    err = OAuthError("slow_down", extra={"interval": 10})
    assert err.extra == {"interval": 10}


def test_oauth_error_eh_excecao():
    err = OAuthError("invalid_request")
    assert isinstance(err, Exception)
    with pytest.raises(OAuthError) as exc_info:
        raise err
    assert exc_info.value.code == "invalid_request"
