"""Probe cognito-login ponta a ponta sobre IdP, listener e AWS sintéticos."""

from pathlib import Path

import pytest

from tests.integration.aws_capability._login_handoff import handoff_path
from tests.integration.aws_capability._login_lab_testing import (
    LoginWorld,
    create_root,
    make_login_world,
)
from tests.integration.aws_capability.contracts import ServiceFailure

STRING_KEYS = {
    "discovery_status", "discovery_issuer_ok", "discovery_jwks_https", "domain_ready",
    "domain_ready_polls", "login", "login_seconds", "a_status", "a_error", "a_token_use",
    "a_scope_api", "a_client_id_ok", "a_aud", "a_claims", "a_verifier", "a_id_aud_is_client",
    "refresh_status", "refresh_error", "refresh_aud", "refresh_verifier", "sdk_refresh",
    "sdk_refresh_aud", "sdk_refresh_verifier", "b_status", "b_error", "b_aud", "b_verifier",
    "neg_wrong_verifier_status", "neg_wrong_verifier_error", "neg_right_after_wrong_status",
    "neg_right_after_wrong_error", "neg_right_after_wrong_consumed", "neg_redirect_status",
    "neg_redirect_error", "neg_reuse_status", "neg_reuse_error", "sdk_password_auth",
}
API = "https://api.cnesdata.com.br"


def run_probe(world: LoginWorld) -> dict[str, object]:
    lab = world.lab
    root = create_root(lab)
    return dict(lab.services.probe([root], lab.spec, lab.context))


def test_caminho_feliz_registra_todas_as_chaves_e_o_veredito_esperado(tmp_path: Path) -> None:
    observed = run_probe(make_login_world(tmp_path))
    assert set(observed) == STRING_KEYS
    assert observed["login"] == "ok"
    assert observed["discovery_status"] == 200
    assert observed["discovery_issuer_ok"] is True
    assert observed["discovery_jwks_https"] is True
    assert observed["a_status"] == 200
    assert observed.get("a_token_use") == "access"
    assert observed["a_scope_api"] is True
    assert observed["a_client_id_ok"] is True
    assert observed["a_aud"] == API
    assert observed["a_id_aud_is_client"] is True
    assert observed["a_verifier"] == "ok"
    claims = observed["a_claims"]
    assert isinstance(claims, list)
    assert claims == sorted(claims)
    assert {"token_use", "scope", "client_id", "aud"} <= set(claims)


def test_refresh_no_endpoint_mantem_aud_e_o_verifier_aceita(tmp_path: Path) -> None:
    observed = run_probe(make_login_world(tmp_path))
    assert observed["refresh_status"] == 200
    assert observed["refresh_aud"] == API
    assert observed["refresh_verifier"] == "ok"


def test_refresh_no_endpoint_que_perde_aud_e_apenas_observado(tmp_path: Path) -> None:
    observed = run_probe(make_login_world(tmp_path, refresh_keeps_aud=False))
    assert observed["refresh_aud"] == "absent"
    assert observed["refresh_verifier"] == "audience"


CASES = [(False, "absent", "audience"), (True, API, "ok")]


@pytest.mark.parametrize(("keeps", "aud", "verdict"), CASES)
def test_refresh_pelo_sdk_registra_aud_e_veredito_sem_afirmar(
    tmp_path: Path, keeps: bool, aud: str, verdict: str
) -> None:
    observed = run_probe(make_login_world(tmp_path, sdk_refresh_aud=keeps))
    assert observed["sdk_refresh"] == "ok"
    assert observed["sdk_refresh_aud"] == aud
    assert observed["sdk_refresh_verifier"] == verdict


def test_codigo_sem_resource_registra_ausencia_de_aud(tmp_path: Path) -> None:
    observed = run_probe(make_login_world(tmp_path))
    assert observed["b_status"] == 200
    assert observed["b_aud"] == "absent"
    assert observed["b_verifier"] == "audience"


def test_negativos_registram_codigo_e_error(tmp_path: Path) -> None:
    observed = run_probe(make_login_world(tmp_path))
    assert (observed["neg_wrong_verifier_status"], observed["neg_wrong_verifier_error"]) == (
        400, "invalid_grant",
    )
    assert observed["neg_right_after_wrong_status"] == 200
    assert observed["neg_right_after_wrong_consumed"] is False
    assert (observed["neg_redirect_status"], observed["neg_redirect_error"]) == (
        400, "invalid_grant",
    )
    assert (observed["neg_reuse_status"], observed["neg_reuse_error"]) == (400, "invalid_grant")


def test_verifier_errado_que_consome_o_codigo_e_registrado(tmp_path: Path) -> None:
    observed = run_probe(make_login_world(tmp_path, consume_on_bad_verifier=True))
    assert observed["neg_right_after_wrong_status"] == 400
    assert observed["neg_right_after_wrong_error"] == "invalid_grant"
    assert observed["neg_right_after_wrong_consumed"] is True


def test_password_auth_no_client_do_modulo_e_rejeitado(tmp_path: Path) -> None:
    observed = run_probe(make_login_world(tmp_path))
    assert observed.get("sdk_password_auth") == "InvalidParameterException"


def test_handoff_existe_durante_a_espera_e_some_depois(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    run_probe(world)
    text = world.listener.handoff_text
    assert text is not None
    assert text.startswith("url=https://cnesdata-cap-a1b2c3d4.auth.us-east-2.amazoncognito.com")
    assert "username=cap-a1b2c3d4@example.invalid\npassword=" in text
    assert not handoff_path(world.lab.spec).exists()


def test_listener_segue_a_cadeia_e_termina_com_pagina_simples(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    run_probe(world)
    listener = world.listener
    assert len(listener.redirects) == 3
    assert all("/oauth2/authorize?" in url for url in listener.redirects)
    assert "resource=" not in listener.redirects[0]
    assert "resource=" in listener.redirects[1]
    assert listener.finished is not None
    assert listener.closed is True
    assert listener.steps == 4


def test_troca_de_codigos_so_depois_da_cadeia_completa(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    run_probe(world)
    kinds = [method for method, _ in world.idp.requests]
    first_post = kinds.index("POST")
    assert kinds.count("AUTHORIZE") == 4
    assert max(i for i, kind in enumerate(kinds) if kind == "AUTHORIZE") < first_post
    assert kinds.count("POST") == 7


def test_state_divergente_e_registrado_e_o_codigo_nao_e_usado(tmp_path: Path) -> None:
    world = make_login_world(tmp_path, wrong_state_at=2)
    observed = run_probe(world)
    assert observed == {
        "discovery_status": 200, "discovery_issuer_ok": True, "discovery_jwks_https": True,
        "domain_ready": observed["domain_ready"], "domain_ready_polls": 3,
        "login": "state_mismatch", "login_seconds": observed["login_seconds"],
    }
    assert not any(record["used"] for record in world.idp.codes.values())
    assert not handoff_path(world.lab.spec).exists()
    assert world.listener.closed is True


def test_timeout_de_login_registra_remove_handoff_e_nao_troca_codigos(tmp_path: Path) -> None:
    world = make_login_world(tmp_path, human_seconds=10_000.0)
    observed = run_probe(world)
    assert observed["login"] == "timeout"
    assert "a_status" not in observed
    assert world.listener.waits
    assert max(world.listener.waits) <= 600.0
    assert not handoff_path(world.lab.spec).exists()
    assert world.listener.closed is True
    assert ("POST", "/oauth2/token") not in world.idp.requests


def test_callback_com_error_encerra_sem_trocar_codigos(tmp_path: Path) -> None:
    world = make_login_world(tmp_path, error_at=1)
    observed = run_probe(world)
    assert observed["login"] == "callback_error"
    assert "a_status" not in observed
    assert not handoff_path(world.lab.spec).exists()


def test_dominio_que_nao_fica_pronto_registra_timeout_sem_handoff_nem_listener(
    tmp_path: Path,
) -> None:
    world = make_login_world(tmp_path, ready_after=10_000)
    observed = run_probe(world)
    assert observed["domain_ready"] == "timeout"
    assert observed["domain_ready_polls"] == 36
    assert "login" not in observed
    assert world.listener.handoff_text is None
    assert world.listener.steps == 0
    assert not handoff_path(world.lab.spec).exists()
    assert world.lab.pauses.count(5.0) == 35


def test_perfil_login_exige_portas_http_e_listener(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    lab = world.lab
    root = create_root(lab)
    from dataclasses import replace

    from tests.integration.aws_capability.services import AwsServices

    bare = AwsServices(lab.spec, replace(lab.services.ports, http=None))
    with pytest.raises(ServiceFailure) as error:
        bare.probe([root], lab.spec, lab.context)
    assert error.value.args == ("probe=login_ports_unavailable",)
    assert "create-user-pool-client" not in lab.aws.operations()


def test_evidencia_nao_carrega_segredos_nem_ids_nem_urls(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    observed = run_probe(world)
    text = repr(observed)
    pool = next(iter(world.lab.aws.pools.values()))
    client_id = next(iter(pool.clients))
    secrets_ = [client_id, pool.pool_id, "refresh-", "code-", "eyJ", "?", "state="]
    secrets_.append("example.invalid")
    secrets_ += [str(row["password"]) for row in pool.users.values()]
    assert not any(secret in text for secret in secrets_)
    assert "https://" not in text.replace(API, "")
