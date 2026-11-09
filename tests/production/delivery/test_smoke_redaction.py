"""Evidência fechada nunca serializa dados de probes ou exceções."""

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from unittest.mock import Mock

import httpx
import pytest
from botocore.exceptions import ClientError, CredentialRetrievalError
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from jose import jwt

from scripts.promotion.smoke_production import (
    ASSERTIONS,
    ProductionSmoke,
    check_refresh,
    evidence,
    run_smoke,
)
from tests.production.delivery._smoke_fixtures import jwt_materials, login_fixture, sfn_fixture

RELEASE = "a" * 64
NOW = datetime(2026, 11, 9, tzinfo=UTC)


def test_evidencia_nao_contem_token_url_assinada_nem_id():
    sensitive = "token https://s3/path?X-Amz-Signature=secret user@example.org TENANT#secret"

    def broken():
        raise ValueError(sensitive)

    probes = dict.fromkeys(ASSERTIONS, broken)
    rows, ok = run_smoke(RELEASE, probes, NOW)
    assert not ok
    assert sensitive not in json.dumps(rows)
    assert all(set(row) == {"assertion", "timestamp", "release_hash"} for row in rows)
    assert all(row["assertion"].endswith(":fail") for row in rows)


def test_prova_ausente_ou_objeto_truthy_falha():
    for value in (None, {"token": "secret"}, "success", 1):
        rows, ok = run_smoke(RELEASE, dict.fromkeys(ASSERTIONS, lambda value=value: value), NOW)
        assert not ok
        assert all(row["assertion"].endswith(":fail") for row in rows)
    rows, ok = run_smoke(RELEASE, {}, NOW)
    assert not ok
    assert len(rows) == len(ASSERTIONS)


def test_somente_todas_assercao_executadas_passam():
    rows, ok = run_smoke(RELEASE, dict.fromkeys(ASSERTIONS, lambda: True), NOW)
    assert ok
    assert all(row["assertion"].endswith(":pass") for row in rows)


@pytest.mark.parametrize(
    ("release", "name"), [("token-secret", ASSERTIONS[0]), (RELEASE, "secret")]
)
def test_recusa_rotulos_ou_hash_nao_revisados(release, name):
    with pytest.raises(ValueError):
        evidence(name, True, release, NOW)


@pytest.fixture(scope="module")
def jwt_material():
    return jwt_materials()


@pytest.mark.parametrize(
    ("change", "passes"),
    [
        ({}, True),
        ({"aud": "other"}, False),
        ({"sub": "other"}, False),
        ({"scope": "openid"}, False),
        ({"exp": 1}, False),
    ],
)
def test_aud_preservado_depois_do_refresh(jwt_material, change, passes):
    pem, jwk = jwt_material
    issuer = "https://cognito-idp.us-east-2.amazonaws.com/example"
    api = "https://api.cnesdata.com.br"
    now = int(datetime.now(UTC).timestamp())
    claims = {
        "iss": issuer,
        "aud": api,
        "sub": "synthetic",
        "iat": now,
        "exp": now + 600,
        "scope": api + "/api.access",
    }
    original = jwt.encode(claims, pem, algorithm="RS256", headers={"kid": "demo"})
    refreshed = jwt.encode(claims | change, pem, algorithm="RS256", headers={"kid": "demo"})

    def handler(request):
        if request.url.path.endswith("openid-configuration"):
            return httpx.Response(200, json={"issuer": issuer, "jwks_uri": issuer + "/jwks"})
        if request.url.path.endswith("jwks"):
            return httpx.Response(200, json={"keys": [jwk]})
        return httpx.Response(200, json={"access_token": refreshed})

    config = {
        "issuer": issuer,
        "domain": "https://demo.auth.us-east-2.amazoncognito.com",
        "client_id": "demo",
    }
    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        if passes:
            assert check_refresh(http, config, {"access_token": original, "refresh_token": "dummy"})
        else:
            with pytest.raises(Exception):
                check_refresh(http, config, {"access_token": original, "refresh_token": "dummy"})


def serving_fixture():
    body = b'{"tenant_id":"demo","run_id":"demo-seed-v1"}'
    cfg = {
        "demo_tenant": "demo",
        "serving": {
            "bucket": "synthetic-bucket",
            "version_id": "demo-seed-v1",
            "body_sha256": sha256(body).hexdigest(),
        },
    }
    url = (
        "https://synthetic-bucket.s3.us-east-2.amazonaws.com/serving/demo/"
        "demo-seed-v1/overview.json?X-Amz-Algorithm=AWS4-HMAC-SHA256&"
        "X-Amz-SignedHeaders=host&X-Amz-Expires=120&X-Amz-Signature=synthetic"
    )
    envelope = {"url": url, "version_id": "demo-seed-v1", "expires_in": 120}
    return cfg, envelope, body


def test_serving_baixa_corpo_sem_bearer_cookie_tenant_ou_referrer():
    config, envelope, body = serving_fixture()
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.host == "api.cnesdata.com.br":
            return httpx.Response(
                200,
                json=envelope,
                headers={"cache-control": "private, no-store", "x-dataset-version": "demo-seed-v1"},
            )
        return httpx.Response(200, content=body)

    with httpx.Client(
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "danger"},
        cookies={"private": "danger"},
    ) as http:
        smoke = ProductionSmoke(config, http, None)
        smoke.access = {"access_token": "private"}
        assert smoke.serving()
    for name in ("authorization", "cookie", "x-tenant-id", "referer"):
        assert name not in requests[1].headers


@pytest.mark.parametrize(
    "change",
    [
        {"url": "https://evil.example/secret"},
        {"version_id": "other"},
        {"expires_in": -1},
        {"unexpected": "secret"},
    ],
)
def test_serving_recusa_envelope_antes_de_enviar_segredos(change):
    config, envelope, _ = serving_fixture()
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json=envelope | change,
            headers={"cache-control": "private, no-store", "x-dataset-version": "demo-seed-v1"},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        smoke = ProductionSmoke(config, http, None)
        smoke.access = {"access_token": "private"}
        rows, ok = run_smoke(RELEASE, {"serving_envelope_body": smoke.serving}, NOW)
    assert not ok
    assert len(requests) == 1
    assert all("secret" not in json.dumps(row) for row in rows)


@pytest.mark.parametrize("failure", ["exception", "http_body"])
def test_excecao_http_corpo_e_url_nao_vazam_na_evidencia(failure):
    config, _, _ = serving_fixture()

    def handler(request):
        if failure == "http_body":
            return httpx.Response(500, json={"token": "private", "email": "email@example.org"})
        raise httpx.ConnectError("email@example.org ?X-Amz-Signature=private", request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        smoke = ProductionSmoke(config, http, None)
        smoke.access = {"access_token": "private"}
        rows, ok = run_smoke(RELEASE, smoke.probes(), NOW)
    assert not ok
    assert not any(value in json.dumps(rows) for value in ("private", "email@", "X-Amz"))


def test_membership_mista_e_negacao_entre_tenants():
    cfg = {"demo_user": "synthetic", "demo_tenant": "demo", "other_tenant": "other"}

    def handler(request):
        if request.url.path.endswith("auth/me"):
            return httpx.Response(200, json={"subject": "synthetic"})
        if request.url.path.endswith("memberships"):
            return httpx.Response(200, json=[{"tenant_id": "demo"}])
        return httpx.Response(403 if "authorization" in request.headers else 401)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        smoke = ProductionSmoke(cfg, http, None)
        smoke.access = {"access_token": "private"}
        assert smoke.memberships()
        cfg["other_tenant"] = "demo"
        with pytest.raises(ValueError):
            smoke.memberships()


def test_cors_exige_negativas_de_origem_metodo_e_header():
    requests = []

    def handler(request):
        requests.append(request)
        allowed = len(requests) == 1
        return httpx.Response(
            200 if allowed else 400,
            headers={"access-control-allow-origin": "https://cnesdata.com.br"},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        assert ProductionSmoke({}, http, None).cors()
    assert len(requests) == 4


def test_object_lock_nao_confunde_governance_com_compliance():
    s3 = Mock()
    s3.get_object_lock_configuration.return_value = {
        "ObjectLockConfiguration": {
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 365}},
        }
    }
    session = Mock()
    session.client.return_value = s3
    with httpx.Client() as http:
        smoke = ProductionSmoke({"object_lock": {"audit_bucket": "audit"}}, http, session)
        with pytest.raises(ValueError):
            smoke.object_lock()
    s3.delete_object.assert_not_called()


@pytest.mark.parametrize(
    "drift", [None, "single", "retry", "trust", "revision", "policy", "empty", "partial"]
)
def test_sfn_exige_trust_policy_e_task_exatas(drift):
    cfg, session, iam, sfn = sfn_fixture()
    if drift == "single":
        cfg["execution_input"]["unit_ids"].pop()
        sfn.describe_execution.return_value["input"] = json.dumps(cfg["execution_input"])
        sfn.get_execution_history.return_value["events"].pop()
    elif drift == "retry":
        events = sfn.get_execution_history.return_value["events"]
        events.extend(events[:1])
    elif drift == "trust":
        iam.get_role.return_value["Role"]["AssumeRolePolicyDocument"]["Statement"][0][
            "Principal"
        ] = "*"
    elif drift == "revision":
        cfg["task_definition_arn"] += "other"
    elif drift == "policy":
        iam.get_role_policy.return_value = {"PolicyDocument": {"Statement": ["drift"]}}
    elif drift == "empty":
        sfn.get_execution_history.return_value["events"] = []
    elif drift == "partial":
        sfn.get_execution_history.return_value["nextToken"] = "next"
    with httpx.Client() as http:
        probe = ProductionSmoke({"sfn": cfg}, http, session)
        if drift in {None, "single", "retry"}:
            assert probe.sfn()
        else:
            with pytest.raises(ValueError):
                probe.sfn()


def test_object_lock_exige_ambas_negativas_e_retencao_intacta():
    s3, session = Mock(), Mock()
    session.client.return_value = s3
    s3.get_object_lock_configuration.return_value = {
        "ObjectLockConfiguration": {
            "ObjectLockEnabled": "Enabled",
            "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 365}},
        }
    }
    s3.get_object_retention.return_value = {
        "Retention": {
            "Mode": "COMPLIANCE",
            "RetainUntilDate": datetime.now(UTC) + timedelta(days=1),
        }
    }
    denial = ClientError({"Error": {"Code": "AccessDenied", "Message": "private"}}, "DeleteObject")
    s3.delete_object.side_effect = denial
    s3.put_object_retention.side_effect = denial
    cfg = {
        "audit_bucket": "audit",
        "approved_synthetic_fixture": True,
        "bucket": "cnesdata-cap-synthetic",
        "key": "synthetic",
        "version_id": "synthetic",
    }
    with httpx.Client() as http:
        probe = ProductionSmoke({"object_lock": cfg}, http, session)
        assert probe.object_lock()
        s3.put_object_retention.side_effect = None
        with pytest.raises(ValueError):
            probe.object_lock()


def _ra_certificates(cfg, key, name, now):
    config_path = Path(cfg["aws_config"])
    tmp_path = config_path.parent
    sections = []
    for kind, serial in (("valid", 1), ("revoked", 2)):
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(serial)
            .not_valid_before(now - timedelta(days=1))
            .not_valid_after(now + timedelta(days=1))
            .sign(key, hashes.SHA256())
        )
        path = tmp_path / (kind + ".pem")
        path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        cfg.update(
            {
                kind + "_certificate": str(path),
                kind + "_key": "/private/" + kind,
                kind + "_profile": kind,
            }
        )
        sections.append(
            f"[profile {kind}]\nregion=us-east-2\ncredential_process=/synthetic/helper "
            f"credential-process --certificate {path} --private-key /private/{kind} "
            "--trust-anchor-arn anchor --profile-arn profile --role-arn role\n"
        )
    config_path.write_text("\n".join(sections))


def _ra_fixture(tmp_path, monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "synthetic")])
    now = datetime.now(UTC)
    config_path = tmp_path / "config"
    cfg = {
        "aws_config": str(config_path),
        "helper": "/synthetic/helper",
        "role_arn": "role",
        "profile_arn": "profile",
        "trust_anchor_arn": "anchor",
        "crl_id": "crl",
        "revoked_serial": "2",
        "expected_session_arn": "session",
    }
    _ra_certificates(cfg, key, name, now)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
    revoked = x509.RevokedCertificateBuilder().serial_number(2).revocation_date(now).build()
    crl = (
        x509.CertificateRevocationListBuilder()
        .issuer_name(name)
        .last_update(now)
        .next_update(now + timedelta(days=1))
        .add_revoked_certificate(revoked)
        .sign(key, hashes.SHA256())
    ).public_bytes(serialization.Encoding.DER)
    cfg["crl_sha256"] = sha256(crl).hexdigest()
    ra, session = Mock(), Mock()
    session.client.return_value = ra
    ra.get_crl.return_value = {"crl": {"enabled": True, "trustAnchorArn": "anchor", "crlData": crl}}

    return cfg, session


def test_roles_anywhere_verifica_crl_serial_profiles_e_negacao(tmp_path, monkeypatch):
    import scripts.promotion.smoke_production as module

    cfg, session = _ra_fixture(tmp_path, monkeypatch)

    def profile_session(profile_name):
        actor = Mock()
        if profile_name == "valid":
            actor.client.return_value.get_caller_identity.return_value = {"Arn": "session"}
        else:
            actor.client.side_effect = CredentialRetrievalError(
                provider="custom-process", error_msg="AccessDenied"
            )
        return actor

    monkeypatch.setattr(module.boto3, "Session", profile_session)
    with httpx.Client() as http:
        probe = ProductionSmoke({"roles_anywhere": cfg}, http, session)
        assert probe.roles_anywhere()
        cfg["revoked_serial"] = "3"
        with pytest.raises(ValueError):
            probe.roles_anywhere()


@pytest.mark.parametrize(
    "drift", [None, "logout_slash", "logout_path", "logout_host", "location", "callback"]
)
def test_login_completo_aceita_logout_exato_e_recusa_deriva(jwt_material, drift):
    cfg, handler, replies = login_fixture(jwt_material)
    if drift == "logout_slash":
        cfg["cognito"]["logout_uri"] += "/"
    elif drift == "logout_path":
        cfg["cognito"]["logout_uri"] += "/unregistered"
    elif drift == "logout_host":
        cfg["cognito"]["logout_uri"] = "https://invalid.example"
    elif drift == "location":
        replies["logout"] += "/unregistered"
    elif drift == "callback":
        cfg["cognito"]["redirect_uri"] += "/unregistered"
    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        probe = ProductionSmoke(cfg, http, None)
        if drift is None:
            assert probe.login()
            assert probe.refresh()
        else:
            with pytest.raises(ValueError):
                probe.login()
            assert not probe.access


@pytest.mark.parametrize(
    "drift",
    [
        "unit",
        "context",
        "run",
        "wave",
        "dispatch",
        "owner",
        "lease",
        "network",
        "missing_unit",
        "actual_input",
    ],
)
def test_map_recusa_unidade_contexto_e_parametros_estaticos_nao_aprovados(drift):
    cfg, session, _, sfn = sfn_fixture()
    events = sfn.get_execution_history.return_value["events"]
    detail = events[1]["taskScheduledEventDetails"]
    params = json.loads(detail["parameters"])
    env = params["Overrides"]["ContainerOverrides"][0]["Environment"]
    names = {
        "unit": "UNIT_ID",
        "context": "TENANT_ID",
        "run": "RUN_ID",
        "wave": "WAVE_ID",
        "dispatch": "DISPATCH_ID",
        "owner": "EXECUTION_OWNER",
        "lease": "LEASE_SECONDS",
    }
    if drift in names:
        name = names[drift]
        next(x for x in env if x["Name"] == name)["Value"] = "unapproved"
    elif drift == "network":
        params["NetworkConfiguration"]["AwsvpcConfiguration"]["Subnets"] = ["unapproved"]
    elif drift == "missing_unit":
        events.pop()
    else:
        sfn.describe_execution.return_value["input"] = json.dumps(
            cfg["execution_input"] | {"run_id": "other"}
        )
    detail["parameters"] = json.dumps(params)
    with httpx.Client() as http:
        with pytest.raises(ValueError):
            ProductionSmoke({"sfn": cfg}, http, session).sfn()
