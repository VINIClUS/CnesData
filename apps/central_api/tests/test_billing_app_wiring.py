"""Composição do billing no lifespan: aws, local e legado."""
import dataclasses
import hashlib
import hmac
import inspect
import json
import logging
import time
from unittest.mock import Mock, patch

import pytest

AWS_TENANT = "354130"
AWS_ISSUER = "https://id.example.test"
ACCEPTED_BEARER = "valid"
SECRET_API = "sk_test_SEGREDO_API"  # noqa: S105
SECRET_WEBHOOK = "whsec_SEGREDO_WEBHOOK"  # noqa: S105
API_ARN = "arn:aws:secretsmanager:us-east-1:000000000000:secret:api"
WEBHOOK_ARN = "arn:aws:secretsmanager:us-east-1:000000000000:secret:webhook"
TABLE = "cnesdata-test-control-plane"
KEY = "k" * 16
CHECKOUT_BODY = {
    "billing_account_id": "ba_x", "plan_version_id": "pv_x", "idempotency_key": KEY,
}


def _make_app():
    with (
        patch("central_api.app.init_telemetry"),
        patch("central_api.deps.install_rls_listener"),
        patch("central_api.deps.instrument_engine"),
        patch("central_api.deps.install_query_counter"),
        patch("central_api.deps.create_engine"),
    ):
        from central_api.app import create_app
        return create_app()


def _bearer(tenant: str = AWS_TENANT) -> dict[str, str]:
    return {"Authorization": f"Bearer {ACCEPTED_BEARER}", "X-Tenant-Id": tenant}


class _Verifier:
    def verify(self, token: str):
        from cnes_infra.auth import OidcPrincipal

        return OidcPrincipal(issuer=AWS_ISSUER, subject="user-1", email=None, display_name=None)


def _membership(tenant_id: str, user_id: str):
    from datetime import UTC, datetime

    from cnes_domain.control_plane.entities import Membership

    if tenant_id != AWS_TENANT:
        return None
    return Membership(
        tenant_id=tenant_id, user_id=user_id, role="viewer",
        created_at=datetime(2026, 9, 1, tzinfo=UTC), oidc_issuer=AWS_ISSUER,
    )


def _aws_runtime(billing_storage):
    from central_api.auth import MembershipAuthorizer
    from central_api.composition import AwsApiServices, RuntimeComponents

    control_plane = Mock(name="control_plane")
    control_plane.get_membership.side_effect = _membership
    return RuntimeComponents(
        control_plane=control_plane, object_store=Mock(), executor=Mock(), audit_sink=Mock(),
        raw_ingestion=Mock(), source_catalog=Mock(), run_planning=Mock(),
        services=AwsApiServices(
            membership_authorizer=MembershipAuthorizer(control_plane, Mock()),
            serving_access=Mock(),
            billing_storage=billing_storage,
        ),
    )


def _aws_env(monkeypatch) -> None:
    values = {
        "PROFILE": "aws",
        "AUTH_MODE": "oidc",
        "AWS_REGION": "us-east-1",
        "AWS_CONTROL_PLANE_TABLE": TABLE,
        "AWS_DATA_BUCKET": "cnesdata-test-data",
        "AWS_AUDIT_BUCKET": "cnesdata-test-audit",
        "AWS_STATE_MACHINE_ARN": (
            "arn:aws:states:us-east-1:000000000000:stateMachine:cnesdata-test"
        ),
        "AWS_PROCESSOR_CONTAINER_NAME": "processor",
        "AWS_AUDIT_RETENTION_DAYS": "365",
        "OIDC_ISSUER": AWS_ISSUER,
        "OIDC_AUDIENCE": "cnesdata-dashboard",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _session_spy(secrets: dict[str, str] | None = None) -> Mock:
    session = Mock(name="session")
    secrets_client = Mock(name="secretsmanager")
    secrets_client.get_secret_value.side_effect = lambda **kwargs: {
        "SecretString": (secrets or {})[kwargs["SecretId"]]
    }
    session.client.side_effect = lambda name, **_: (
        secrets_client if name == "secretsmanager" else Mock()
    )
    return session


def _secretsmanager_calls(session: Mock) -> list:
    return [c for c in session.client.call_args_list if c.args[:1] == ("secretsmanager",)]


def _stripe_env(monkeypatch) -> None:
    values = {
        "BILLING_MODE": "stripe",
        "STRIPE_SECRET_KEY_SECRET_ARN": API_ARN,
        "STRIPE_WEBHOOK_SECRET_SECRET_ARN": WEBHOOK_ARN,
        "BILLING_SUCCESS_URL": "https://app.example.test/billing/success",
        "BILLING_CANCEL_URL": "https://app.example.test/billing/cancel",
        "BILLING_PORTAL_RETURN_URL": "https://app.example.test/billing",
        "BILLING_RETURN_ORIGINS": "https://app.example.test",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _storage():
    from cnes_infra.billing import BillingStorage

    return BillingStorage(Mock(name="dynamodb"), TABLE)


def _harness(monkeypatch, session: Mock, storage=None):
    from fastapi.testclient import TestClient

    _aws_env(monkeypatch)
    build = patch("central_api.deps.build_runtime", return_value=_aws_runtime(storage))
    verifier = patch("central_api.deps.OidcVerifier", return_value=_Verifier())
    sess = patch("central_api.deps.Session", return_value=session)
    return build, verifier, sess, TestClient(_make_app())


@pytest.mark.parametrize("explicit", [False, True])
def test_aws_disabled_nao_toca_secrets_manager_e_responde_404(monkeypatch, explicit) -> None:
    if explicit:
        monkeypatch.setenv("BILLING_MODE", "disabled")
    session = _session_spy()
    build, verifier, sess, client = _harness(monkeypatch, session)
    with build, verifier, sess, client:
        checkout = client.post(
            "/api/v1/billing/checkout", json=CHECKOUT_BODY, headers=_bearer(),
        )
        status = client.get(
            "/api/v1/billing/status?billing_account_id=x", headers=_bearer(),
        )
        webhook = client.post("/api/v1/billing/webhooks/stripe", content=b"{}")

    assert _secretsmanager_calls(session) == []
    assert (checkout.status_code, checkout.json()) == (404, {"detail": "billing_disabled"})
    assert status.status_code == 200
    assert status.json()["state"] == "disabled"
    assert (webhook.status_code, webhook.json()) == (404, {"detail": "billing_disabled"})


def _stripe_client(monkeypatch, storage=None):
    _stripe_env(monkeypatch)
    session = _session_spy({API_ARN: SECRET_API, WEBHOOK_ARN: SECRET_WEBHOOK})
    return session, _harness(monkeypatch, session, storage or _storage())


def test_aws_stripe_le_segredos_uma_vez_e_valida_assinatura_do_webhook(monkeypatch) -> None:
    session, (build, verifier, sess, client) = _stripe_client(monkeypatch)
    with build as built, verifier, sess, client:
        invalid = client.post(
            "/api/v1/billing/webhooks/stripe", content=b"{}",
            headers={"Stripe-Signature": "t=1,v1=abc"},
        )
        missing = client.post("/api/v1/billing/webhooks/stripe", content=b"{}")

    assert len(_secretsmanager_calls(session)) == 1
    assert built.call_args.args[2] is session
    assert (invalid.status_code, invalid.json()) == (
        400, {"detail": "stripe_signature_invalid"},
    )
    assert missing.status_code == 400


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("post", "/api/v1/billing/accounts", {"idempotency_key": KEY}),
        ("post", "/api/v1/billing/checkout", CHECKOUT_BODY),
        ("post", "/api/v1/billing/portal",
         {"billing_account_id": "ba_x", "idempotency_key": KEY}),
        ("get", "/api/v1/billing/status?billing_account_id=x", None),
    ],
)
def test_aws_stripe_rotas_sem_bearer_retornam_401(monkeypatch, method, path, body) -> None:
    _, (build, verifier, sess, client) = _stripe_client(monkeypatch)
    with build, verifier, sess, client:
        response = getattr(client, method)(path, **({"json": body} if body else {}))

    assert response.status_code == 401
    assert response.json() == {"detail": "auth_required"}


def test_aws_stripe_tenant_sem_membership_retorna_403(monkeypatch) -> None:
    _, (build, verifier, sess, client) = _stripe_client(monkeypatch)
    with build, verifier, sess, client:
        response = client.get(
            "/api/v1/billing/status?billing_account_id=x", headers=_bearer("999999"),
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "tenant_not_allowed"}


def test_aws_stripe_nao_vaza_segredos_em_estado_logs_ou_runtime(
    monkeypatch, caplog,
) -> None:
    _, (build, verifier, sess, client) = _stripe_client(monkeypatch)
    with caplog.at_level(logging.DEBUG), build as built, verifier, sess, client:
        client.post(
            "/api/v1/billing/webhooks/stripe", content=b"{}",
            headers={"Stripe-Signature": "t=1,v1=abc"},
        )
        state_text = "".join(
            f"{name}={value!r}" for name, value in client.app.state._state.items()
        )
        runtime_text = repr(client.app.state.runtime)

    assert built is not None
    for secret in (SECRET_API, SECRET_WEBHOOK):
        assert secret not in state_text
        assert secret not in caplog.text
        assert secret not in runtime_text


def test_aws_stripe_sem_billing_storage_falha_o_startup(monkeypatch) -> None:
    from cnes_infra.billing import BillingConfigurationError

    _stripe_env(monkeypatch)
    session = _session_spy({API_ARN: SECRET_API, WEBHOOK_ARN: SECRET_WEBHOOK})
    build, verifier, sess, client = _harness(monkeypatch, session, None)
    with build, verifier, sess, pytest.raises(BillingConfigurationError) as error:
        with client:
            pass

    assert error.value.code == "billing_dynamodb_required"


def test_create_aws_clients_mantem_assinatura() -> None:
    from central_api.composition import build_runtime
    from cnes_infra.aws import create_aws_clients

    assert list(inspect.signature(create_aws_clients).parameters) == ["settings", "session"]
    assert list(inspect.signature(build_runtime).parameters) == [
        "profile", "values", "session", "execution_started",
    ]


def test_legado_billing_responde_503_nao_configurado(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    monkeypatch.delenv("PROFILE", raising=False)
    monkeypatch.setenv("DB_URL", "postgresql+psycopg://u:p@localhost/x")
    with (
        patch("central_api.deps.create_engine"),
        patch("central_api.deps.install_rls_listener"),
        patch("central_api.deps.instrument_engine"),
        patch("central_api.deps.install_query_counter"),
        patch("central_api.deps._lease_reaper_loop", new=Mock(side_effect=lambda e: _idle())),
        TestClient(_make_app()) as client,
    ):
        status = client.get("/api/v1/billing/status?billing_account_id=x")
        webhook = client.post("/api/v1/billing/webhooks/stripe", content=b"{}")

    assert (status.status_code, status.json()) == (503, {"detail": "billing_not_configured"})
    assert (webhook.status_code, webhook.json()) == (
        503, {"detail": "billing_not_configured"},
    )


async def _idle() -> None:
    return None


def test_install_billing_local_disabled_nao_toca_secrets_manager(monkeypatch) -> None:
    from central_api import billing_deps
    from central_api.routes import billing, stripe_webhook
    from cnes_domain.profiles import BillingMode

    monkeypatch.setenv("PROFILE", "local")
    monkeypatch.setenv("TENANT_ID", AWS_TENANT)
    monkeypatch.setenv("BILLING_MODE", "disabled")
    session = _session_spy()
    app = _make_app()

    billing_deps.install_billing(app, Mock(), session)

    assert _secretsmanager_calls(session) == []
    assert app.dependency_overrides[billing.get_billing_mode]() is BillingMode.DISABLED
    assert stripe_webhook.get_stripe_webhook_verifier in app.dependency_overrides


def test_build_local_state_usa_mesma_session_no_runtime_e_no_billing(
    tmp_path, monkeypatch,
) -> None:
    from central_api import deps

    monkeypatch.setenv("PROFILE", "local")
    monkeypatch.setenv("TENANT_ID", AWS_TENANT)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    session = _session_spy()
    with (
        patch("central_api.deps.Session", return_value=session),
        patch("central_api.deps.build_runtime") as build,
        patch("central_api.deps.install_billing") as install,
        patch("central_api.deps._install_local_auth_and_serving"),
        patch("central_api.deps._install_edge_overrides"),
    ):
        deps._build_local_state(_make_app())

    assert build.call_args.args[2] is session
    assert install.call_args.args[2] is session
    assert install.call_args.args[1] is build.return_value


ADMIN_TOKEN = "admin-token-de-teste"  # noqa: S105
ENQUEUE_BODY = {"tenant_id": AWS_TENANT, "agent_id": "agent-1", "competencia": "2026-09"}


def _components_patch(**replacements):
    from cnes_infra.billing import build_stripe_billing as real

    def _build(*args, **kwargs):
        return dataclasses.replace(real(*args, **kwargs), **replacements)

    return patch("cnes_infra.billing.build_stripe_billing", side_effect=_build)


def _owned_catalog() -> Mock:
    from datetime import UTC, datetime

    from cnes_domain.billing.models import BillingAccount, BillingAccountStatus

    moment = datetime(2026, 9, 1, tzinfo=UTC)
    catalog = Mock(name="catalog")
    catalog.get_account.return_value = BillingAccount(
        "ba_x", "cus_x", "user-1", BillingAccountStatus.ACTIVE, moment, moment,
    )
    return catalog


def _signed_event(event_id: str) -> tuple[bytes, str]:
    payload = json.dumps({
        "id": event_id, "object": "event", "type": "customer.subscription.updated",
        "created": int(time.time()), "livemode": False,
        "data": {"object": {"id": "sub_x", "object": "subscription", "customer": "cus_x"}},
    }).encode()
    timestamp = str(int(time.time()))
    digest = hmac.new(
        SECRET_WEBHOOK.encode(), f"{timestamp}.".encode() + payload, hashlib.sha256,
    ).hexdigest()
    return payload, f"t={timestamp},v1={digest}"


def test_aws_stripe_nao_habilita_rota_admin_raw_jobs_sem_control_plane(monkeypatch) -> None:
    from cnes_infra import config

    monkeypatch.setattr(config, "ADMIN_TOKEN", ADMIN_TOKEN, raising=False)
    _, (build, verifier, sess, client) = _stripe_client(monkeypatch)
    with build as built, verifier, sess, client:
        response = client.post(
            "/api/v1/admin/raw-jobs/enqueue", json=ENQUEUE_BODY,
            headers={**_bearer(), "X-Admin-Token": ADMIN_TOKEN, "Idempotency-Key": KEY},
        )

    assert (response.status_code, response.json()) == (
        503, {"detail": "control_plane_not_configured"},
    )
    built.return_value.control_plane.assert_not_called()


def test_aws_stripe_status_alcanca_projecao_com_catalogo_e_projecao_compostos(
    monkeypatch,
) -> None:
    projection = Mock(name="projection")
    projection.get_snapshot.return_value = None
    _, (build, verifier, sess, client) = _stripe_client(monkeypatch)
    components = _components_patch(catalog=_owned_catalog(), projection=projection)
    with build, verifier, sess, components, client:
        response = client.get(
            "/api/v1/billing/status?billing_account_id=ba_x", headers=_bearer(),
        )

    assert response.status_code == 200
    assert response.json() == {"state": "pending", "billing_account_id": "ba_x"}
    projection.get_snapshot.assert_called_once()


def test_aws_stripe_webhook_assinado_e_aceito_no_inbox(monkeypatch) -> None:
    from cnes_domain.billing.inbox import InboxAcceptResult, InboxDisposition

    inbox = Mock(name="inbox")
    inbox.accept.return_value = InboxAcceptResult("evt_1", InboxDisposition.ACCEPTED)
    payload, signature = _signed_event("evt_1")
    _, (build, verifier, sess, client) = _stripe_client(monkeypatch)
    with build, verifier, sess, _components_patch(inbox=inbox), client:
        response = client.post(
            "/api/v1/billing/webhooks/stripe", content=payload,
            headers={"Stripe-Signature": signature},
        )

    assert (response.status_code, response.json()) == (200, {"received": True})
    inbox.accept.assert_called_once()
    assert inbox.accept.call_args.args[0].event_id == "evt_1"
