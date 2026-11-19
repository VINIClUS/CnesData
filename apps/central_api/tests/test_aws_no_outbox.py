"""API AWS atende sem permissões de auditoria nem despacho de outbox."""

from __future__ import annotations

import asyncio
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from contextvars import copy_context
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import boto3
import httpx
import pytest
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from jose import jwt
from moto import mock_aws

from apps.central_api.tests.routes.test_pointer_only_cutover import (
    NOW,
    TENANT,
    USER,
    Publication,
    _publish_command,
    _publishing_run,
    _stage_objects,
)
from apps.central_api.tests.test_aws_composition import (
    _aws_values,
    _make_app,
    _step_functions_client,
)
from apps.central_api.tests.test_tenant_discovery import _keys
from central_api import deps
from central_api.auth import MembershipAuthorizer
from central_api.composition import AwsApiServices, build_runtime
from central_api.services.raw_ingestion import RawIngestionService
from central_api.services.run_planning import RunPlanningService
from central_api.serving import S3SignedServingAccess
from cnes_domain import outbox_dispatcher
from cnes_domain.control_plane.entities import Membership
from cnes_domain.orchestration.source_catalog import SourceCatalog
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.executor.step_functions import StepFunctionsExecutor
from cnes_infra.object_store import S3ObjectStore
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

ROUTE = "/api/v1/dashboard/serving/cnes/overview"
DENIED = ("GetObjectLockConfiguration", "PutObject", "PutObjectRetention")
RUN = "api-current"


@pytest.fixture(autouse=True)
def _isolar_rede(monkeypatch: pytest.MonkeyPatch) -> None:
    def negar(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("network=forbidden")

    for name in ("create_connection", "getaddrinfo", "gethostbyname", "gethostbyname_ex"):
        monkeypatch.setattr(socket, name, negar)
    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, negar)


@dataclass
class AwsBoundary:
    session: Any
    values: dict[str, str]
    calls: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)
    clients: list[str] = field(default_factory=list)
    deny_audit: bool = True

    def client(self, service: str, **request: Any) -> Any:
        self.clients.append(service)
        return self.session.client(service, **request)

    def forbidden(self) -> list[str]:
        return [
            operation
            for service, operation, _ in self.calls
            if service == "s3" and operation in DENIED
        ]

    def outbox(self) -> list[tuple[str, str, dict[str, Any]]]:
        return [
            call
            for call in self.calls
            if call[0] == "dynamodb" and (call[1] != "GetItem" or "OUTBOX" in str(call[2]))
        ]


def _seed(boundary: AwsBoundary) -> None:
    ddb = boundary.session.client("dynamodb", region_name="us-east-1")
    s3 = boundary.session.client("s3", region_name="us-east-1")
    _create_table(ddb)
    s3.create_bucket(Bucket=boundary.values["AWS_DATA_BUCKET"])
    s3.create_bucket(Bucket=boundary.values["AWS_AUDIT_BUCKET"], ObjectLockEnabledForBucket=True)
    cp = DynamoDBControlPlane(ddb, _TABLE_NAME, lambda: NOW)
    cp.put_membership(
        Membership(
            tenant_id=TENANT,
            user_id=USER,
            role="gestor",
            created_at=NOW,
            oidc_issuer=boundary.values["OIDC_ISSUER"],
        )
    )
    publication = Publication("cnes", RUN, expected=None)
    _stage_objects(S3ObjectStore(s3, boundary.values["AWS_DATA_BUCKET"]), publication)
    cp.put_run(_publishing_run(publication))
    cp.publish_dataset(_publish_command(publication))


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch) -> Iterator[AwsBoundary]:
    values = _aws_values() | {"AWS_CONTROL_PLANE_TABLE": _TABLE_NAME, "BILLING_MODE": "disabled"}
    env = values | {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_EC2_METADATA_DISABLED": "true",
        "DB_URL": "sqlite+pysqlite:///:memory:",
    }
    with patch.dict("os.environ", env, clear=True), mock_aws():
        boundary = AwsBoundary(boto3.Session(), values)
        _seed(boundary)
        original = cast("Any", BaseClient)._make_api_call

        def call(client: Any, operation: str, request: dict[str, Any]) -> Any:
            service = client.meta.service_model.service_name
            boundary.calls.append((service, operation, dict(request)))
            if service == "s3" and boundary.deny_audit and operation in DENIED:
                raise ClientError({"Error": {"Code": "AccessDenied"}}, operation)
            if service == "stepfunctions" and operation == "DescribeStateMachine":
                return _step_functions_client().describe_state_machine(**request)
            return original(client, operation, request)

        monkeypatch.setattr(BaseClient, "_make_api_call", call)
        monkeypatch.setattr(deps, "Session", lambda **_: boundary)
        monkeypatch.setattr(deps, "_OUTBOX_INTERVAL", 0, raising=False)
        yield boundary


@dataclass
class OidcBoundary:
    pem: str
    jwk: dict[str, Any]
    issuer: str
    clients: list[httpx.Client] = field(default_factory=list)
    closes: list[httpx.Client] = field(default_factory=list)
    requests: list[str] = field(default_factory=list)

    def respond(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        if str(request.url) == f"{self.issuer}/.well-known/openid-configuration":
            body = {"issuer": self.issuer, "jwks_uri": f"{self.issuer}/keys"}
        else:
            assert str(request.url) == f"{self.issuer}/keys"
            body = {"keys": [self.jwk]}
        return httpx.Response(200, json=body, request=request)

    def headers(self, audience: str = "cnesdata-dashboard") -> dict[str, str]:
        now = int(time.time())
        token = jwt.encode(
            {"iss": self.issuer, "sub": USER, "aud": audience, "iat": now, "exp": now + 300},
            self.pem,
            algorithm="RS256",
            headers={"kid": "test"},
        )
        return {"Authorization": f"Bearer {token}", "X-Tenant-Id": TENANT}


@pytest.fixture
def oidc(aws: AwsBoundary, monkeypatch: pytest.MonkeyPatch) -> OidcBoundary:
    boundary = OidcBoundary(*_keys(), aws.values["OIDC_ISSUER"])
    initialize, close = httpx.Client.__init__, httpx.Client.close

    def construct(client: httpx.Client, *args: Any, **kwargs: Any) -> None:
        initialize(client, *args, **kwargs)
        if type(client) is httpx.Client:
            boundary.clients.append(client)

    def terminate(client: httpx.Client) -> None:
        if type(client) is httpx.Client:
            boundary.closes.append(client)
        close(client)

    monkeypatch.setattr(httpx.Client, "__init__", construct)
    monkeypatch.setattr(httpx.Client, "close", terminate)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", lambda _, req: boundary.respond(req))
    return boundary


@dataclass
class SchedulerProbe:
    dispatches: list[bool] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    workers: list[str] = field(default_factory=list)
    previous_factory: Any = None
    previous_handler: Any = None

    async def install(self) -> None:
        loop = asyncio.get_running_loop()
        self.previous_factory = loop.get_task_factory()
        self.previous_handler = loop.get_exception_handler()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        loop.set_task_factory(self._create_task)
        loop.set_exception_handler(self._record_error)

    def _create_task(self, loop: Any, coroutine: Any, **kwargs: Any) -> Any:
        factory = self.previous_factory
        task = (
            factory(loop, coroutine, **kwargs)
            if factory is not None
            else asyncio.Task(
                coroutine,
                loop=loop,
                **kwargs,
            )
        )
        task.add_done_callback(self._completed)
        return task

    def _completed(self, task: Any) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            self._record_error(
                task.get_loop(),
                {
                    "message": "background_task_failed",
                    "exception": error,
                    "task": task,
                },
            )

    def _record_error(self, loop: Any, context: dict[str, Any]) -> None:
        self.errors.append(context)
        if self.previous_handler is None:
            loop.default_exception_handler(context)
        else:
            self.previous_handler(loop, context)

    async def restore(self) -> None:
        loop = asyncio.get_running_loop()
        loop.set_task_factory(self.previous_factory)
        loop.set_exception_handler(self.previous_handler)

    async def barrier(self) -> None:
        async def execute() -> None:
            for _ in range(4):
                await asyncio.sleep(0)
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    self.workers.append,
                    "executed",
                )

        await asyncio.wait_for(execute(), timeout=5)


@pytest.fixture
def scheduler(monkeypatch: pytest.MonkeyPatch, oidc: OidcBoundary) -> SchedulerProbe:
    probe = SchedulerProbe()
    dispatch = outbox_dispatcher.dispatch_once

    def record(*args: Any, **kwargs: Any) -> Any:
        probe.dispatches.append(any(client.is_closed for client in oidc.clients))
        return dispatch(*args, **kwargs)

    monkeypatch.setattr(outbox_dispatcher, "dispatch_once", record)
    if hasattr(deps, "dispatch_once"):
        monkeypatch.setattr(deps, "dispatch_once", record)
    return probe


def assert_quiet(aws: AwsBoundary, scheduler: SchedulerProbe, caplog: Any) -> None:
    assert scheduler.workers, "scheduler=unreached"
    observed = {
        "dispatches": len(scheduler.dispatches),
        "outbox": len(aws.outbox()),
        "audit": len(aws.forbidden()),
        "task_errors": len(scheduler.errors),
        "worker_errors": len([record for record in caplog.records if record.exc_info]),
    }
    assert observed == dict.fromkeys(observed, 0), observed


def assert_closed(oidc: OidcBoundary) -> None:
    assert len(oidc.clients) == 1
    assert oidc.closes == oidc.clients
    assert oidc.clients[0].is_closed


def observed_app(scheduler: SchedulerProbe) -> Any:
    app = _make_app()
    lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def observe(app: Any) -> AsyncIterator[None]:
        await scheduler.install()
        try:
            async with lifespan(app):
                await scheduler.barrier()
                yield
                await scheduler.barrier()
            await scheduler.barrier()
        finally:
            await scheduler.restore()

    app.router.lifespan_context = observe
    return app


@pytest.mark.parametrize(
    ("operation", "method", "sdk_request"),
    [
        ("GetObjectLockConfiguration", "get_object_lock_configuration", {}),
        ("PutObject", "put_object", {"Key": "audit/control", "Body": b"denied"}),
        (
            "PutObjectRetention",
            "put_object_retention",
            {
                "Key": "audit/control",
                "Retention": {
                    "Mode": "COMPLIANCE",
                    "RetainUntilDate": NOW + timedelta(days=365),
                },
            },
        ),
    ],
    ids=DENIED,
)
def test_fronteira_nega_cada_operacao_de_auditoria(
    aws: AwsBoundary,
    operation: str,
    method: str,
    sdk_request: dict[str, Any],
) -> None:
    s3 = aws.session.client("s3", region_name="us-east-1")

    with pytest.raises(ClientError) as caught:
        getattr(s3, method)(Bucket=aws.values["AWS_AUDIT_BUCKET"], **sdk_request)

    assert caught.value.response.get("Error", {}).get("Code") == "AccessDenied"
    assert caught.value.operation_name == operation
    assert aws.forbidden() == [operation]


@pytest.mark.asyncio
async def test_barreira_executa_worker_real_limitado() -> None:
    scheduler = SchedulerProbe()
    await scheduler.install()
    try:
        await scheduler.barrier()
        assert scheduler.workers == ["executed"] * 4
        assert scheduler.errors == []
    finally:
        await scheduler.restore()


def test_composicao_aws_sem_permissao_audit_preserva_grafo(aws: AwsBoundary) -> None:
    runtime = build_runtime("aws", aws.values, cast("Any", aws))

    assert runtime.audit_sink is None
    assert aws.forbidden() == []
    assert aws.clients == ["dynamodb", "s3", "stepfunctions"]
    assert isinstance(runtime.control_plane, DynamoDBControlPlane)
    assert isinstance(runtime.object_store, S3ObjectStore)
    assert isinstance(runtime.executor, StepFunctionsExecutor)
    assert isinstance(runtime.raw_ingestion, RawIngestionService)
    assert isinstance(runtime.source_catalog, SourceCatalog)
    assert isinstance(runtime.run_planning, RunPlanningService)
    assert isinstance(runtime.services, AwsApiServices)
    assert isinstance(runtime.services.membership_authorizer, MembershipAuthorizer)
    assert isinstance(runtime.services.serving_access, S3SignedServingAccess)
    assert [op for service, op, _ in aws.calls if service == "stepfunctions"] == [
        "DescribeStateMachine",
    ]


def _check_routes(client: TestClient, oidc: OidcBoundary) -> None:
    assert client.get("/api/v1/system/health").status_code == 200
    headers = oidc.headers()
    response = client.get(ROUTE, headers=headers)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    assert "location" not in response.headers
    assert set(response.json()) == {"url", "version_id", "expires_in"}
    assert (response.json()["version_id"], response.json()["expires_in"]) == (RUN, 300)
    url = urlsplit(response.json()["url"])
    assert url.path == f"/serving/{TENANT}/{RUN}/overview.json"
    assert parse_qs(url.query)["X-Amz-Expires"] == ["300"]
    for bad in (headers | {"Authorization": "Bearer invalid"}, oidc.headers("wrong-audience")):
        denied = client.get(ROUTE, headers=bad)
        assert (denied.status_code, denied.json()) == (401, {"detail": "token_invalid"})
    denied = client.get(ROUTE, headers=headers | {"X-Tenant-Id": "other-tenant"})
    assert (denied.status_code, denied.json()) == (403, {"detail": "tenant_not_allowed"})
    missing = client.get(ROUTE.replace("cnes", "sihd"), headers=headers)
    assert (missing.status_code, missing.json()) == (503, {"detail": "active_serving_unavailable"})
    disabled = client.post("/api/v1/billing/checkout", json={})
    assert (disabled.status_code, disabled.json()) == (404, {"detail": "billing_disabled"})
    assert oidc.requests == [
        f"{oidc.issuer}/.well-known/openid-configuration",
        f"{oidc.issuer}/keys",
    ]


def test_lifespan_aws_atende_sem_audit_e_fecha_oidc(
    aws: AwsBoundary,
    oidc: OidcBoundary,
    scheduler: SchedulerProbe,
    caplog: Any,
) -> None:
    app = observed_app(scheduler)
    with (
        patch.object(deps, "get_engine", side_effect=AssertionError("postgres=forbidden")),
        patch.object(deps, "create_engine", side_effect=AssertionError("postgres=forbidden")),
        TestClient(app) as client,
    ):
        assert client.portal is not None
        client.portal.call(scheduler.barrier)
        _check_routes(client, oidc)
        client.portal.call(scheduler.barrier)
    assert_closed(oidc)
    assert_quiet(aws, scheduler, caplog)


def test_composicao_local_preserva_sink_real(tmp_path: Any) -> None:
    runtime = build_runtime(
        "local",
        {"PROFILE": "local", "TENANT_ID": TENANT, "DATA_DIR": str(tmp_path)},
        cast("Any", object()),
    )

    assert isinstance(runtime.audit_sink, LocalAuditSink)
    assert runtime.control_plane.get_tenant(TENANT) is not None


@pytest.mark.asyncio
async def test_oraculo_recusa_task_retida_que_falha_antes_de_dispatch(caplog: Any) -> None:
    loop = asyncio.get_running_loop()
    original_factory, original_handler = loop.get_task_factory(), loop.get_exception_handler()
    created: list[tuple[Any, dict[str, Any]]] = []
    forwarded: list[tuple[Any, dict[str, Any]]] = []

    def factory(loop: Any, coroutine: Any, **kwargs: Any) -> Any:
        task = asyncio.Task(coroutine, loop=loop, **kwargs)
        created.append((task, kwargs))
        return task

    def handler(loop: Any, context: dict[str, Any]) -> None:
        forwarded.append((loop, context))

    async def failed_worker() -> None:
        raise RuntimeError("control=worker_failed_before_dispatch")

    probe = SchedulerProbe()
    context = copy_context()
    loop.set_task_factory(factory)
    loop.set_exception_handler(handler)
    try:
        await probe.install()
        retained = asyncio.create_task(failed_worker(), context=context)
        canceled = asyncio.create_task(asyncio.sleep(0))
        canceled.cancel()
        await probe.barrier()
        assert retained.done()
        assert not retained.cancelled()
        with pytest.raises(AssertionError, match="task_errors"):
            assert_quiet(AwsBoundary(None, {}), probe, caplog)
        assert [error["exception"] for error in probe.errors] == [retained.exception()]
        assert forwarded == [(loop, probe.errors[0])]
        assert any(task is retained and kwargs["context"] is context for task, kwargs in created)
        assert canceled.cancelled()
        await probe.restore()
        assert loop.get_task_factory() is factory
        assert loop.get_exception_handler() is handler
    finally:
        loop.set_task_factory(original_factory)
        loop.set_exception_handler(original_handler)
