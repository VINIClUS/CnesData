"""Stack de API com gates de billing sobre control plane SQLite/DynamoDB reais."""

from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from moto import mock_aws

from central_api.auth.aws_oidc import MembershipAuthorizer
from central_api.composition import api_billing_gates, entitled_serving_access
from central_api.routes import billing, billing_admin, raw_jobs, serving, tenants
from central_api.schemas.raw_api import EdgeIdentity
from central_api.services.agent_admission import AgentAdmission
from central_api.services.billing_gates import ApiBillingGates
from central_api.serving.aws_signed import S3SignedServingAccess, SignedServingSettings
from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_domain.billing.models import (
    CapacityReservation,
    EntitlementSnapshot,
    QuotaLimits,
    ReadConsistency,
)
from cnes_domain.billing.revocation import (
    ImmediateRevocationService,
    RevocationDependencies,
    RevocationSettings,
)
from cnes_domain.control_plane.entities import DatasetPointer, DatasetVersion
from cnes_infra.auth.oidc import OidcPrincipal
from cnes_infra.billing.audit_outbox import DynamoBillingAudit
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.dynamodb_quota_items import decode_capacity_reservation
from cnes_infra.billing.dynamodb_revocation import DynamoRevocationStore
from cnes_infra.billing.keys import capacity_usage_key
from cnes_infra.billing.wiring import BillingGateResources
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import encode_model
from cnes_infra.control_plane.dynamodb_keys import pointer_key, version_key
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.object_store.filesystem import FilesystemObjectStore
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_create_command,
    make_snapshot,
    put_tenant,
    table_items,
)
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    TENANT,
    make_limits,
    seed_snapshot,
)
from packages.cnes_infra.tests.billing.revocation_support import get_raw
from packages.cnes_infra.tests.contracts.clock import MutableClock
from tests.integration.billing._enforcement_stack import RawView
from tests.integration.billing._execution_stack import Case

if TYPE_CHECKING:
    from packages.cnes_infra.tests.billing.revocation_support import RevEnv

OWNER = "user-owner"
MANAGER = "manager-1"
VIEWER = "viewer-1"
FINGERPRINT = "a" * 64
ROTATED_FINGERPRINT = "b" * 64
SERVING_FEATURES = frozenset({"serving_history"})
COMPETENCIA = "2026-07"
DOCUMENT = b'{"documento": "overview"}'
SERVING_URL = "/api/v1/dashboard/serving/{dataset}/overview"
REVOKE_URL = f"/api/v1/admin/billing/{ACCOUNT}/revoke"
TENANTS_URL = f"/api/v1/billing/accounts/{ACCOUNT}/tenants"
NEXT_JOB_URL = "/api/v1/edge/jobs/next"
ISSUER = "https://issuer"
_ENTITY = "entity"


class FaultyClient:
    """Cliente DynamoDB do control plane que falha só na transação de criação de tenant."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.fail_tenant_creation = False
        self.failures = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def transact_write_items(self, **request: Any) -> Any:
        if self.fail_tenant_creation and _creates_tenant(request["TransactItems"]):
            self.failures += 1
            raise ClientError(
                {"Error": {"Code": "InternalServerError", "Message": "boom"}},
                "TransactWriteItems",
            )
        return self._inner.transact_write_items(**request)


def _creates_tenant(actions: list[dict[str, Any]]) -> bool:
    return any(
        action.get("Put", {}).get("Item", {}).get(_ENTITY, {}).get("S") == "TENANT"
        for action in actions
    )


class InterceptingCapacity:
    """Porta de capacidade que executa um gancho antes da primeira reserva."""

    def __init__(self, inner: Any, before_first_reserve: Callable[[], None]) -> None:
        self._inner = inner
        self._hook: Callable[[], None] | None = before_first_reserve

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def reserve_capacity(self, command: Any) -> Any:
        hook, self._hook = self._hook, None
        if hook is not None:
            hook()
        return self._inner.reserve_capacity(command)


@dataclass
class ApiStack:
    case: Case
    plane: Any
    clock: MutableClock
    client: Any
    faulty: FaultyClient | None
    gates: ApiBillingGates
    executor: Mock
    store: FilesystemObjectStore
    signer: Any


def edge_headers(agent_id: str, fingerprint: str = FINGERPRINT, tenant: str = TENANT) -> dict:
    return {
        "X-Test-Tenant": tenant, "X-Test-Agent": agent_id, "X-Test-Fingerprint": fingerprint,
    }


def user_headers(subject: str, tenant: str | None = None) -> dict[str, str]:
    headers = {"X-Test-Subject": subject}
    if tenant is not None:
        headers["X-Tenant-Id"] = tenant
    return headers


def _build_plane(case: Case, clock: MutableClock, tmp_path: Path) -> tuple[Any, Any, Any]:
    if not case.dynamo:
        plane = SQLiteControlPlane(tmp_path / "cp.db", clock.now)
        plane.initialize()
        return plane, None, None
    client = boto3.client("dynamodb", region_name="us-east-1")
    create_table(client)
    faulty = FaultyClient(client)
    plane = DynamoDBControlPlane(faulty, TABLE_NAME, clock.now, billing=case.settings)
    return plane, client, faulty


def _seed_stripe(client: Any, clock: MutableClock, snapshot: EntitlementSnapshot) -> None:
    seed_snapshot(client, snapshot)
    put_tenant(client, TENANT)
    DynamoBillingCatalog(client, TABLE_NAME, clock.now).create_account(
        make_create_command(ACCOUNT, TENANT)
    )


def default_snapshot(**limits: int | None) -> EntitlementSnapshot:
    quotas: QuotaLimits = make_limits(**limits)
    return make_snapshot(ACCOUNT, quotas=quotas, features=SERVING_FEATURES)


@contextmanager
def open_api_stack(
    case: Case, tmp_path: Path, snapshot: EntitlementSnapshot | None = None,
) -> Iterator[ApiStack]:
    clock = MutableClock(NOW)
    with ExitStack() as exits:
        if case.dynamo:
            exits.enter_context(mock_aws())
        plane, client, faulty = _build_plane(case, clock, tmp_path)
        if case.stripe:
            _seed_stripe(client, clock, snapshot or default_snapshot())
        resources = BillingGateResources(
            clock.now, 4, client, TABLE_NAME if case.dynamo else None,
        )
        gates = api_billing_gates(case.settings, resources)
        signer = boto3.client("s3", region_name="us-east-1") if case.dynamo else None
        store = FilesystemObjectStore(tmp_path / "objects")
        yield ApiStack(case, plane, clock, client, faulty, gates, Mock(), store, signer)


def _revocation_service(stack: ApiStack) -> ImmediateRevocationService:
    client, clock = stack.client, stack.clock.now
    return ImmediateRevocationService(
        RevocationDependencies(
            DynamoEntitlementProjection(client, TABLE_NAME, clock),
            DynamoRevocationStore(client, TABLE_NAME, clock),
            stack.executor,
            DynamoBillingAudit(client, TABLE_NAME),
            clock,
        ),
        RevocationSettings(),
    )


class _NoCandidates:
    def list_candidates(self, user_id: str) -> tuple[str, ...]:
        return ()


def _identity(request: Request) -> EdgeIdentity:
    headers = request.headers
    return EdgeIdentity(
        tenant_id=headers["X-Test-Tenant"], agent_id=headers["X-Test-Agent"],
        certificate_fingerprint=headers["X-Test-Fingerprint"],
    )


def _principal(request: Request) -> OidcPrincipal:
    return OidcPrincipal(ISSUER, request.headers.get("X-Test-Subject", OWNER), None, None)


def _serving_principal(request: Request) -> serving.ServingPrincipal:
    return serving.ServingPrincipal(TENANT, request.headers.get("X-Test-Subject", VIEWER))


def _billing_overrides(stack: ApiStack) -> dict[Callable[..., Any], Callable[..., Any]]:
    catalog = DynamoBillingCatalog(stack.client, TABLE_NAME, stack.clock.now)
    authorizer = MembershipAuthorizer(stack.plane, _NoCandidates())
    return {
        billing.get_billing_mode: lambda: stack.case.settings.mode,
        billing.get_billing_principal: _principal,
        billing.get_membership_authorizer: lambda: authorizer,
        billing.get_billing_catalog: lambda: catalog,
        billing.get_billing_clock: lambda: stack.clock.now,
        tenants.get_tenant_gates: lambda: stack.gates,
        billing_admin.get_revocation_service: lambda: _revocation_service(stack),
    }


def _serving_delivery(stack: ApiStack, redirect: bool) -> serving.ServingDelivery:
    access = entitled_serving_access(stack.plane, stack.store, stack.gates)
    if not redirect:
        return serving.get_serving_delivery(access, stack.store)
    signed = S3SignedServingAccess(
        access, stack.store, stack.signer, SignedServingSettings("bucket", 300),
    )
    return serving.signed_serving_delivery(signed, stack.clock.now)


def build_client(stack: ApiStack, redirect: bool = False) -> TestClient:
    app = FastAPI()
    for module in (raw_jobs, tenants, billing_admin, serving):
        app.include_router(module.router)
    overrides: dict[Callable[..., Any], Callable[..., Any]] = {
        raw_jobs.get_control_plane: lambda: stack.plane,
        raw_jobs.get_edge_identity: _identity,
        raw_jobs.get_agent_admission: lambda: AgentAdmission(stack.plane, stack.gates),
        serving.get_serving_principal: _serving_principal,
    }
    if stack.case.stripe:
        overrides |= _billing_overrides(stack)
        delivery = _serving_delivery(stack, redirect)
        overrides[serving.get_serving_delivery] = lambda: delivery
    app.dependency_overrides.update(overrides)
    return TestClient(app, follow_redirects=False)


def with_capacity_hook(stack: ApiStack, hook: Callable[[], None]) -> None:
    stack.gates = replace(stack.gates, capacity=InterceptingCapacity(stack.gates.capacity, hook))


def create_tenant(client: TestClient, tenant_id: str, key: str = "key-1") -> Any:
    body = {"tenant_id": tenant_id, "municipality_name": "Municipio", "idempotency_key": key}
    return client.post(TENANTS_URL, json=body, headers=user_headers(OWNER))


def capacity_counter(stack: ApiStack, name: str) -> int:
    item = get_raw(cast("RevEnv", RawView(stack.client, TABLE_NAME)), capacity_usage_key(ACCOUNT))
    return 0 if item is None else int(item.get(name, {"N": "0"})["N"])


def capacity_reservations(stack: ApiStack) -> list[CapacityReservation]:
    items = [i for i in table_items(stack.client) if i["sk"]["S"].startswith("CAPACITY_RESERV")]
    return [decode_capacity_reservation(item)[0] for item in items]


def stored_keys(stack: ApiStack) -> set[tuple[str, str]]:
    return {(item["pk"]["S"], item["sk"]["S"]) for item in table_items(stack.client)}


def read_snapshot(stack: ApiStack) -> EntitlementSnapshot:
    projection = DynamoEntitlementProjection(stack.client, TABLE_NAME, stack.clock.now)
    return cast("EntitlementSnapshot", projection.get_snapshot(ACCOUNT, ReadConsistency.STRONG))


def _put_object(store: FilesystemObjectStore, key: str, body: bytes) -> None:
    store.put(key, BytesIO(body), sha256(body).hexdigest())


def _run_manifest(dataset: str, run_id: str) -> RunManifest:
    output = OutputManifest(
        manifest_version=1, manifest_id="serving-overview", tenant_id=TENANT, layer="serving",
        source_type=None, competencia=COMPETENCIA, run_id=run_id, unit_id="unit-1", attempt=1,
        schema_version="cnes-serving-v1", object_key=f"serving/{TENANT}/{run_id}/overview.json",
        object_sha256=sha256(DOCUMENT).hexdigest(), row_count=1, created_at=NOW,
    )
    return RunManifest(
        manifest_version=1, tenant_id=TENANT, dataset_name=dataset, run_id=run_id,
        competencia=COMPETENCIA, outputs=(output,), missing_sources=(), published_at=NOW,
    )


def seed_dataset(stack: ApiStack, dataset: str, created_at: datetime) -> None:
    run_id = f"run-{dataset}"
    manifest = _run_manifest(dataset, run_id)
    manifest_key = f"reconciliation/{TENANT}/{COMPETENCIA}/{run_id}/run-manifest.json"
    _put_object(stack.store, manifest.outputs[0].object_key, DOCUMENT)
    _put_object(stack.store, manifest_key, manifest.model_dump_json().encode())
    version = DatasetVersion(
        tenant_id=TENANT, dataset_name=dataset, version_id=run_id, run_id=run_id,
        run_manifest_key=manifest_key, created_at=created_at,
    )
    pointer = DatasetPointer(
        tenant_id=TENANT, dataset_name=dataset, pointer_name="current", version_id=run_id,
        updated_at=created_at,
    )
    keys = version_key(TENANT, dataset, run_id), pointer_key(TENANT, dataset, "current")
    for model, entity, key in ((version, "DATASETVERSION", keys[0]),
                               (pointer, "DATASETPOINTER", keys[1])):
        stack.client.put_item(TableName=TABLE_NAME, Item=encode_model(model, entity, key))


def utc_now() -> datetime:
    return datetime.now(UTC)
