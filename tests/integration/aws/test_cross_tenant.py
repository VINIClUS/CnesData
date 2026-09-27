"""Negação cross-tenant do serving AWS composto: membership antes de qualquer assinatura."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from central_api.serving.aws_signed import ServingKeyForbidden
from tests.integration.aws._harness import publish_version, seed_membership, serving_request

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from httpx import Response

    from tests.integration.aws._harness import AwsTestRuntime

pytestmark = [pytest.mark.dynamodb_local, pytest.mark.s3_integration]

_OVERVIEW = "/api/v1/dashboard/serving/cnes/overview"
_FORBIDDEN_NAMES = (
    "../../raw/data.parquet",
    "../../normalized/data.parquet",
    "../../reconciliation/data.parquet",
    "../../audit/event.json",
)


def _get_overview(client: TestClient, user_id: str, tenant_id: str) -> Response:
    return client.get(
        _OVERVIEW,
        headers={"Authorization": f"Bearer {user_id}", "X-Tenant-Id": tenant_id},
        follow_redirects=False,
    )


def test_tenant_b_nao_le_pointer_ou_serving_do_tenant_a(
    aws_runtime: AwsTestRuntime, serving_client: TestClient,
) -> None:
    seed_membership(aws_runtime, "tenant-a", "user-a")
    seed_membership(aws_runtime, "tenant-b", "user-b")
    publish_version(aws_runtime, "run-a", tenant_id="tenant-a")

    denied = _get_overview(serving_client, "user-b", "tenant-a")

    assert (denied.status_code, denied.json()) == (403, {"detail": "tenant_not_allowed"})
    assert aws_runtime.s3.presigned == []
    granted = _get_overview(serving_client, "user-a", "tenant-a")
    assert (granted.status_code, granted.headers["x-dataset-version"]) == (307, "run-a")
    assert [call["Params"]["Key"] for call in aws_runtime.s3.presigned] == [
        "serving/tenant-a/run-a/overview.json",
    ]


def test_request_nao_assina_raw_normalized_reconciliation_ou_audit(
    aws_runtime: AwsTestRuntime,
) -> None:
    seed_membership(aws_runtime, "tenant-a", "user-a")
    publish_version(aws_runtime, "run-a", tenant_id="tenant-a")
    access = aws_runtime.api.services.serving_access

    for relative_name in _FORBIDDEN_NAMES:
        request = serving_request("user-a", "tenant-a", relative_name)
        with pytest.raises(ServingKeyForbidden, match="serving_key_forbidden"):
            access.grant(request, aws_runtime.clock.now())

    assert aws_runtime.s3.presigned == []
