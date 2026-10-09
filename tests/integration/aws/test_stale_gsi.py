"""Candidato stale do GSI é revalidado na chave base do DynamoDB Local antes de autorizar."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from central_api.auth.aws_oidc import MembershipAuthorizer, TenantAccessDenied
from cnes_domain.control_plane.commands import ClaimJob
from cnes_domain.control_plane.enums import JobState
from cnes_infra.auth.dynamodb_memberships import DynamoDBMembershipCandidates
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import entity_key
from tests.integration.aws._doubles import QueryRecorder, StaleQueryClient
from tests.integration.aws._harness import (
    NACIONAL,
    TENANT,
    accept_raw_job,
    create_raw_job,
    delete_membership,
    principal,
    seed_membership,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from tests.integration.aws._harness import AwsTestRuntime

pytestmark = [pytest.mark.dynamodb_local, pytest.mark.s3_integration]


def test_gsi_membership_revogada_nao_autoriza(aws_runtime: AwsTestRuntime) -> None:
    table = aws_runtime.resources.table_name
    seed_membership(aws_runtime, "tenant-a", "user-1")
    recorder = QueryRecorder(aws_runtime.dynamodb)
    candidates = DynamoDBMembershipCandidates(cast("BaseClient", recorder), table)
    assert candidates.list_candidates("user-1") == ("tenant-a",)
    delete_membership(aws_runtime, "tenant-a", "user-1")
    stale = StaleQueryClient(aws_runtime.dynamodb, recorder.pages)
    authorizer = MembershipAuthorizer(
        aws_runtime.api.control_plane,
        DynamoDBMembershipCandidates(cast("BaseClient", stale), table),
    )

    assert authorizer.list_authorized(principal("user-1")) == ()
    with pytest.raises(TenantAccessDenied, match="membership_not_active"):
        authorizer.authorize(principal("user-1"), "tenant-a")
    assert len(stale.served_items) == 1
    live = DynamoDBMembershipCandidates(aws_runtime.dynamodb, table)
    assert live.list_candidates("user-1") == ()


def test_gsi_job_stale_nao_permite_claim(aws_runtime: AwsTestRuntime) -> None:
    table = aws_runtime.resources.table_name
    submission = create_raw_job(aws_runtime, NACIONAL)
    job = submission.job
    recorder = QueryRecorder(aws_runtime.dynamodb)
    discovering = DynamoDBControlPlane(recorder, table, aws_runtime.clock.now)
    assert discovering.list_claimable_jobs(TENANT, job.agent_id, 10) == (job,)
    completed = accept_raw_job(aws_runtime, submission)
    assert completed.state is JobState.SUCCEEDED
    stale = StaleQueryClient(aws_runtime.dynamodb, recorder.pages)
    control_plane = DynamoDBControlPlane(stale, table, aws_runtime.clock.now)

    assert control_plane.list_claimable_jobs(TENANT, job.agent_id, 10) == ()
    claim = ClaimJob(
        tenant_id=TENANT, job_id=job.job_id, owner="worker-new",
        now=aws_runtime.clock.now(), lease_seconds=300,
    )
    assert control_plane.claim_job(claim) is None
    served_keys = {(item["pk"]["S"], item["sk"]["S"]) for item in stale.served_items}
    assert entity_key(TENANT, "JOB", job.job_id) in served_keys
    assert aws_runtime.api.control_plane.get_job(TENANT, job.job_id) == completed
