"""Fence de billing na publicação do control plane DynamoDB."""

from collections.abc import Iterator
from dataclasses import replace
from typing import Any

import pytest

from cnes_domain.billing.errors import PublishDenied
from cnes_domain.billing.execution import PublicationGuard
from cnes_domain.billing.models import (
    BillingEnforcementMode,
    ReservationStatus,
    SubscriptionStatus,
)
from cnes_domain.control_plane.commands import PublicationPermit, PublishDataset, TransitionRun
from cnes_domain.control_plane.entities import DatasetPointer, DatasetVersion
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_items import encode_snapshot
from cnes_infra.billing.dynamodb_quota_items import encode_run_billing_state
from cnes_infra.billing.keys import run_billing_key
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT, make_quota_snapshot
from packages.cnes_infra.tests.billing.revocation_support import (
    RevEnv,
    create_run,
    event_of,
    open_env,
    stored_reservation,
    stored_run,
    usage_counters,
)

STRIPE = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.ENFORCE, 0)
DISABLED = BillingSettings(BillingMode.DISABLED, BillingEnforcementMode.OFF, 0)
DATASET = "cnes_vinculos"


@pytest.fixture
def env() -> Iterator[RevEnv]:
    with open_env() as opened:
        create_run(opened)
        yield opened


def plane_of(env: RevEnv, settings: BillingSettings) -> DynamoDBControlPlane:
    return DynamoDBControlPlane(env.spy, TABLE_NAME, env.clock.now, billing=settings)


def publishing(env: RevEnv) -> None:
    env.plane.transition_run(
        TransitionRun(
            tenant_id=TENANT, run_id="run-01", expected_state=RunState.PROCESSING,
            new_state=RunState.PUBLISHING, missing_sources=(),
        ),
        event_of("run.publishing"),
    )


def guard(**changes: Any) -> PublicationGuard:
    base = PublicationGuard(
        billing_account_id=ACCOUNT, expected_entitlement_version=1,
        expected_run_fencing_token=0, checked_at=NOW,
    )
    return replace(base, **changes)


def publish_command(binding_context: object | None = None, token: int = 0) -> PublishDataset:
    return PublishDataset(
        version=DatasetVersion(
            tenant_id=TENANT, dataset_name=DATASET, version_id="run-01", run_id="run-01",
            run_manifest_key=f"reconciliation/{TENANT}/2026-08/run-01/run-manifest.json",
            created_at=NOW,
        ),
        pointer_name="current", expected_version_id=None, final_state=RunState.PUBLISHED,
        missing_sources=(),
        publication_permit=PublicationPermit(
            tenant_id=TENANT, run_id="run-01", policy_version=1, fencing_token=token,
            binding_context=binding_context,
        ),
        event=event_of("dataset.published", aggregate_id="run-01"),
    )


def put_companion(env: RevEnv, **changes: Any) -> None:
    state = env.store.get_run_billing_state(TENANT, "run-01")
    env.client.put_item(
        TableName=TABLE_NAME, Item=encode_run_billing_state(replace(state, **changes))
    )


def put_snapshot(env: RevEnv, **changes: Any) -> None:
    snapshot = replace(make_quota_snapshot(), **changes)
    env.client.put_item(TableName=TABLE_NAME, Item=encode_snapshot(snapshot))


def pointer(plane: DynamoDBControlPlane) -> DatasetPointer | None:
    return plane.get_dataset_pointer(TENANT, DATASET)


def assert_untouched(env: RevEnv, plane: DynamoDBControlPlane) -> None:
    assert pointer(plane) is None
    assert stored_run(env).state is RunState.PUBLISHING


def test_stripe_publica_e_consome_a_reserva_sem_tocar_em_runs(env: RevEnv) -> None:
    plane = plane_of(env, STRIPE)
    publishing(env)
    runs_before = usage_counters(env)["consumed_runs"]

    result = plane.publish_dataset(publish_command(guard()))

    assert pointer(plane) == result
    assert stored_run(env).state is RunState.PUBLISHED
    assert stored_reservation(env).status is ReservationStatus.CONSUMED
    assert usage_counters(env)["consumed_runs"] == runs_before


def test_stripe_sem_guard_nega_a_publicacao(env: RevEnv) -> None:
    plane = plane_of(env, STRIPE)
    publishing(env)

    with pytest.raises(PublishDenied, match="publication_guard_invalid"):
        plane.publish_dataset(publish_command())

    assert_untouched(env, plane)
    assert stored_reservation(env).status is ReservationStatus.RESERVED


def test_cancelamento_solicitado_nega_a_publicacao(env: RevEnv) -> None:
    plane = plane_of(env, STRIPE)
    publishing(env)
    put_companion(env, cancel_requested=True)

    with pytest.raises(PublishDenied, match="reason=run_cancel_requested"):
        plane.publish_dataset(publish_command(guard()))

    assert_untouched(env, plane)


def test_fence_antigo_nega_a_publicacao(env: RevEnv) -> None:
    plane = plane_of(env, STRIPE)
    publishing(env)
    put_companion(env, fencing_token=1)

    with pytest.raises(PublishDenied, match="reason=stale_fence"):
        plane.publish_dataset(publish_command(guard()))

    assert_untouched(env, plane)


def test_snapshot_revogado_apos_o_guard_nega_a_publicacao(env: RevEnv) -> None:
    plane = plane_of(env, STRIPE)
    publishing(env)
    put_snapshot(env, subscription_status=SubscriptionStatus.ADMIN_REVOKED, entitlement_version=2)

    with pytest.raises(PublishDenied, match="reason=admin_revoked"):
        plane.publish_dataset(publish_command(guard()))

    assert_untouched(env, plane)


def test_snapshot_com_versao_nova_nega_a_publicacao(env: RevEnv) -> None:
    plane = plane_of(env, STRIPE)
    publishing(env)
    put_snapshot(env, entitlement_version=2)

    with pytest.raises(PublishDenied, match="reason=stale_entitlement"):
        plane.publish_dataset(publish_command(guard()))

    assert_untouched(env, plane)


def once(env: RevEnv, action: Any) -> None:
    def hook() -> None:
        env.spy.before_transact = None
        action()

    env.spy.before_transact = hook


def test_snapshot_alterado_entre_leitura_e_transacao_nega_a_publicacao(env: RevEnv) -> None:
    plane = plane_of(env, STRIPE)
    publishing(env)
    once(env, lambda: put_snapshot(env, subscription_status=SubscriptionStatus.ADMIN_REVOKED,
                                   entitlement_version=2))

    with pytest.raises(PublishDenied, match="reason=admin_revoked"):
        plane.publish_dataset(publish_command(guard()))

    assert_untouched(env, plane)
    assert stored_reservation(env).status is ReservationStatus.RESERVED


def test_companion_alterado_sem_violar_fence_repete_o_conflito_original(env: RevEnv) -> None:
    plane = plane_of(env, STRIPE)
    publishing(env)
    once(env, lambda: put_companion(env, updated_at=NOW.replace(minute=5)))

    with pytest.raises(Conflict):
        plane.publish_dataset(publish_command(guard()))

    assert_untouched(env, plane)


def test_disabled_publica_run_legado_sem_companion(env: RevEnv) -> None:
    plane = plane_of(env, DISABLED)
    publishing(env)
    env.client.delete_item(TableName=TABLE_NAME, Key=item_key(*run_billing_key(TENANT, "run-01")))

    result = plane.publish_dataset(publish_command())

    assert pointer(plane) == result
    assert stored_reservation(env).status is ReservationStatus.RESERVED


def test_disabled_com_companion_valido_publica_verificando_o_companion(env: RevEnv) -> None:
    plane = plane_of(env, DISABLED)
    publishing(env)

    result = plane.publish_dataset(publish_command())

    pk, sk = run_billing_key(TENANT, "run-01")
    keys = [
        action["ConditionCheck"]["Key"]
        for action in env.spy.transactions[-1]
        if "ConditionCheck" in action
    ]
    assert pointer(plane) == result
    assert {"pk": {"S": pk}, "sk": {"S": sk}} in keys
    assert stored_reservation(env).status is ReservationStatus.RESERVED


def test_disabled_com_fence_divergente_nega_a_publicacao(env: RevEnv) -> None:
    plane = plane_of(env, DISABLED)
    publishing(env)

    with pytest.raises(PublishDenied, match="reason=stale_fence"):
        plane.publish_dataset(publish_command(token=3))

    assert_untouched(env, plane)


def test_replay_de_publicacao_concluida_nao_reavalia_o_billing(env: RevEnv) -> None:
    plane = plane_of(env, STRIPE)
    publishing(env)
    command = publish_command(guard())
    first = plane.publish_dataset(command)
    put_companion(env, cancel_requested=True)
    put_snapshot(env, subscription_status=SubscriptionStatus.ADMIN_REVOKED, entitlement_version=3)

    assert plane.publish_dataset(command) == first
