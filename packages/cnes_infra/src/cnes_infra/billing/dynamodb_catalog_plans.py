"""DynamoDB catalog mixin for immutable plan versions and price mappings."""

from typing import Any

from cnes_domain.billing.errors import ImmutablePlanConflict, RetryableBillingError
from cnes_domain.billing.models import PlanVersion
from cnes_infra.billing.dynamodb_items import (
    PRICE_MAP_ENTITY,
    corrupt_item,
    decode_plan,
    decode_price_map,
    encode_plan,
    encode_price_map,
    get_item,
    put_new,
    transact,
)
from cnes_infra.billing.keys import plan_version_key, stripe_price_key


class DynamoPlanCatalogMixin:
    _client: Any
    _table: str

    def publish_plan(self, plan: PlanVersion) -> PlanVersion:
        """Publica uma PlanVersion imutável com seus mapas de preço.

        Args: PlanVersion a publicar.
        Returns: A própria PlanVersion, inclusive em replay idêntico.
        Raises: ImmutablePlanConflict, RetryableBillingError.
        """
        actions = (
            put_new(self._table, encode_plan(plan)),
            *(
                put_new(self._table, encode_price_map(price, plan.plan_version_id))
                for price in plan.stripe_price_ids
            ),
        )
        if transact(self._client, actions):
            return plan
        return self._classify_publish(plan)

    def get_plan(self, plan_version_id: str) -> PlanVersion | None:
        """Lê uma PlanVersion por chave base com leitura forte."""
        item = get_item(self._client, self._table, plan_version_key(plan_version_id), True)
        return None if item is None else decode_plan(item, plan_version_id)

    def get_plan_by_price(self, stripe_price_id: str) -> PlanVersion | None:
        """Resolve a PlanVersion de um Price; mapa inconsistente é corrupção."""
        plan_id = self._mapped_plan_id(stripe_price_id)
        if plan_id is None:
            return None
        plan = self.get_plan(plan_id)
        if plan is None or stripe_price_id not in plan.stripe_price_ids:
            raise corrupt_item(PRICE_MAP_ENTITY)
        return plan

    def _mapped_plan_id(self, stripe_price_id: str) -> str | None:
        item = get_item(self._client, self._table, stripe_price_key(stripe_price_id), True)
        return None if item is None else decode_price_map(item, stripe_price_id)

    def _classify_publish(self, plan: PlanVersion) -> PlanVersion:
        existing = self.get_plan(plan.plan_version_id)
        if existing is not None and existing != plan:
            raise ImmutablePlanConflict(f"plan_version_id={plan.plan_version_id}")
        mapped = {price: self._mapped_plan_id(price) for price in plan.stripe_price_ids}
        for price, plan_id in mapped.items():
            if plan_id is not None and plan_id != plan.plan_version_id:
                raise ImmutablePlanConflict(f"stripe_price_id={price}")
        if existing == plan and all(plan_id is not None for plan_id in mapped.values()):
            return plan
        raise RetryableBillingError("billing_transaction_conflict")
