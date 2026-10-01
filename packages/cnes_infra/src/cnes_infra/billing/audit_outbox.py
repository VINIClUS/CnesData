"""Auditoria de billing gravada no outbox canônico do DynamoDB."""

import logging
from typing import Any

from cnes_domain.billing.errors import BillingError
from cnes_domain.billing.models import BillingAuditEvent
from cnes_domain.billing.ports import BillingAuditPort, BillingMetricsPort, ClockPort
from cnes_infra.billing.dynamodb_items import (
    audit_outbox_event,
    outbox_item,
    put_new,
    transact,
)
from cnes_infra.billing.metrics import BillingMetricName, billing_metric

logger = logging.getLogger(__name__)


class DynamoBillingAudit:
    """Implementa BillingAuditPort com um Put condicional no outbox."""

    def __init__(self, client: Any, table_name: str) -> None:
        self._client = client
        self._table_name = table_name

    def append(self, event: BillingAuditEvent) -> None:
        """Grava o evento de auditoria no outbox; repetição do event_id é no-op.

        Args: Evento de auditoria com event_id determinístico.
        Raises: BillingDependencyError, PermanentBillingError.
        """
        action = put_new(self._table_name, outbox_item(audit_outbox_event(event)))
        if not transact(self._client, (action,)):
            logger.info("billing_audit_duplicate event_id=%s", event.event_id)


class BestEffortBillingAudit:
    """Auditoria que nunca propaga BillingError; falhas viram métrica."""

    def __init__(
        self, inner: BillingAuditPort, metrics: BillingMetricsPort, clock: ClockPort,
    ) -> None:
        self._inner = inner
        self._metrics = metrics
        self._clock = clock

    def append(self, event: BillingAuditEvent) -> None:
        """Args: event: Evento delegado; falhas BillingError são registradas e engolidas."""
        try:
            self._inner.append(event)
        except BillingError as error:
            logger.warning(
                "billing_audit_append_failed event_type=%s code=%s",
                event.event_type,
                error.code,
            )
            self._metrics.emit(
                billing_metric(
                    BillingMetricName.AUDIT_OUTBOX_FAILURES,
                    1,
                    self._clock(),
                    {"EventType": event.event_type},
                )
            )
