"""Auditoria de billing gravada no outbox canônico do DynamoDB."""

import logging
from typing import Any

from cnes_domain.billing.models import BillingAuditEvent
from cnes_infra.billing.dynamodb_items import (
    audit_outbox_event,
    outbox_item,
    put_new,
    transact,
)

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
