"""Reconciliação conservadora e remoção limitada com prova final fresca."""

from dataclasses import dataclass, field

from tests.integration.aws_capability._inventory import dependents, inventory
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._safety import SERVICES, SafetyFailure, matches_name, owned
from tests.integration.aws_capability.contracts import (
    BudgetExhausted,
    Outcome,
    Ownership,
    Ports,
    RequestContext,
    Resource,
    ServiceFailure,
)


@dataclass
class Cleanup:
    journal: Journal
    ports: Ports
    attempted: set[Resource] = field(default_factory=set, init=False)
    absent: set[Resource] = field(default_factory=set, init=False)

    @property
    def context(self) -> RequestContext:
        return RequestContext(self.ports.budget, "cleanup")

    def relevant(self, rows: list[tuple[Resource, Ownership]]) -> list[tuple[Resource, Ownership]]:
        references = {(ref.service, ref.resource_id) for ref in self.journal.created}
        return [
            (ref, owner)
            for ref, owner in rows
            if matches_name(ref, self.journal.spec) or (ref.service, ref.resource_id) in references
        ]

    def retire_confirmed(self, rows: list[tuple[Resource, Ownership]]) -> None:
        visible = {(ref.service, ref.resource_id) for ref, _ in rows}
        retired = {(ref.service, ref.resource_id) for ref in self.absent} - visible
        retained = [
            ref for ref in self.journal.created if (ref.service, ref.resource_id) not in retired
        ]
        if retained != self.journal.created:
            self.journal.created = retained
            self.journal.save()

    def remove_omitted(self, rows: list[tuple[Resource, Ownership]]) -> str | None:
        visible = {(ref.service, ref.resource_id) for ref, _ in rows}
        reason = None
        for ref in self.journal.created:
            if ref in self.attempted or (ref.service, ref.resource_id) in visible:
                continue
            try:
                if self.journal.is_baseline(ref):
                    raise SafetyFailure("ownership_mismatch")
                owner = self.ports.services.lookup(ref, self.context)
                if owner is None:
                    self.absent.add(ref)
                else:
                    reason = self.remove([(ref, owner)]) or reason
            except SafetyFailure as error:
                reason = error.reason
            except ServiceFailure:
                reason = "ownership_mismatch"
        return reason

    def confirm_pending(self, rows: list[tuple[Resource, Ownership]]) -> None:
        resolved = [ref for ref, owner in rows if self.resolves(ref, owner)]
        if self.journal.operation_state == "unknown" and resolved:
            self.journal.created.extend(ref for ref in resolved if ref not in self.journal.created)
            self.journal.pending_service = None
            self.journal.operation_state = "confirmed"
            self.journal.save()

    def reconcile(self) -> str | None:
        reason = None
        for _ in range(self.ports.policy.limits.pages):
            rows = self.relevant(inventory(self.ports, self.context))
            self.confirm_pending(rows)
            reason = self.remove(rows) or reason
            reason = self.remove_omitted(rows) or reason
            if self.journal.operation_state != "unknown":
                return reason
        raise SafetyFailure("creation_uncertain")

    def resolves(self, ref: Resource, owner: Ownership) -> bool:
        return (
            ref.service == self.journal.pending_service
            and not self.journal.is_baseline(ref)
            and owned(ref, owner, self.journal.spec)
        )

    def removal_order(self, root: Resource) -> list[Resource]:
        pending = [root]
        visited: set[Resource] = set()
        order: list[Resource] = []
        while pending:
            ref = pending.pop()
            if ref in visited:
                raise SafetyFailure("inventory_incomplete")
            visited.add(ref)
            owner = self.ports.services.inspect(ref, self.context)
            if self.journal.is_baseline(ref) or not owned(ref, owner, self.journal.spec):
                raise SafetyFailure("ownership_mismatch")
            pending.extend(dependents(ref, self.ports, self.context))
            order.append(ref)
        return list(reversed(order))

    def delete(self, resource: Resource) -> None:
        for _ in range(self.ports.policy.limits.attempts):
            try:
                self.ports.services.delete(resource, True, self.context)
            except ServiceFailure:
                owner = self.ports.services.lookup(resource, self.context)
                if owner is None:
                    return
                if not owned(resource, owner, self.journal.spec):
                    raise SafetyFailure("ownership_mismatch") from None
            else:
                if self.ports.services.lookup(resource, self.context) is None:
                    return
                raise SafetyFailure("cleanup_failed")
        raise SafetyFailure("cleanup_failed")

    def remove(self, rows: list[tuple[Resource, Ownership]]) -> str | None:
        reason = None
        for ref, owner in rows:
            if ref in self.attempted:
                continue
            self.attempted.add(ref)
            try:
                if self.journal.is_baseline(ref) or not owned(ref, owner, self.journal.spec):
                    raise SafetyFailure("ownership_mismatch")
                self.remove_tree(ref)
                self.absent.add(ref)
            except SafetyFailure as error:
                reason = error.reason
            except ServiceFailure:
                reason = "cleanup_failed"
        return reason

    def remove_tree(self, root: Resource) -> None:
        for resource in self.removal_order(root):
            if dependents(resource, self.ports, self.context):
                raise SafetyFailure("cleanup_failed")
            self.delete(resource)

    def execute(self) -> Outcome:
        try:
            reason = self.reconcile()
            final = self.relevant(inventory(self.ports, self.context))
            self.retire_confirmed(final)
            remaining = tuple(ref for ref, _ in final)
            evidence = {
                "inventory_complete": True,
                "counts": {
                    service: sum(ref.service == service for ref in remaining)
                    for service in SERVICES
                },
            }
            if remaining or reason or self.journal.created:
                return Outcome("incomplete", reason or "cleanup_failed", remaining, evidence)
            self.journal.phase = "complete"
            self.journal.pending_service = None
            self.journal.operation_state = "confirmed"
            self.journal.save()
            return Outcome("succeeded", "complete", (), evidence)
        except SafetyFailure as error:
            return Outcome("incomplete", error.reason, None, {"inventory_complete": False})
        except BudgetExhausted:
            reason = (
                "cleanup_timeout"
                if self.context.budget.remaining_seconds("cleanup") <= 0
                else "request_budget_exhausted"
            )
            return Outcome("incomplete", reason, None, {"inventory_complete": False})
        except OSError:
            return Outcome("incomplete", "journal_unavailable")
