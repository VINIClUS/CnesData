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
        return [(ref, owner) for ref, owner in rows if self._relevant_ref(ref, references)]

    def _relevant_ref(self, ref: Resource, references: set[tuple[str, str]]) -> bool:
        if (
            ref.service == "cognito"
            and self.journal.operation_state == "unknown"
            and self.journal.pending_service == "cognito"
        ):
            return ref.name == f"cnesdata-cap-{self.journal.spec.run_id}"
        return matches_name(ref, self.journal.spec) or (ref.service, ref.resource_id) in references

    def retire_confirmed(self, rows: list[tuple[Resource, Ownership]]) -> None:
        retired = self.retired_identities(rows)
        retained = [
            ref for ref in self.journal.created if (ref.service, ref.resource_id) not in retired
        ]
        if retained != self.journal.created:
            previous = (
                self.journal.created,
                self.journal.dynamodb_delete_accepted,
                self.journal.operation_state,
            )
            self.journal.created = retained
            self.journal.dynamodb_delete_accepted = [
                ref
                for ref in self.journal.dynamodb_delete_accepted
                if (ref.service, ref.resource_id) not in retired
            ]
            if self.journal.spec.probe == "cognito-resource-server" and not retained:
                self.journal.operation_state = "confirmed"
            try:
                self.checkpoint()
            except OSError:
                (
                    self.journal.created,
                    self.journal.dynamodb_delete_accepted,
                    self.journal.operation_state,
                ) = previous
                raise

    def retired_identities(self, rows: list[tuple[Resource, Ownership]]) -> set[tuple[str, str]]:
        visible = {(ref.service, ref.resource_id) for ref, _ in rows}
        retired = {(ref.service, ref.resource_id) for ref in self.absent} - visible
        accepted = {(ref.service, ref.resource_id) for ref in self.journal.dynamodb_delete_accepted}
        retired = {key for key in retired if key[0] != "dynamodb" or key in accepted}
        if self.journal.spec.probe == "cognito-resource-server" and len(self.journal.created) == 2:
            root, child = self.journal.created
            if (child.service, child.resource_id) not in retired:
                retired.discard((root.service, root.resource_id))
        return retired

    def remove_omitted(self, rows: list[tuple[Resource, Ownership]]) -> str | None:
        visible = {(ref.service, ref.resource_id) for ref, _ in rows}
        reason = None
        for ref in self.journal.created:
            if ref in self.attempted or (ref.service, ref.resource_id) in visible:
                continue
            try:
                reason = self.remove_omitted_resource(ref) or reason
            except SafetyFailure as error:
                reason = error.reason
            except ServiceFailure:
                reason = "ownership_mismatch"
        return reason

    def remove_omitted_resource(self, ref: Resource) -> str | None:
        if self.journal.is_baseline(ref):
            raise SafetyFailure("ownership_mismatch")
        owner = self.ports.services.lookup(ref, self.context)
        if ref in self.journal.dynamodb_delete_accepted:
            self.confirm_dynamodb_absent(ref, owner)
            self.absent.add(ref)
        elif owner is None:
            if ref.service == "dynamodb":
                raise SafetyFailure("creation_uncertain")
            self.absent.add(ref)
        else:
            return self.remove([(ref, owner)])
        return None

    def confirm_pending(self, rows: list[tuple[Resource, Ownership]]) -> None:
        if self.journal.pending_resource is not None:
            self.confirm_server_pending()
            return
        self._guard_cognito_unknown(rows)
        resolved = [ref for ref, owner in rows if self.resolves(ref, owner)]
        if self.journal.operation_state == "unknown" and resolved:
            self.journal.created.extend(ref for ref in resolved if ref not in self.journal.created)
            self.journal.pending_service = None
            self.journal.operation_state = "confirmed"
            self.checkpoint()

    def checkpoint(self) -> None:
        if self.journal.spec.probe == "cognito-resource-server":
            self.journal.phase = "cleanup_requested"
        self.journal.save()

    def confirm_server_pending(self) -> None:
        pending = self.journal.pending_resource
        if pending is None or self.journal.operation_state != "unknown":
            raise SafetyFailure("creation_uncertain")
        result = self.ports.services.lookup_server_exact(pending, self.context)
        if result not in {"exact_present", "exact_absent"}:
            raise SafetyFailure("creation_uncertain")
        if result == "exact_present":
            self.journal.created.append(pending)
        self.journal.operation_state = "confirmed" if result == "exact_present" else "rejected"
        self.journal.pending_service = None
        self.journal.pending_resource = None
        self.checkpoint()

    def _guard_cognito_unknown(self, rows: list[tuple[Resource, Ownership]]) -> None:
        if self.journal.operation_state != "unknown" or self.journal.pending_service != "cognito":
            return
        candidates = self.cognito_candidates(rows)
        if any(
            self.journal.is_baseline(ref) or not self._owned_cognito(ref, owner)
            for ref, owner in candidates
        ):
            raise SafetyFailure("ownership_mismatch")
        if len(candidates) > 1:
            raise SafetyFailure("ownership_mismatch")

    def cognito_candidates(
        self, rows: list[tuple[Resource, Ownership]]
    ) -> list[tuple[Resource, Ownership]]:
        exact_name = f"cnesdata-cap-{self.journal.spec.run_id}"
        return [
            (ref, owner)
            for ref, owner in rows
            if ref.service == "cognito"
            and (ref.name == exact_name or ref.name.startswith(exact_name + "-"))
        ]

    def _owned_cognito(self, ref: Resource, owner: Ownership) -> bool:
        expected = f"cnesdata-cap-{self.journal.spec.run_id}"
        return owned(ref, owner, self.journal.spec) and ref.name == expected

    def reconcile(self) -> str | None:
        reason = None
        if self.journal.pending_resource is not None:
            self.confirm_pending([])
        for _ in range(self.ports.policy.limits.pages):
            all_rows = inventory(self.ports, self.context)
            self._guard_cognito_unknown(all_rows)
            rows = self.relevant(all_rows)
            self.confirm_pending(rows)
            reason = self.remove(rows) or reason
            reason = self.remove_omitted(rows) or reason
            if self.journal.operation_state != "unknown":
                return reason
        raise SafetyFailure("creation_uncertain")

    def resolves(self, ref: Resource, owner: Ownership) -> bool:
        if ref.service == "cognito":
            return (
                self.journal.pending_service == "cognito"
                and ref.name == f"cnesdata-cap-{self.journal.spec.run_id}"
                and self._owned_cognito(ref, owner)
                and not self.journal.is_baseline(ref)
            )
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
        if resource.service == "dynamodb":
            self.delete_dynamodb(resource)
            return
        for _ in range(self.ports.policy.limits.attempts):
            if self.delete_attempt(resource):
                return
        raise SafetyFailure("cleanup_failed")

    def delete_attempt(self, resource: Resource) -> bool:
        try:
            self.ports.services.delete(resource, True, self.context)
        except ServiceFailure:
            owner = self.ports.services.lookup(resource, self.context)
            if owner is None:
                return True
            if not owned(resource, owner, self.journal.spec):
                raise SafetyFailure("ownership_mismatch") from None
            return False
        if self.ports.services.lookup(resource, self.context) is None:
            return True
        raise SafetyFailure("cleanup_failed")

    def delete_dynamodb(self, resource: Resource) -> None:
        self.journal.authorize_dynamodb_delete(resource)
        if resource not in self.journal.dynamodb_delete_accepted:
            self.ports.services.delete(resource, True, self.context)
            self.journal.record_dynamodb_delete(resource)
        owner = self.ports.services.lookup(resource, self.context)
        self.confirm_dynamodb_absent(resource, owner)

    def confirm_dynamodb_absent(self, resource: Resource, owner: Ownership | None) -> None:
        self.journal.authorize_dynamodb_delete(resource)
        if owner is not None:
            if not owned(resource, owner, self.journal.spec):
                raise SafetyFailure("ownership_mismatch")
            raise SafetyFailure("cleanup_failed")
        if resource not in self.journal.dynamodb_delete_accepted:
            raise SafetyFailure("creation_uncertain")

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
            self.retire_before_parent(resource)
            self.delete(resource)
            if self.is_created_server(resource):
                self.retire_server(resource)

    def is_created_server(self, resource: Resource) -> bool:
        return (
            self.journal.spec.probe == "cognito-resource-server"
            and resource in self.journal.created[1:]
        )

    def retire_before_parent(self, resource: Resource) -> None:
        if (
            self.journal.spec.probe == "cognito-resource-server"
            and len(self.journal.created) == 2
            and resource == self.journal.created[0]
        ):
            self.retire_server(self.journal.created[1])

    def retire_server(self, resource: Resource) -> None:
        if self.ports.services.lookup_server_exact(resource, self.context) != "exact_absent":
            raise SafetyFailure("creation_uncertain")
        self.attempted.add(resource)
        self.absent.add(resource)
        rows = inventory(self.ports, self.context)
        self.retire_confirmed(rows)
        if resource in self.journal.created:
            raise SafetyFailure("inventory_incomplete")

    def execute(self) -> Outcome:
        try:
            return self.final_outcome(self.reconcile())
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

    def final_outcome(self, reason: str | None) -> Outcome:
        final = self.relevant(inventory(self.ports, self.context))
        self.retire_confirmed(final)
        remaining = tuple(ref for ref, _ in final)
        evidence = {
            "inventory_complete": True,
            "counts": {
                service: sum(ref.service == service for ref in remaining) for service in SERVICES
            },
        }
        if remaining or reason or self.journal.created:
            return Outcome("incomplete", reason or "cleanup_failed", remaining, evidence)
        self.journal.phase = "complete"
        self.journal.pending_service = None
        self.journal.operation_state = "confirmed"
        self.journal.save()
        return Outcome("succeeded", "complete", (), evidence)
