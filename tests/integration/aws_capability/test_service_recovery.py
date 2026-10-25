"""Ausência autoritativa por serviço e ID com journal durável real."""

import json
from dataclasses import dataclass, field, replace

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._testing import (
    ACCOUNT,
    NAME,
    OTHER_ACCOUNT,
    Lab,
    Remote,
    make_lab,
    owned_tags,
    ref_json,
    seed_journal,
)
from tests.integration.aws_capability.contracts import (
    Ownership,
    Page,
    RequestContext,
    Resource,
    ServiceFailure,
)


@dataclass
class RecoveryRemote(Remote):
    rows: dict[tuple[str, str], tuple[Resource, Ownership]] = field(default_factory=dict)
    hidden: set[tuple[str, str]] = field(default_factory=set)
    lookup_errors: dict[tuple[str, str], str] = field(default_factory=dict)
    lost_replies: set[tuple[str, str]] = field(default_factory=set)
    accepted_without_removal: set[tuple[str, str]] = field(default_factory=set)
    errors_after_delete: dict[tuple[str, str], str] = field(default_factory=dict)
    incomplete_service: str | None = None
    incomplete_after: int = 1
    appear_after_lookup: Resource | None = None

    def store(self, resource: Resource, owner: Ownership | None = None) -> None:
        self.rows[(resource.service, resource.resource_id)] = (
            resource, owner or Ownership(ACCOUNT, owned_tags())
        )

    def inventory(self, service: str, cursor: str | None, context: RequestContext) -> Page:
        self.request(context)
        self.touch("inventory", service, cursor)
        scans = sum(e[:2] == ("inventory", service) for e in self.events)
        if service == self.incomplete_service and scans >= self.incomplete_after:
            self.touch("inventory_error", service, cursor)
            raise ServiceFailure("inventory=unavailable")
        rows = [
            r for key, (r, _) in self.rows.items()
            if key[0] == service and key not in self.hidden
        ]
        page = self.paginate(rows, cursor)
        self.touch("inventory_result", service, cursor, page)
        return page

    def inspect(self, resource: Resource, context: RequestContext) -> Ownership:
        self.request(context)
        self.touch("inspect", resource.service, resource.resource_id)
        row = self.rows.get((resource.service, resource.resource_id))
        if row is None:
            raise ServiceFailure("inspect=not_found")
        return row[1]

    def lookup(self, resource: Resource, context: RequestContext) -> Ownership | None:
        self.request(context)
        key = (resource.service, resource.resource_id)
        self.touch("lookup", resource.service, resource.resource_id, resource.name)
        if key in self.lookup_errors:
            self.touch("lookup_error", *key, self.lookup_errors[key])
            raise ServiceFailure(self.lookup_errors[key])
        row = self.rows.get(key)
        result = row[1] if row else None
        self.touch("lookup_result", *key, result)
        if self.appear_after_lookup is not None:
            self.store(self.appear_after_lookup, Ownership(OTHER_ACCOUNT, {}))
        return result

    def delete(self, resource: Resource, bypass: bool, context: RequestContext) -> None:
        self.request(context)
        key = (resource.service, resource.resource_id)
        self.touch("delete", *key, bypass)
        if key not in self.rows:
            raise ServiceFailure("delete=not_found")
        if key in self.accepted_without_removal:
            self.hidden.add(key)
            if key in self.errors_after_delete:
                self.lookup_errors[key] = self.errors_after_delete[key]
            self.touch("delete_accepted", *key)
            return
        del self.rows[key]
        if key in self.lost_replies:
            raise ServiceFailure("delete=response_lost")


def recovery(lab: Lab, resource: Resource) -> RecoveryRemote:
    remote = RecoveryRemote(lab.events)
    lab.remote = remote
    seed_journal(lab, [resource])
    return remote


def saved(lab: Lab) -> dict:
    return json.loads(lab.journal().read_bytes())


def assert_reference_retained(lab: Lab, resource: Resource) -> None:
    data = saved(lab)
    assert data["created"] == [ref_json(resource)]
    assert data["phase"] != "complete"


def assert_inventory_after(lab: Lab, resource: Resource, after: int) -> None:
    events = lab.events[after + 1 :]
    assert not any(e[0] == "inventory_error" for e in events)
    for service in ("cognito", "dynamodb", "s3", "iam"):
        responses = [e for e in events if e[:2] == ("inventory_result", service)]
        assert responses
        cursor = None
        for event in responses:
            assert event[2] == cursor
            page = event[3]
            assert isinstance(page, Page)
            assert (resource.service, resource.resource_id) not in {
                (ref.service, ref.resource_id) for ref in page.resources
            }
            cursor = page.cursor
        assert cursor is None


def assert_absence_after_delete(lab: Lab, resource: Resource) -> None:
    deletes = [
        i for i, e in enumerate(lab.events)
        if e[:3] == ("delete", resource.service, resource.resource_id)
    ]
    assert deletes
    after = max(deletes)
    lookup = ("lookup", resource.service, resource.resource_id, resource.name)
    result = ("lookup_result", resource.service, resource.resource_id, None)
    requests = [i for i, e in enumerate(lab.events) if i > after and e == lookup]
    assert requests
    proofs = [i for i, e in enumerate(lab.events) if i > requests[0] and e == result]
    assert proofs
    assert_inventory_after(lab, resource, proofs[-1])


def test_id_ausente_confirmado_aposenta_so_apos_lookup_e_lista(lab: Lab) -> None:
    resource = Resource("s3", "exact-bucket-id", NAME)
    recovery(lab, resource)
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "succeeded"
    assert outcome.remaining == ()
    assert saved(lab)["created"] == []
    lookup = ("lookup", "s3", "exact-bucket-id", NAME)
    assert lookup in lab.events
    index = lab.events.index(lookup)
    final = lab.events[index + 1 :]
    assert {e[1] for e in final if e[0] == "inventory_result"} == {
        "cognito", "dynamodb", "s3", "iam"
    }
    assert not any(e[0] == "delete" for e in lab.events)


@pytest.mark.parametrize(
    "error",
    ["timeout", "AccessDenied", "NoSuchTagSet", "invalid_cursor", "empty_response", "s3_404"],
)
def test_erro_de_lookup_preserva_journal(lab: Lab, error: str) -> None:
    resource = Resource("s3", "uncertain-bucket", NAME)
    remote = recovery(lab, resource)
    remote.lookup_errors[("s3", "uncertain-bucket")] = error
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert_reference_retained(lab, resource)
    assert ("lookup_error", "s3", "uncertain-bucket", error) in lab.events
    assert not any(e[0] == "delete" for e in lab.events)
    remote.lookup_errors.clear()
    recovered = lifecycle.janitor(lab.spec, lab.ports())
    assert recovered.status == "succeeded"
    assert saved(lab)["created"] == []


def test_lista_omissa_com_presenca_autoritativa_ainda_exige_delete(lab: Lab) -> None:
    resource = Resource("dynamodb", "omitted-table", NAME)
    remote = recovery(lab, resource)
    remote.store(resource)
    remote.hidden.add(("dynamodb", "omitted-table"))
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "succeeded"
    assert remote.rows == {}
    assert len([e for e in lab.events if e[:3] == ("delete", "dynamodb", "omitted-table")]) == 1
    assert saved(lab)["created"] == []
    assert_absence_after_delete(lab, resource)


def test_delete_sem_resposta_exige_lookup_e_recupera_id_ausente(lab: Lab) -> None:
    resource = Resource("iam", "deleted-role", NAME)
    remote = recovery(lab, resource)
    remote.store(resource)
    remote.lost_replies.add(("iam", "deleted-role"))
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "succeeded"
    assert saved(lab)["created"] == []
    assert ("lookup_result", "iam", "deleted-role", None) in lab.events
    assert len([e for e in lab.events if e[:3] == ("delete", "iam", "deleted-role")]) <= 2


def test_id_visivel_com_outro_nome_nao_e_aposentado(lab: Lab) -> None:
    resource = Resource("s3", "renamed-bucket", NAME)
    remote = recovery(lab, resource)
    remote.store(replace(resource, name="foreign-name"), Ownership(OTHER_ACCOUNT, {}))
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert_reference_retained(lab, resource)
    assert ("s3", "renamed-bucket") in remote.rows
    assert not any(e[0] == "delete" for e in lab.events)


def test_lookup_de_ownership_divergente_preserva_referencia_omitida(lab: Lab) -> None:
    resource = Resource("iam", "foreign-role", NAME)
    remote = recovery(lab, resource)
    remote.store(resource, Ownership(OTHER_ACCOUNT, owned_tags()))
    remote.hidden.add(("iam", "foreign-role"))
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert_reference_retained(lab, resource)
    assert not any(e[0] == "delete" for e in lab.events)
    assert ("lookup", "iam", "foreign-role", NAME) in lab.events


def test_mesmo_id_em_outro_servico_nao_confirma_ausencia(lab: Lab) -> None:
    resource = Resource("s3", "shared-id", NAME)
    remote = recovery(lab, resource)
    remote.store(resource)
    remote.hidden.add(("s3", "shared-id"))
    remote.store(Resource("iam", "shared-id", "unrelated"), Ownership(OTHER_ACCOUNT, {}))
    remote.lookup_errors[("s3", "shared-id")] = "AccessDenied"
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert_reference_retained(lab, resource)
    assert ("s3", "shared-id") in remote.rows
    assert ("iam", "shared-id") in remote.rows
    assert not any(e[0] == "delete" for e in lab.events)


def test_ausencia_de_um_servico_nao_aposenta_outro_com_mesmo_id(lab: Lab) -> None:
    absent = Resource("s3", "shared-id", NAME)
    present = Resource("iam", "shared-id", NAME)
    remote = recovery(lab, absent)
    remote.store(present, Ownership(OTHER_ACCOUNT, {}))
    journal = Journal(lab.spec, phase="cleanup_requested", created=[absent, present])
    journal.save()
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert saved(lab)["created"] == [ref_json(present)]
    assert ("iam", "shared-id") in remote.rows
    assert not any(e[0] == "delete" for e in lab.events)


def test_segundo_janitor_e_idempotente(lab: Lab) -> None:
    resource = Resource("cognito", "already-deleted-pool", NAME)
    remote = recovery(lab, resource)
    remote.store(resource)
    remote.lost_replies.add(("cognito", "already-deleted-pool"))
    first = lifecycle.janitor(lab.spec, lab.ports())
    assert first.status == "succeeded"
    deletes = [e for e in lab.events if e[:3] == ("delete", "cognito", "already-deleted-pool")]
    assert 1 <= len(deletes) <= 2
    durable = lab.journal().read_bytes()
    assert json.loads(durable)["created"] == []
    fresh = make_lab(lab.spec.journal_dir)
    fresh_remote = RecoveryRemote(fresh.events)
    fresh_remote.rows = remote.rows
    fresh.remote = fresh_remote
    fresh.time = 1061
    second = lifecycle.janitor(fresh.spec, fresh.ports())
    assert second.status == "succeeded"
    assert fresh.journal().read_bytes() == durable
    assert not any(e[0] in {"create", "delete", "lookup", "probe"} for e in fresh.events)
    assert {e[1] for e in fresh.events if e[0] == "inventory"} == {
        "cognito", "dynamodb", "s3", "iam"
    }


def test_pending_desconhecido_nao_vira_ausente_por_lista(lab: Lab) -> None:
    remote = RecoveryRemote(lab.events)
    lab.remote = remote
    seed_journal(lab)
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert saved(lab)["operation_state"] == "unknown"
    assert saved(lab)["pending_service"] == "cognito"
    assert saved(lab)["phase"] != "complete"
    assert not any(e[0] in {"lookup", "delete"} for e in lab.events)


def test_ausencia_baseline_nao_autoriza_aposentadoria(lab: Lab) -> None:
    resource = Resource("iam", "preexisting-id", NAME)
    recovery(lab, resource)
    journal = Journal(lab.spec, phase="cleanup_requested", baseline=[resource], created=[resource])
    journal.save()
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert_reference_retained(lab, resource)
    assert saved(lab)["baseline"] == [ref_json(resource)]
    assert not any(e[0] == "delete" for e in lab.events)


def test_lookup_ausente_nao_substitui_inventario_final_completo(lab: Lab) -> None:
    resource = Resource("s3", "absent-with-broken-scan", NAME)
    remote = recovery(lab, resource)
    remote.incomplete_service = "iam"
    remote.incomplete_after = 2
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert outcome.evidence["inventory_complete"] is False
    assert ("lookup_result", "s3", "absent-with-broken-scan", None) in lab.events
    assert_reference_retained(lab, resource)


def test_lookup_ausente_com_id_visivel_no_final_nao_aposenta_journal(lab: Lab) -> None:
    resource = Resource("s3", "visible-after-lookup", NAME)
    remote = recovery(lab, resource)
    remote.appear_after_lookup = replace(resource, name="foreign-visible-name")
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "incomplete"
    assert_reference_retained(lab, resource)
    assert ("s3", "visible-after-lookup") in remote.rows
    assert not any(e[0] == "delete" for e in lab.events)


def test_lookup_presente_e_lista_visivel_limpam_com_controle_positivo(lab: Lab) -> None:
    resource = Resource("dynamodb", "visible-table", NAME)
    remote = recovery(lab, resource)
    remote.store(resource)
    outcome = lifecycle.janitor(lab.spec, lab.ports())
    assert outcome.status == "succeeded"
    assert saved(lab)["created"] == []
    assert remote.rows == {}
    assert len([e for e in lab.events if e[:3] == ("delete", "dynamodb", "visible-table")]) == 1
    assert_absence_after_delete(lab, resource)


@pytest.mark.parametrize(
    "lookup_state",
    [
        "present", "timeout", "AccessDenied", "NoSuchTagSet",
        "invalid_cursor", "empty_response", "s3_404",
    ],
)
def test_delete_aceito_com_residuo_omitido_exige_lookup_e_novo_janitor(
    lab: Lab, lookup_state: str
) -> None:
    resource = Resource("dynamodb", "pending-delete", NAME)
    key = ("dynamodb", "pending-delete")
    remote = recovery(lab, resource)
    remote.store(resource)
    remote.accepted_without_removal.add(key)
    if lookup_state != "present":
        remote.errors_after_delete[key] = lookup_state
    first = lifecycle.janitor(lab.spec, lab.ports())
    assert first.status == "incomplete"
    assert_reference_retained(lab, resource)
    assert key in remote.rows
    deletes = [i for i, e in enumerate(lab.events) if e[:3] == ("delete", *key)]
    assert 1 <= len(deletes) <= 2
    assert ("delete_accepted", *key) in lab.events
    result = (
        ("lookup_result", *key, Ownership(ACCOUNT, owned_tags()))
        if lookup_state == "present" else ("lookup_error", *key, lookup_state)
    )
    lookups = [i for i, e in enumerate(lab.events) if i > max(deletes) and e == result]
    assert lookups
    assert_inventory_after(lab, resource, max(deletes))
    fresh = make_lab(lab.spec.journal_dir)
    fresh_remote = RecoveryRemote(fresh.events)
    fresh_remote.rows = remote.rows.copy()
    fresh_remote.hidden = remote.hidden.copy()
    fresh.remote = fresh_remote
    fresh.time = 1061
    second = lifecycle.janitor(fresh.spec, fresh.ports())
    assert second.status == "succeeded"
    assert saved(fresh)["created"] == []
    assert fresh_remote.rows == {}
    assert not any(e[0] in {"create", "probe"} for e in fresh.events)
    assert_absence_after_delete(fresh, resource)
