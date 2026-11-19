"""Probe iam-leadingkeys: LeadingKeys, índice e ações não concedidas, cada negação pareada."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from tests.integration.aws_capability import _matrix_policies as policies
from tests.integration.aws_capability._assumed_session import assume_role_session
from tests.integration.aws_capability._iam_policy_ops import put_statements
from tests.integration.aws_capability._probe_attempt import Attempt, attempt, must, retry_denied
from tests.integration.aws_capability._safety import root_name, run_name
from tests.integration.aws_capability._service_contracts import CommandPort, ServicePorts
from tests.integration.aws_capability._table_ready import await_active_table
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import (
    RequestContext,
    Resource,
    RunSpec,
    ServiceFailure,
)

SORT = "meta"
SEED_ITEMS = (
    {"pk": "TENANT#_GLOBAL"},
    {"pk": "OUTBOX#CURSOR", "gsi6pk": "OUTBOX#PENDING", "gsi6sk": "cursor"},
    {"pk": "TENANT#x", "gsi1pk": "USER#1", "gsi1sk": "user"},
    {"pk": "ENV#prod#ATTEMPTS", "gsi6pk": "OUTBOX#OTHER", "gsi6sk": "attempts"},
    {"pk": "OTHER#1", "gsi1pk": "ADMIN#1", "gsi1sk": "admin"},
)
GLOBAL, CURSOR, TENANT, ATTEMPTS, OTHER = (
    "TENANT#_GLOBAL", "OUTBOX#CURSOR", "TENANT#x", "ENV#prod#ATTEMPTS", "OTHER#1",
)
PENDING, OUTBOX_OTHER, USER, ADMIN = "OUTBOX#PENDING", "OUTBOX#OTHER", "USER#1", "ADMIN#1"


def message_category(message: str) -> str:
    """Returns: not_authorized, transaction_canceled ou other, sem expor o texto."""
    text = message.lower()
    if "not authorized" in text:
        return "not_authorized"
    if "transaction cancel" in text:
        return "transaction_canceled"
    return "other"


def _key(pk: str) -> dict[str, object]:
    return {"pk": {"S": pk}, "sk": {"S": SORT}}


def _update(table: str, pk: str) -> dict[str, object]:
    return {
        "TableName": table,
        "Key": _key(pk),
        "UpdateExpression": "SET touched = :v",
        "ExpressionAttributeValues": {":v": {"N": "2"}},
    }


def _query(table: str, index: str | None, pk: str) -> Request:
    attribute = f"{index}pk" if index else "pk"
    payload: dict[str, object] = {"TableName": table}
    if index:
        payload["IndexName"] = index
    payload.update(
        {
            "Select": "COUNT",
            "KeyConditionExpression": f"{attribute} = :pk",
            "ExpressionAttributeValues": {":pk": {"S": pk}},
        }
    )
    return Request("dynamodb", "query", payload)


@dataclass(frozen=True)
class _Matrix:
    ports: ServicePorts
    spec: RunSpec
    context: RequestContext
    table: str

    @property
    def arn(self) -> str:
        return policies.table_arn(self.spec.region, self.spec.account, self.table)

    def get(self, pk: str) -> Request:
        return Request("dynamodb", "get-item", {"TableName": self.table, "Key": _key(pk)})

    def update(self, pk: str) -> Request:
        return Request("dynamodb", "update-item", _update(self.table, pk))

    def call(self, session: CommandPort, request: Request) -> Attempt:
        return attempt(session, request, self.context)

    def control(self, session: CommandPort, request: Request, step: str) -> float:
        _, seconds = retry_denied(
            lambda: self.call(session, request), self.ports, self.context, step
        )
        return seconds

    def code(self, session: CommandPort, request: Request) -> str:
        return self.call(session, request).code

    def seed(self) -> None:
        for item in SEED_ITEMS:
            row = {key: {"S": value} for key, value in {**item, "sk": SORT}.items()}
            request = Request("dynamodb", "put-item", {"TableName": self.table, "Item": row})
            must(self.call(self.ports.commands, request), "seed")

    def open(
        self, suffix: str, policy: str, statements: Sequence[Mapping[str, object]]
    ) -> tuple[CommandPort, float]:
        session, seconds = assume_role_session(self.ports, self.spec, self.context, suffix)
        target = (root_name(self.spec, suffix), policy)
        must(put_statements(self.ports.commands, target, statements, self.context), "policy")
        return session, seconds

    def audit(self) -> dict[str, object]:
        session, seconds = self.open(
            "audit", policies.AUDIT_DDB, policies.audit_dynamodb(self.arn)
        )
        observed: dict[str, object] = {"assume_audit_seconds": seconds}
        observed["audit_get_ready_seconds"] = self.control(session, self.get(GLOBAL), "audit_get")
        observed["audit_get_other"] = self.code(session, self.get(OTHER))
        observed["audit_update_ready_seconds"] = self.control(
            session, self.update(CURSOR), "audit_update"
        )
        observed["audit_update_tenant"] = self.code(session, self.update(TENANT))
        observed["audit_update_attempts"] = self.code(session, self.update(ATTEMPTS))
        observed.update(self.audit_queries(session))
        observed.update(self.ungranted(session))
        observed.update(self.transactions(session))
        return observed

    def audit_queries(self, session: CommandPort) -> dict[str, object]:
        t = self.table
        seconds = self.control(session, _query(t, "gsi6", PENDING), "audit_query")
        return {
            "audit_query_ready_seconds": seconds,
            "audit_query_other_key": self.code(session, _query(t, "gsi6", OUTBOX_OTHER)),
            "audit_query_gsi1": self.code(session, _query(t, "gsi1", PENDING)),
            "audit_query_table": self.code(session, _query(t, None, PENDING)),
        }

    def ungranted(self, session: CommandPort) -> dict[str, object]:
        must(self.call(session, self.get(GLOBAL)), "audit_live_control")
        put = Request(
            "dynamodb", "put-item", {"TableName": self.table, "Item": _key(GLOBAL)}
        )
        delete = Request("dynamodb", "delete-item", {"TableName": self.table, "Key": _key(GLOBAL)})
        scan = Request(
            "dynamodb", "scan", {"TableName": self.table, "Select": "COUNT", "ConsistentRead": True}
        )
        return {
            "audit_put_item_ungranted": self.code(session, put),
            "audit_delete_item_ungranted": self.code(session, delete),
            "audit_scan_ungranted": self.code(session, scan),
        }

    def transactions(self, session: CommandPort) -> dict[str, object]:
        observed: dict[str, object] = {}
        for label, keys in (("allowed", (GLOBAL, CURSOR)), ("mixed", (GLOBAL, CURSOR, OTHER))):
            items = [{"Update": _update(self.table, pk)} for pk in keys]
            result = self.call(
                session, Request("dynamodb", "transact-write-items", {"TransactItems": items})
            )
            observed[f"tx_{label}_code"] = result.code
            observed[f"tx_{label}_category"] = (
                "none" if result.ok else message_category(result.message)
            )
        return observed

    def api(self) -> dict[str, object]:
        session, seconds = self.open("api", policies.API_DDB, policies.api_dynamodb(self.arn))
        t = self.table
        observed: dict[str, object] = {"assume_api_seconds": seconds}
        observed["api_get_ready_seconds"] = self.control(session, self.get(TENANT), "api_get")
        observed["api_get_other"] = self.code(session, self.get(OTHER))
        observed["api_query_ready_seconds"] = self.control(
            session, _query(t, "gsi1", USER), "api_query"
        )
        observed["api_query_admin"] = self.code(session, _query(t, "gsi1", ADMIN))
        observed["api_query_gsi6"] = self.code(session, _query(t, "gsi6", USER))
        return observed


def probe_leadingkeys(
    resources: Sequence[Resource], spec: RunSpec, ports: ServicePorts, context: RequestContext
) -> Mapping[str, object]:
    """Returns: códigos, categorias e segundos; Raises: ServiceFailure sem controle verde."""
    name = run_name(spec)
    roles = [Resource("iam", root_name(spec, s), root_name(spec, s)) for s in ("audit", "api")]
    if context.lane != "work" or list(resources) != [Resource("dynamodb", name, name), *roles]:
        raise ServiceFailure("probe=leadingkeys_invalid")
    if ports.session is None or ports.operator is None:
        raise ServiceFailure("probe=session_unavailable")
    matrix = _Matrix(ports, spec, context, name)
    await_active_table(ports, name, context)
    matrix.seed()
    observed = matrix.audit()
    observed.update(matrix.api())
    return observed
