"""Consulta pública de atividade com descoberta forte e pares atômicos."""

import logging
from collections.abc import Callable, Iterator
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import botocore.session
import pytest

from cnes_domain.refresh_tokens import RefreshTokenContext
from cnes_infra.auth.dynamodb_refresh_tokens import DynamoDBRefreshTokenStore
from packages.cnes_infra.tests.auth.dynamodb_refresh_cas_support import (
    USE,
    keys,
    mutation,
    native_pair,
    pair,
    seed,
)
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    AGENT,
    EXPIRES,
    ISSUED,
    OTHER_AGENT,
    OTHER_TENANT,
    TENANT,
    TOKEN,
    DynamoBoundary,
    assert_log_privacy,
    fresh_store,
    token_input,
    token_record,
)

if TYPE_CHECKING:
    from cnes_domain.ports.refresh_tokens import RefreshTokenStore

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)
TABLE = "refresh-tokens"
OBSERVATIONS: list[dict[str, Any]] = []
OTHER_SUFFIX = "#AGENT#34343434343434342d343434342d343434342d383434342d343434343434343434343434"


def context(now: datetime = USE, tenant: str = TENANT) -> RefreshTokenContext:
    return RefreshTokenContext(tenant, now)


def variant(name: str, **changes: Any) -> dict[str, Any]:
    items = pair(name)
    for item in items.values():
        item.update(deepcopy(changes))
    return items


def inactive(name: str = "base7") -> dict[str, Any]:
    return variant(name, revoked_at={"S": "2026-01-02T06:00:00.100002+00:00"})


def another_agent(items: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(items)
    result["agent"]["pk"]["S"] = result["digest"]["pk"]["S"] + OTHER_SUFFIX
    for item in result.values():
        item["agent_id"] = {"S": OTHER_AGENT}
    return result


class ActiveBoundary(DynamoBoundary):
    def __init__(self, client: Any) -> None:
        super().__init__(client)
        self.limit: int | None = None
        self.query_responses: list[Any] = []
        self.query_hook: Callable[[Any], Any] = lambda value: value
        self.snapshot_hook: Callable[[Any], Any] = lambda value: value

    def query(self, **kwargs: Any) -> Any:
        forwarded = deepcopy(kwargs)
        if self.limit is not None:
            forwarded["Limit"] = self.limit
        self.requests.append(("query", forwarded))
        assert sum(name == "query" for name, _ in self.requests) <= 12, "query_loop"
        result = self.query_hook(self.client.query(**forwarded))
        self.query_responses.append(deepcopy(result))
        return result

    def transact_get_items(self, **kwargs: Any) -> Any:
        self.requests.append(("transact_get_items", deepcopy(kwargs)))
        return self.snapshot_hook(self.client.transact_get_items(**kwargs))


class ActiveCase:
    def __init__(self, original: DynamoBoundary) -> None:
        self.reader = ActiveBoundary(original.client)
        self.observer = original.independent()
        self.writer = original.independent()
        self.writer_store = fresh_store(self.writer, self.secret)
        self.secret_calls = 0
        self.effects: list[str] = []
        self.expected: list[Any] = []
        self.addresses: list[list[dict[str, Any]]] = []
        self.partitions = [keys()[1]["pk"]]

    def secret(self) -> str:
        self.secret_calls += 1
        raise AssertionError("active_secret_factory")

    def store(self) -> DynamoDBRefreshTokenStore:
        return fresh_store(self.reader, self.secret)

    def state(self) -> list[Any]:
        items = self.observer.client.scan(TableName=TABLE, ConsistentRead=True)["Items"]
        return sorted(items, key=lambda item: (item["pk"]["S"], item["sk"]["S"]))

    def seed(self, items: dict[str, Any]) -> None:
        seed(self.observer, items)
        self.addresses.append([{"pk": item["pk"], "sk": item["sk"]} for item in items.values()])
        if "agent" in items:
            self.partitions.append(items["agent"]["pk"])
        self.expected = self.state()

    def accept_writer(self) -> None:
        self.expected = self.state()

    def blocked(self, *_: Any, **__: Any) -> None:
        self.effects.append("client_session_or_close")
        raise AssertionError("active_borrowed_client_effect")

    def assert_reads(self) -> None:
        observed = [
            {"pk": row["pk"], "sk": row["sk"]}
            for response in self.reader.query_responses
            if isinstance(response, dict)
            for row in (response["Items"] if isinstance(response.get("Items"), list) else [])
            if isinstance(row, dict)
        ]
        snapshots = [
            request["TransactItems"][1]["Get"]["Key"]
            for name, request in self.reader.requests
            if name == "transact_get_items"
        ]
        assert snapshots == observed[: len(snapshots)]
        for name, request in self.reader.requests:
            assert name in {"query", "transact_get_items"}
            if name == "transact_get_items":
                expected = [{"Get": {"TableName": TABLE, "Key": key}} for key in keys()]
                actual = request["TransactItems"]
                assert actual == expected or any(
                    actual == [{"Get": {"TableName": TABLE, "Key": key}} for key in addresses]
                    for addresses in self.addresses
                )
                assert set(request) == {"TransactItems"}
            else:
                self.assert_query(request)

    def assert_query(self, request: dict[str, Any]) -> None:
        allowed = {"TableName", "KeyConditionExpression", "ExpressionAttributeNames"}
        allowed |= {"ExpressionAttributeValues", "ConsistentRead", "Limit", "ExclusiveStartKey"}
        assert set(request) <= allowed
        assert request["TableName"] == TABLE
        assert request["ConsistentRead"] is True
        expression = request["KeyConditionExpression"]
        names, values = request["ExpressionAttributeNames"], request["ExpressionAttributeValues"]
        assert sorted(names.values()) == ["pk", "sk"]
        assert len(values) == 2
        for alias, field in names.items():
            expression = expression.replace(alias, field)
        assert {"S": "REFRESH_TOKEN#"} in values.values()
        assert any(value in self.partitions for value in values.values())
        for alias, value in values.items():
            expression = expression.replace(
                alias, "prefix" if value["S"] == "REFRESH_TOKEN#" else "pkv"
            )
        assert " ".join(expression.split()) == "pk = pkv AND begins_with(sk, prefix)"


@pytest.fixture
def active_case(
    dynamodb_case: tuple[Any, Any],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    request: pytest.FixtureRequest,
) -> Iterator[ActiveCase]:
    case = ActiveCase(dynamodb_case[0])
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(boto3, "client", case.blocked)
    monkeypatch.setattr(boto3.session.Session, "__init__", case.blocked)
    monkeypatch.setattr(botocore.session.Session, "__init__", case.blocked)
    monkeypatch.setattr(boto3.session.Session, "client", case.blocked)
    monkeypatch.setattr(botocore.session.Session, "create_client", case.blocked)
    for boundary in (case.reader, case.observer, case.writer):
        monkeypatch.setattr(boundary.client, "close", case.blocked)
    try:
        yield case
    finally:
        assert case.secret_calls == 0
        assert case.effects == []
        assert case.state() == case.expected
        case.assert_reads()
        assert_log_privacy(caplog.records)
        assert not [record for record in caplog.records if "/src/cnes_" in record.pathname]
        OBSERVATIONS.append(
            {
                "nodeid": request.node.nodeid,
                "requests": case.reader.requests,
                "responses": case.reader.query_responses,
                "factory_calls": case.secret_calls,
                "effects": case.effects,
            }
        )


def test_pares_literais_e_binding_do_worktree(active_case: ActiveCase) -> None:
    source = Path(DynamoDBRefreshTokenStore.create.__code__.co_filename).resolve()
    assert source == Path(__file__).parents[2] / "src/cnes_infra/auth/dynamodb_refresh_tokens.py"
    assert fresh_store(active_case.reader).__class__ is DynamoDBRefreshTokenStore
    for name, group in (("base7", "target"), ("space7", "space"), ("other2", "other")):
        items = pair(name)
        assert [{"pk": row["pk"], "sk": row["sk"]} for row in items.values()] == keys(group)
        active_case.seed(items)
        assert native_pair(active_case.observer, group) == items
    assert keys("space")[1]["sk"]["S"] < keys()[1]["sk"]["S"]


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("vazio", False),
        ("base7", True),
        ("revoked8", False),
        ("expirado", False),
        ("futuro", False),
    ],
    ids=["vazio", "base7", "revogado", "expirado", "futuro"],
)
def test_atividade_basica(active_case: ActiveCase, state: str, expected: bool) -> None:
    if state == "vazio":
        active_case.seed({"digest": pair()["digest"]})
    else:
        items = pair(state if state in {"base7", "revoked8"} else "base7")
        if state == "expirado":
            items = variant(
                "base7",
                token_expires_at={"S": "2026-01-02T05:00:00+00:00"},
                expires_at={"N": "1767330000"},
            )
        if state == "futuro":
            items = variant(
                "base7", issued_at={"S": "2026-01-02T07:00:00+00:00"}, last_used_at={"NULL": True}
            )
        active_case.seed(items)
    assert active_case.store().has_active_for_agent(AGENT, context()) is expected
    assert len(active_case.reader.requests) == (1 if state == "vazio" else 2)


@pytest.mark.parametrize(
    ("instant", "expected"),
    [
        (ISSUED - timedelta(microseconds=1), False),
        (ISSUED, True),
        (EXPIRES - timedelta(microseconds=1), True),
        (EXPIRES, False),
        (EXPIRES + timedelta(microseconds=1), False),
    ],
    ids=["antes-emissao", "emissao", "antes-expiracao", "expiracao", "apos-expiracao"],
)
def test_intervalo_inclusivo_exclusivo(
    active_case: ActiveCase,
    instant: datetime,
    expected: bool,
) -> None:
    active_case.seed(pair())
    assert active_case.store().has_active_for_agent(AGENT, context(instant)) is expected


@pytest.mark.parametrize(
    ("states", "expected"),
    [
        (("ativo", "ativo"), True),
        (("revogado", "ativo"), True),
        (("revogado", "revogado"), False),
    ],
    ids=["dois-ativos", "revogado-primeiro", "dois-inativos"],
)
def test_acumula_todos_candidatos(
    active_case: ActiveCase,
    states: tuple[str, str],
    expected: bool,
) -> None:
    for name, state in zip(("space7", "base7"), states, strict=True):
        active_case.seed(pair(name) if state == "ativo" else inactive(name))
    assert active_case.store().has_active_for_agent(AGENT, context()) is expected
    assert sum(name == "transact_get_items" for name, _ in active_case.reader.requests) == 2


def test_isola_quatro_combinacoes_tenant_agente(active_case: ActiveCase) -> None:
    active_case.seed(pair())
    active_case.seed(another_agent(pair("space7")))
    active_case.seed(pair("other2"))
    active_case.seed(inactive_other_agent())
    for tenant, agent, expected in [
        (TENANT, AGENT, True),
        (TENANT, OTHER_AGENT, True),
        (OTHER_TENANT, AGENT, True),
        (OTHER_TENANT, OTHER_AGENT, False),
    ]:
        store = active_case.store()
        assert store.has_active_for_agent(agent, context(tenant=tenant)) is expected
        query = [request for name, request in active_case.reader.requests if name == "query"][-1]
        selected = [
            row
            for row in active_case.expected
            if row.get("projection") == {"S": "AGENT"}
            and row["tenant_id"] == {"S": tenant}
            and row["agent_id"] == {"S": agent}
        ]
        assert selected[0]["pk"] in query["ExpressionAttributeValues"].values()


def inactive_other_agent() -> dict[str, Any]:
    items = another_agent(inactive("space7"))
    tenant_pk = keys("other")[0]["pk"]
    for row in items.values():
        row["tenant_id"] = {"S": OTHER_TENANT}
    items["digest"]["pk"] = tenant_pk
    items["agent"]["pk"] = {"S": tenant_pk["S"] + OTHER_SUFFIX}
    return items


def test_corrupcao_externa_e_outro_namespace_nao_contaminam(active_case: ActiveCase) -> None:
    active_case.seed(pair())
    foreign = pair("other2")
    foreign["agent"]["issued_at"] = {"S": "corrupt-outside-tenant"}
    active_case.seed(foreign)
    outsider = another_agent(pair("space7"))
    outsider["agent"]["schema_version"] = {"N": "9"}
    active_case.seed(outsider)
    active_case.observer.client.put_item(
        TableName=TABLE, Item={"pk": keys()[1]["pk"], "sk": {"S": "OTHER#row"}}
    )
    active_case.accept_writer()
    assert active_case.store().has_active_for_agent(AGENT, context()) is True
    assert len(active_case.reader.requests) == 2


def test_timestamp_semantico_e_extras_preservados(active_case: ActiveCase) -> None:
    items = pair()
    items["digest"]["issued_at"]["S"] = "2026-01-02T03:04:05.123456Z"
    items["agent"]["issued_at"]["S"] = "2026-01-02 03:04:05.123456+00:00"
    items["digest"]["extra_digest"] = {"M": {"nested": {"S": "preserve-digest"}}}
    items["agent"]["extra_agent"] = {"L": [{"N": "27"}, {"BOOL": True}]}
    active_case.seed(items)
    assert active_case.store().has_active_for_agent(AGENT, context()) is True
    assert native_pair(active_case.observer) == items


def test_facade_preserva_transicoes_de_uso_e_revogacao(active_case: ActiveCase) -> None:
    store: RefreshTokenStore = fresh_store(active_case.writer, active_case.secret)
    store.import_hashed(token_record(revoked_at=None))
    active_case.accept_writer()
    assert native_pair(active_case.observer) == pair()
    reader: RefreshTokenStore = active_case.store()
    assert reader.has_active_for_agent(AGENT, context()) is True
    assert store.mark_used(TOKEN, mutation()) is True
    assert native_pair(active_case.observer) == pair("used8")
    active_case.accept_writer()
    assert reader.has_active_for_agent(AGENT, context()) is True
    assert store.mark_used(TOKEN, mutation(now=USE.replace(microsecond=100003), expected_version=8))
    assert native_pair(active_case.observer) == pair("reused9")
    active_case.accept_writer()
    assert store.revoke(TOKEN, mutation(now=USE.replace(microsecond=100004), expected_version=9))
    assert native_pair(active_case.observer) == pair("revoked10")
    active_case.accept_writer()
    assert reader.has_active_for_agent(AGENT, context()) is False


@pytest.mark.parametrize(
    "scenario",
    ["ativo-depois", "ativo-primeiro", "inativos", "vazio-cursor", "fim-omitido", "fim-vazio"],
    ids=["ativo-depois", "ativo-primeiro", "inativos", "vazio-cursor", "fim-omitido", "fim-vazio"],
)
def test_paginacao_nativa_e_envelopes(active_case: ActiveCase, scenario: str) -> None:
    reader = active_case.reader
    reader.limit = 1
    active_case.seed(pair("space7") if scenario == "ativo-primeiro" else inactive("space7"))
    active_case.seed(inactive() if scenario in {"ativo-primeiro", "inativos"} else pair())
    calls: list[Any] = []

    def envelope(response: Any) -> Any:
        calls.append(deepcopy(response))
        if scenario == "vazio-cursor" and len(calls) == 1:
            response.update(Items=[], LastEvaluatedKey=keys("space")[1])
        if "LastEvaluatedKey" not in response or not response["LastEvaluatedKey"]:
            if scenario == "fim-vazio":
                response["LastEvaluatedKey"] = {}
            else:
                response.pop("LastEvaluatedKey", None)
        return response

    reader.query_hook = envelope
    assert active_case.store().has_active_for_agent(AGENT, context()) is (scenario != "inativos")
    requests = [request for name, request in reader.requests if name == "query"]
    assert len(requests) >= 2
    assert len(requests) == len(calls)
    assert "ExclusiveStartKey" not in requests[0]
    for previous, request in zip(reader.query_responses[:-1], requests[1:], strict=True):
        assert request["ExclusiveStartKey"] == previous["LastEvaluatedKey"]
    assert requests[1]["ExclusiveStartKey"] == keys("space")[1]
    assert all(request["Limit"] == 1 and request["ConsistentRead"] is True for request in requests)
    assert not reader.query_responses[-1].get("LastEvaluatedKey")


@pytest.mark.parametrize(
    "moment",
    ["uso-antes", "revoga-antes", "revoga-depois"],
    ids=["uso-antes", "revoga-antes", "revoga-depois"],
)
def test_snapshot_decide_interleaving_publico(active_case: ActiveCase, moment: str) -> None:
    active_case.seed(pair())
    reached: list[str] = []

    def write(response: Any) -> Any:
        assert reached == []
        reached.append(moment)
        operation = (
            active_case.writer_store.mark_used
            if moment == "uso-antes"
            else active_case.writer_store.revoke
        )
        assert operation(TOKEN, mutation()) is True
        assert native_pair(active_case.observer) == pair(
            "used8" if moment == "uso-antes" else "revoked8"
        )
        active_case.accept_writer()
        return response

    if moment == "revoga-depois":
        active_case.reader.snapshot_hook = write
    else:
        active_case.reader.query_hook = write
    assert active_case.store().has_active_for_agent(AGENT, context()) is (moment != "revoga-antes")
    assert reached == [moment]
    assert sum(name == "transact_get_items" for name, _ in active_case.reader.requests) == 1
    active_case.reader.query_hook = lambda value: value
    active_case.reader.snapshot_hook = lambda value: value
    assert active_case.store().has_active_for_agent(AGENT, context()) is (moment == "uso-antes")


def test_substituibilidade_completa_e_cliente_emprestado(active_case: ActiveCase) -> None:
    produced: list[str] = []

    def create_secret() -> str:
        produced.append(TOKEN)
        return TOKEN

    facade: RefreshTokenStore = fresh_store(active_case.writer, create_secret)
    assert facade.create(token_input()) == TOKEN
    assert produced == [TOKEN]
    active_case.accept_writer()
    assert facade.validate(TOKEN, context()) is not None
    reader: RefreshTokenStore = active_case.store()
    assert reader.has_active_for_agent(AGENT, context()) is True
    assert facade.mark_used(TOKEN, mutation(expected_version=0))
    active_case.accept_writer()
    assert facade.revoke(TOKEN, mutation(expected_version=1))
    active_case.accept_writer()
    imported = token_record(tenant_id=OTHER_TENANT, revoked_at=None, version=2, last_used_at=None)
    facade.import_hashed(imported)
    active_case.accept_writer()
    assert native_pair(active_case.observer, "other") == pair("other2")
    assert reader.has_active_for_agent(AGENT, context()) is False
    assert (
        active_case.reader.client.describe_table(TableName=TABLE)["Table"]["TableStatus"]
        == "ACTIVE"
    )
