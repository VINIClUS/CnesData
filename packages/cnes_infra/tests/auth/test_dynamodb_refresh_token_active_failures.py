"""Falhas fechadas, cursores e privacidade da consulta de atividade."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from logging import DEBUG, LogRecord
from traceback import format_exception
from typing import Any
from uuid import UUID

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from cnes_domain.refresh_tokens import (
    RefreshTokenContext,
    RefreshTokenInvalid,
    RefreshTokenUnavailable,
)
from packages.cnes_infra.tests.auth.dynamodb_refresh_cas_support import keys, pair
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    AGENT,
    CANARY,
    FINGERPRINT,
    FINGERPRINT_CANARY,
    OTHER_AGENT,
    RECORD_CANARY,
    SECRET_CANARY,
    TENANT,
    TOKEN,
    assert_log_privacy,
    item_key,
)
from packages.cnes_infra.tests.auth.test_dynamodb_refresh_token_active import (
    TABLE,
    ActiveCase,
    active_case,
    another_agent,
    context,
    variant,
)

__all__ = ["active_case"]

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)
CANARIES = (CANARY, SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY)
FOREIGN_FIXTURE_EVIDENCE: list[dict[str, Any]] = []


def wrong_context() -> RefreshTokenContext:
    values: dict[str, Any] = {"tenant_id": TENANT, "now": "wrong-type"}
    return RefreshTokenContext(**values)


def assert_private(error: BaseException) -> None:
    assert type(error) is RefreshTokenUnavailable
    assert str(error) == "operation=has_active_refresh"
    assert repr(error) == "RefreshTokenUnavailable('operation=has_active_refresh')"
    assert error.__cause__ is None
    assert error.__suppress_context__
    rendered = str(error) + repr(error) + "".join(format_exception(error))
    for canary in (*CANARIES, TOKEN, FINGERPRINT):
        assert canary not in rendered


def unavailable(case: ActiveCase) -> None:
    with pytest.raises(RefreshTokenUnavailable) as caught:
        case.store().has_active_for_agent(AGENT, context())
    assert_private(caught.value)


def assert_reached(case: ActiveCase, snapshots: int) -> None:
    operations = [name for name, _ in case.reader.requests]
    assert operations.count("query") >= 1
    assert operations.count("transact_get_items") == snapshots


INVALID_INPUTS = [
    pytest.param(None, context(), id="agente-none"),
    pytest.param(b"33333333-3333-4333-8333-333333333333", context(), id="agente-bytes"),
    pytest.param("", context(), id="agente-vazio"),
    pytest.param("33333333333343338333333333333333", context(), id="agente-nao-canonico"),
    pytest.param(UUID(AGENT), context(), id="agente-objeto-uuid"),
    pytest.param("' OR 1=1 --", context(), id="agente-injecao"),
    pytest.param(AGENT, RefreshTokenContext("invalid-tenant", context().now), id="tenant"),
    pytest.param(
        AGENT, RefreshTokenContext(TENANT, context().now.replace(tzinfo=None)), id="now-naive"
    ),
    pytest.param(
        AGENT,
        RefreshTokenContext(TENANT, datetime(2026, 1, 2, tzinfo=timezone(timedelta(hours=1)))),
        id="now-offset",
    ),
    pytest.param(AGENT, wrong_context(), id="now-tipo"),
    pytest.param(AGENT, None, id="contexto-none"),
]


@pytest.mark.parametrize(("agent", "requested"), INVALID_INPUTS)
def test_entrada_invalida_antes_de_io(
    active_case: ActiveCase,
    agent: Any,
    requested: Any,
) -> None:
    active_case.seed(pair())
    with pytest.raises(RefreshTokenInvalid):
        active_case.store().has_active_for_agent(agent, requested)
    assert active_case.reader.requests == []
    assert active_case.secret_calls == 0


ROWS = [
    pytest.param("foreign", pair("other2"), "base7", id="pk-outro-tenant"),
    pytest.param("foreign", another_agent(pair("space7")), "base7", id="pk-outro-agente"),
    pytest.param("sk", keys("space")[1]["sk"], "base7", id="sk-digest-contraditorio"),
    pytest.param("projection", {"S": "DIGEST"}, "base7", id="projecao-digest"),
    pytest.param("entity", {"S": "OTHER"}, "base7", id="entity"),
    pytest.param("schema_version", {"N": "2"}, "base7", id="schema"),
    pytest.param("token_sha256", {"S": "not-a-digest"}, "base7", id="digest"),
    pytest.param("machine_fingerprint", {"S": ""}, "expirado", id="fingerprint-inativo"),
    pytest.param("issued_at", {"S": "unreadable"}, "base7", id="issued-ilegivel"),
    pytest.param("last_used_at", {"S": "2026-01-04T00:00:00+00:00"}, "base7", id="uso-apos"),
    pytest.param("revoked_at", {"S": "2026-01-01T00:00:00+00:00"}, "base7", id="revogacao-antes"),
    pytest.param("version", {"N": "9223372036854775808"}, "futuro", id="overflow-futuro"),
]


def row_fixture(state: str) -> dict[str, Any]:
    if state == "expirado":
        return variant(
            "base7",
            token_expires_at={"S": "2026-01-02T05:00:00+00:00"},
            expires_at={"N": "1767330000"},
        )
    if state == "futuro":
        return variant(
            "base7", issued_at={"S": "2026-01-02T07:00:00+00:00"}, last_used_at={"NULL": True}
        )
    return pair()


def assert_native_coherent_pair(case: ActiveCase, items: dict[str, Any]) -> None:
    digest, agent = items["digest"], items["agent"]
    shared = {field: value for field, value in digest.items() if field not in {"pk", "projection"}}
    assert shared == {
        field: value for field, value in agent.items() if field not in {"pk", "projection"}
    }
    tenant, owner = agent["tenant_id"]["S"], agent["agent_id"]["S"]
    assert str(UUID(tenant)) == tenant
    assert str(UUID(owner)) == owner
    observed = {}
    for projection, row in items.items():
        assert row["projection"] == {"S": projection.upper()}
        key = item_key(tenant, row["token_sha256"]["S"], projection.upper(), owner)
        assert key == {"pk": row["pk"], "sk": row["sk"]}
        observed[projection] = case.observer.client.get_item(
            TableName=TABLE,
            Key=key,
            ConsistentRead=True,
        )["Item"]
    assert observed == items
    FOREIGN_FIXTURE_EVIDENCE.append(
        {
            "native_pair": observed,
            "state_before": case.state(),
            "tenant_id": tenant,
            "agent_id": owner,
            "requested_identity": (tenant, owner) == (TENANT, AGENT),
            "coherence_assertions_passed": True,
        }
    )


@pytest.mark.parametrize(("field", "value", "state"), ROWS)
def test_linha_query_inteira_validada_antes_de_atividade(
    active_case: ActiveCase,
    field: str,
    value: Any,
    state: str,
) -> None:
    active_case.seed(row_fixture(state))
    if field == "foreign":
        assert_native_coherent_pair(active_case, pair())
        active_case.seed(value)
        assert_native_coherent_pair(active_case, value)
        assert tuple(value["agent"][key]["S"] for key in ("tenant_id", "agent_id")) != (
            TENANT,
            AGENT,
        )
    reached: list[str] = []

    def corrupt(response: Any) -> Any:
        assert len(response["Items"]) == 1
        if field == "foreign":
            assert response["Items"] == [pair()["agent"]]
        reached.append("query")
        if field == "foreign":
            response["Items"] = [deepcopy(value["agent"])]
        else:
            response["Items"][0][field] = deepcopy(value)
        return response

    active_case.reader.query_hook = corrupt
    unavailable(active_case)
    assert reached == ["query"]
    assert_reached(active_case, 0)


@pytest.mark.parametrize(
    "scenario",
    [
        "digest-ausente",
        "agent-ausente",
        "ambos-ausentes",
        "divergente",
        "identidade",
        "slots",
        "slot-tipo",
        "digest-inativo-corrompido",
    ],
    ids=[
        "digest-ausente",
        "agent-ausente",
        "ambos-ausentes",
        "divergente",
        "identidade",
        "slots",
        "slot-tipo",
        "digest-inativo-corrompido",
    ],
)
def test_snapshot_obrigatorio_rejeita_par_invalido(active_case: ActiveCase, scenario: str) -> None:
    active_case.seed(row_fixture("expirado") if scenario == "digest-inativo-corrompido" else pair())
    reached: list[str] = []

    def corrupt(response: Any) -> Any:
        reached.append("snapshot")
        slots = response["Responses"]
        absent = {"digest-ausente": [0], "agent-ausente": [1], "ambos-ausentes": [0, 1]}
        for index in absent.get(scenario, []):
            slots[index] = {}
        if scenario == "divergente":
            slots[1]["Item"]["version"] = {"N": "8"}
        if scenario == "identidade":
            slots[1]["Item"]["agent_id"] = {"S": OTHER_AGENT}
        if scenario == "slots":
            response["Responses"] = slots[:1]
        if scenario == "slot-tipo":
            slots[0] = []
        if scenario == "digest-inativo-corrompido":
            slots[0]["Item"]["machine_fingerprint"] = {"S": ""}
        return response

    active_case.reader.snapshot_hook = corrupt
    unavailable(active_case)
    assert reached == ["snapshot"]
    assert_reached(active_case, 1)


ENVELOPES = [
    pytest.param("none", None, id="none"),
    pytest.param("vazio", {}, id="vazio"),
    pytest.param("metadata-ausente", None, id="metadata-ausente"),
    pytest.param("status", 503, id="status"),
    pytest.param("status", True, id="status-bool"),
    pytest.param("request-id", "", id="request-id"),
    pytest.param("items-ausente", None, id="items-ausente"),
    pytest.param("items", None, id="items-none"),
    pytest.param("items", [7], id="item-nao-mapa"),
]


@pytest.mark.parametrize(("scenario", "value"), ENVELOPES)
def test_envelope_query_falha_fechado(active_case: ActiveCase, scenario: str, value: Any) -> None:
    active_case.seed(pair())
    reached: list[str] = []

    def corrupt(response: Any) -> Any:
        reached.append("query")
        if scenario in {"none", "vazio"}:
            return value
        if scenario == "metadata-ausente":
            response.pop("ResponseMetadata")
        if scenario == "status":
            response["ResponseMetadata"]["HTTPStatusCode"] = value
        if scenario == "request-id":
            response["ResponseMetadata"]["RequestId"] = value
        if scenario == "items-ausente":
            response.pop("Items")
        if scenario == "items":
            response["Items"] = value
        return response

    active_case.reader.query_hook = corrupt
    unavailable(active_case)
    assert reached == ["query"]
    assert_reached(active_case, 0)


CURSORS = [
    pytest.param(None, id="none"),
    pytest.param([], id="lista"),
    pytest.param({"sk": keys()[1]["sk"]}, id="pk-ausente"),
    pytest.param({**keys()[1], "extra": {"S": "x"}}, id="extra"),
    pytest.param({**keys()[1], "sk": {"S": 7}}, id="s-tipo"),
    pytest.param({**keys()[1], "sk": {"S": "REFRESH_TOKEN#x", "N": "1"}}, id="s-e-n"),
    pytest.param(keys("other")[1], id="pk-externo"),
    pytest.param({**keys()[1], "sk": {"S": "OTHER#x"}}, id="namespace"),
]


@pytest.mark.parametrize("cursor", CURSORS)
def test_cursor_malformado_falha_fechado(active_case: ActiveCase, cursor: Any) -> None:
    active_case.seed(pair())
    reached: list[str] = []
    variants = [cursor]
    if cursor == {**keys()[1], "sk": {"S": 7}}:
        variants.extend(
            [
                {**keys()[1], "pk": {"S": 7}},
                {**keys()[1], "pk": {"S": ""}},
                {**keys()[1], "pk": {"N": "1"}},
                {"pk": keys()[1]["pk"]},
                {**keys()[1], "sk": {"NULL": True}},
            ]
        )
    if cursor == {**keys()[1], "sk": {"S": "OTHER#x"}}:
        variants.extend(
            [{**keys()[1], "sk": {"S": "REFRESH_TOKEN#"}}, {**keys()[1], "sk": {"S": ""}}]
        )
    for index, current in enumerate(variants):

        def corrupt(response: Any, current: Any = current) -> Any:
            reached.append("query")
            response["LastEvaluatedKey"] = deepcopy(current)
            return response

        active_case.reader.query_hook = corrupt
        unavailable(active_case)
        assert reached == ["query"] * (index + 1)
        assert sum(name == "query" for name, _ in active_case.reader.requests) == index + 1


def test_cursor_repetido_e_regressivo_nao_reiniciam_query(active_case: ActiveCase) -> None:
    active_case.seed(pair("space7"))
    active_case.seed(pair())
    for last in [keys()[1], keys("space")[1]]:
        active_case.reader.requests.clear()
        reached: list[int] = []

        def sequence(response: Any, reached: list[int] = reached, last: Any = last) -> Any:
            reached.append(len(reached) + 1)
            response["Items"] = []
            response["LastEvaluatedKey"] = deepcopy(keys()[1] if len(reached) == 1 else last)
            return response

        active_case.reader.query_hook = sequence
        unavailable(active_case)
        assert reached == [1, 2]
        requests = [request for name, request in active_case.reader.requests if name == "query"]
        assert len(requests) == 2
        assert requests[1]["ExclusiveStartKey"] == keys()[1]
        assert_reached(active_case, 0)


def sdk_error(kind: str) -> Exception:
    text = ":".join(CANARIES)
    if kind == "client":
        return ClientError({"Error": {"Code": "InternalServerError", "Message": text}}, "Query")
    if kind == "transporte":
        return EndpointConnectionError(endpoint_url="https://" + text)
    return RuntimeError(text)


@pytest.mark.parametrize(
    ("operation", "kind"),
    [
        ("query", "client"),
        ("query", "transporte"),
        ("query", "runtime"),
        ("snapshot", "client"),
        ("snapshot", "transporte"),
        ("snapshot", "runtime"),
    ],
    ids=[
        "query-client",
        "query-transporte",
        "query-runtime",
        "snapshot-client",
        "snapshot-transporte",
        "snapshot-runtime",
    ],
)
def test_erro_sdk_sanitizado_sem_retry(active_case: ActiveCase, operation: str, kind: str) -> None:
    active_case.seed(pair())
    reached: list[str] = []

    def fail(response: Any) -> Any:
        reached.append(operation)
        raise sdk_error(kind)

    if operation == "query":
        active_case.reader.query_hook = fail
    else:
        active_case.reader.snapshot_hook = fail
    unavailable(active_case)
    assert reached == [operation]
    assert_reached(active_case, int(operation == "snapshot"))
    assert sum(name == "query" for name, _ in active_case.reader.requests) == 1


@pytest.mark.parametrize(
    "scenario",
    ["mesma-pagina", "proxima-pagina", "erro-pagina"],
    ids=["mesma-pagina", "proxima-pagina", "erro-pagina"],
)
def test_erro_posterior_vence_ativo_anterior(active_case: ActiveCase, scenario: str) -> None:
    active_case.seed(pair("space7"))
    active_case.seed(pair())
    if scenario != "mesma-pagina":
        active_case.reader.limit = 1
    reached: list[int] = []

    def corrupt(response: Any) -> Any:
        reached.append(len(reached) + 1)
        if scenario == "erro-pagina" and len(reached) == 2:
            raise sdk_error("runtime")
        for row in response["Items"]:
            if row["sk"] == keys()[1]["sk"]:
                row["machine_fingerprint"] = {"S": ""}
        return response

    active_case.reader.query_hook = corrupt
    unavailable(active_case)
    assert reached == ([1] if scenario == "mesma-pagina" else [1, 2])
    assert_reached(active_case, 1)


def test_oraculo_privacidade_detecta_log_e_traceback_reais() -> None:
    record = LogRecord("cnes_infra.auth.refresh", DEBUG, __file__, 1, CANARY, (), None)
    with pytest.raises(AssertionError):
        assert_log_privacy([record])
    with pytest.raises(RuntimeError) as caught:
        raise RuntimeError(SECRET_CANARY)
    error = caught.value
    record = LogRecord(
        "root", DEBUG, __file__, 1, "clean", (), (type(error), error, error.__traceback__)
    )
    assert SECRET_CANARY in "".join(format_exception(error))
    with pytest.raises(AssertionError):
        assert_log_privacy([record])


def test_snapshot_com_canarios_nao_vaza_em_superficies_publicas(
    active_case: ActiveCase,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    active_case.seed(pair())
    reached: list[str] = []

    def fail(response: Any) -> Any:
        reached.append("snapshot")
        raise RuntimeError(":".join((*CANARIES, TOKEN, FINGERPRINT)))

    active_case.reader.snapshot_hook = fail
    unavailable(active_case)
    assert reached == ["snapshot"]
    assert_reached(active_case, 1)
    output = capsys.readouterr()
    assert output.out == output.err == ""
    assert_log_privacy(caplog.records)
