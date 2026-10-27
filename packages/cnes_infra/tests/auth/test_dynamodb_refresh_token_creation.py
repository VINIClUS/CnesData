"""Criação e importação DynamoDB com par literal e transação real em moto."""

import logging
from datetime import UTC, datetime
from traceback import format_exception
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError, ReadTimeoutError

from cnes_domain.refresh_tokens import (
    RefreshTokenConflict,
    RefreshTokenContext,
    RefreshTokenInvalid,
    RefreshTokenUnavailable,
)
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    CANARY,
    DIGEST,
    FINGERPRINT,
    FINGERPRINT_CANARY,
    OTHER_AGENT,
    OTHER_TENANT,
    PRIVACY_POSITIVE_LOGS,
    RECORD_CANARY,
    SECRET_CANARY,
    TENANT,
    TOKEN,
    DynamoBoundary,
    assert_log_privacy,
    assert_ttl_ceiling,
    fresh_store,
    item_key,
    literal_item,
    literal_key,
    read_item,
    requests_for,
    seed_item,
    token_input,
    token_record,
)

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)


def _assert_sanitized(error: BaseException, caplog: Any) -> None:
    public = "".join(format_exception(error)) + repr(error)
    for secret in (TOKEN, CANARY, FINGERPRINT, SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY):
        assert secret not in public
    assert error.__cause__ is None
    assert_log_privacy(caplog.records)


def _assert_requests_plaintext_free(boundary: DynamoBoundary) -> None:
    text = repr(boundary.requests)
    for value in (SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY):
        assert value not in text


def _conditional_error() -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "TransactionCanceledException"},
            "CancellationReasons": [
                {"Code": "ConditionalCheckFailed"},
                {"Code": "ConditionalCheckFailed"},
            ],
        },
        "TransactWriteItems",
    )


def test_cria_par_completo_e_reabre_por_novo_store(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    calls = 0

    def secret() -> str:
        nonlocal calls
        calls += 1
        return TOKEN

    store = fresh_store(boundary, secret)
    assert store.create(token_input()) == TOKEN
    digest_item = read_item(boundary, literal_key("DIGEST"))
    agent_item = read_item(boundary, literal_key("AGENT"))
    assert digest_item == literal_item(
        "DIGEST", last_used_at={"NULL": True}, revoked_at={"NULL": True}, version={"N": "0"}
    )
    assert agent_item == literal_item(
        "AGENT", last_used_at={"NULL": True}, revoked_at={"NULL": True}, version={"N": "0"}
    )
    assert read_item(boundary, item_key(OTHER_TENANT, DIGEST, "DIGEST")) is None
    fresh_client = boundary.independent()
    assert (
        fresh_store(fresh_client).validate(
            TOKEN, RefreshTokenContext(TENANT, datetime(2026, 1, 2, 6, tzinfo=UTC))
        )
        is not None
    )
    assert calls == 1
    writes = requests_for(boundary, "transact_write_items")
    assert len(writes) == 1
    assert len(writes[0]["TransactItems"]) == 2
    assert all(
        action["Put"]["ConditionExpression"]
        == "attribute_not_exists(pk) AND attribute_not_exists(sk)"
        for action in writes[0]["TransactItems"]
    )
    _assert_requests_plaintext_free(boundary)


def test_importa_preserva_os_nove_campos_sem_chamar_segredo(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    calls = 0

    def forbidden() -> str:
        nonlocal calls
        calls += 1
        pytest.fail("token_factory_called_on_import")

    fresh_store(boundary, forbidden).import_hashed(token_record())
    assert calls == 0
    assert read_item(boundary, item_key(TENANT, DIGEST, "DIGEST")) == literal_item("DIGEST")
    assert read_item(boundary, item_key(TENANT, DIGEST, "AGENT")) == literal_item("AGENT")
    assert_ttl_ceiling()


def test_cada_escrita_publica_tem_token_de_cliente_novo(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    first = fresh_store(boundary)
    assert first.create(token_input()) == TOKEN
    with pytest.raises(RefreshTokenConflict):
        first.create(token_input())
    requests = requests_for(boundary, "transact_write_items")
    assert len(requests) == 2
    assert requests[0]["ClientRequestToken"] != requests[1]["ClientRequestToken"]


@pytest.mark.parametrize("record", [token_record(), token_record(agent_id=OTHER_AGENT)])
def test_colisao_nao_sobrescreve_par_existente(
    dynamodb_case: tuple[Any, Any],
    record: Any,
) -> None:
    boundary, store = dynamodb_case
    store.import_hashed(token_record())
    before = [
        read_item(boundary, item_key(TENANT, DIGEST, projection))
        for projection in ("DIGEST", "AGENT")
    ]
    with pytest.raises(RefreshTokenConflict, match="operation=insert_refresh"):
        fresh_store(boundary).import_hashed(record)
    after = [
        read_item(boundary, item_key(TENANT, DIGEST, projection))
        for projection in ("DIGEST", "AGENT")
    ]
    assert after == before


def test_mesmo_digest_em_outro_tenant_eh_independente(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, store = dynamodb_case
    store.import_hashed(token_record())
    store.import_hashed(token_record(tenant_id=OTHER_TENANT, agent_id=OTHER_AGENT))
    expected = literal_item(
        "AGENT",
        **item_key(OTHER_TENANT, DIGEST, "AGENT", OTHER_AGENT),
    )
    expected["tenant_id"] = {"S": OTHER_TENANT}
    expected["agent_id"] = {"S": OTHER_AGENT}
    assert read_item(boundary, item_key(OTHER_TENANT, DIGEST, "AGENT", OTHER_AGENT)) == expected


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (token_record(version=-1), RefreshTokenInvalid),
        (token_record(version=2**63), RefreshTokenUnavailable),
        (token_record(token_sha256="G" * 64), RefreshTokenInvalid),
    ],
)
def test_importacao_classifica_invalido_e_fora_do_storage(
    dynamodb_case: tuple[Any, Any],
    record: Any,
    expected: type[BaseException],
) -> None:
    boundary, _ = dynamodb_case
    with pytest.raises(expected):
        fresh_store(boundary).import_hashed(record)
    assert boundary.requests == []


def test_entrada_invalida_precede_fabrica_e_sdk(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    calls = 0

    def secret() -> str:
        nonlocal calls
        calls += 1
        return TOKEN

    with pytest.raises(RefreshTokenInvalid):
        fresh_store(boundary, secret).create(token_input(tenant_id="bad"))
    assert calls == 0
    assert requests_for(boundary, "transact_write_items") == []


def test_falha_da_fabrica_eh_indisponibilidade_sanitizada(
    dynamodb_case: tuple[Any, Any],
    caplog: Any,
) -> None:
    boundary, _ = dynamodb_case
    caplog.set_level(logging.DEBUG)

    def secret() -> str:
        raise RuntimeError(f"{CANARY}:{TOKEN}:{FINGERPRINT_CANARY}:{SECRET_CANARY}:{RECORD_CANARY}")

    with pytest.raises(RefreshTokenUnavailable) as raised:
        fresh_store(boundary, secret).create(token_input())
    _assert_sanitized(raised.value, caplog)


@pytest.mark.parametrize("occupied", ["DIGEST", "AGENT"])
def test_projecao_ja_ocupada_nao_cria_contraparte(
    dynamodb_case: tuple[Any, Any],
    occupied: str,
) -> None:
    boundary, store = dynamodb_case
    seed_item(boundary, literal_item(occupied, entity={"S": "CORRUPT_SYNTHETIC"}))
    with pytest.raises(RefreshTokenUnavailable):
        store.import_hashed(token_record())
    missing = "AGENT" if occupied == "DIGEST" else "DIGEST"
    assert read_item(boundary, item_key(TENANT, DIGEST, missing)) is None


def test_cancelamento_ambiguo_nao_vira_colisao(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, store = dynamodb_case
    boundary.fail_operation = "transact_write_items"
    boundary.fail_error = ClientError(
        {
            "Error": {"Code": "TransactionCanceledException"},
            "CancellationReasons": [
                {"Code": "ConditionalCheckFailed"},
                {"Code": "TransactionConflict"},
            ],
        },
        "TransactWriteItems",
    )
    with pytest.raises(RefreshTokenUnavailable):
        store.import_hashed(token_record())


def test_resposta_perdida_preserva_par_sem_inventar_sucesso(
    dynamodb_case: tuple[Any, Any],
    caplog: Any,
) -> None:
    boundary, _ = dynamodb_case
    caplog.set_level(logging.DEBUG)
    factory_calls = 0

    def secret() -> str:
        nonlocal factory_calls
        factory_calls += 1
        return TOKEN

    store = fresh_store(boundary, secret)
    original = boundary.transact_write_items
    invocations = 0

    def uncertain(**kwargs: Any) -> Any:
        nonlocal invocations
        invocations += 1
        if invocations > 1:
            raise AssertionError("application_retry")
        original(**kwargs)
        raise ReadTimeoutError(endpoint_url="https://synthetic.invalid")

    boundary.transact_write_items = uncertain  # type: ignore[method-assign]
    with pytest.raises(RefreshTokenUnavailable) as raised:
        store.create(token_input())
    _assert_sanitized(raised.value, caplog)
    assert read_item(boundary, literal_key("DIGEST")) == literal_item(
        "DIGEST", last_used_at={"NULL": True}, revoked_at={"NULL": True}, version={"N": "0"}
    )
    assert read_item(boundary, literal_key("AGENT")) == literal_item(
        "AGENT", last_used_at={"NULL": True}, revoked_at={"NULL": True}, version={"N": "0"}
    )
    assert invocations == 1
    assert factory_calls == 1
    assert len(requests_for(boundary, "transact_write_items")) == 1
    _assert_requests_plaintext_free(boundary)
    boundary.transact_write_items = original  # type: ignore[method-assign]
    with pytest.raises(RefreshTokenConflict):
        fresh_store(boundary).import_hashed(token_record())
    assert invocations == 1
    assert len(requests_for(boundary, "transact_write_items")) == 2


def test_corrida_de_dois_clientes_tem_um_vencedor(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    client_boundaries = [boundary.independent(), boundary.independent()]
    stores = [fresh_store(client_boundaries[0]), fresh_store(client_boundaries[1])]
    results: list[str] = []
    errors: list[BaseException] = []
    events: list[str] = []
    original = client_boundaries[0].transact_write_items

    def write(store: Any) -> None:
        try:
            results.append(store.create(token_input()))
        except BaseException as error:
            errors.append(error)

    def interleave(**kwargs: Any) -> Any:
        events.append("A-ready")
        write(stores[1])
        events.extend(("B-commit", "A-forward"))
        return original(**kwargs)

    client_boundaries[0].transact_write_items = interleave
    try:
        write(stores[0])
    finally:
        client_boundaries[0].transact_write_items = original
    assert len(results) == 1
    assert results == [TOKEN]
    assert read_item(boundary, item_key(TENANT, DIGEST, "DIGEST")) == literal_item(
        "DIGEST", last_used_at={"NULL": True}, revoked_at={"NULL": True}, version={"N": "0"}
    )
    assert read_item(boundary, item_key(TENANT, DIGEST, "AGENT")) == literal_item(
        "AGENT", last_used_at={"NULL": True}, revoked_at={"NULL": True}, version={"N": "0"}
    )
    assert len(errors) == 1
    assert isinstance(errors[0], (RefreshTokenConflict, RefreshTokenUnavailable))
    assert all(len(requests_for(item, "transact_write_items")) == 1 for item in client_boundaries)
    losing_boundary = next(item for item in client_boundaries if item.forwarded_errors)
    assert losing_boundary is client_boundaries[0]
    assert len(losing_boundary.forwarded_errors) == 1
    sdk_error = losing_boundary.forwarded_errors[0][1]
    assert isinstance(sdk_error, ClientError)
    response = cast("Any", sdk_error.response)
    assert response.get("Error", {}).get("Code") == "TransactionCanceledException"
    assert [reason["Code"] for reason in response["CancellationReasons"]] == [
        "ConditionalCheckFailed",
        "ConditionalCheckFailed",
    ]
    events.append("A-cancel")
    assert events == ["A-ready", "B-commit", "A-forward", "A-cancel"]


@pytest.mark.parametrize("record", PRIVACY_POSITIVE_LOGS)
def test_oraculo_aceita_oito_diagnosticos_sdk_exatos(
    record: logging.LogRecord,
    caplog: Any,
) -> None:
    caplog.set_level(logging.DEBUG)
    caplog.handler.handle(record)
    assert_log_privacy(caplog.records)
