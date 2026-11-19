"""Classificação de falhas, envelopes, privacidade e limites da escrita DynamoDB."""

import logging
from datetime import UTC, datetime
from traceback import format_exception
from typing import Any

import pytest
from botocore.exceptions import ClientError

from cnes_domain.refresh_tokens import (
    RefreshTokenConflict,
    RefreshTokenContext,
    RefreshTokenInvalid,
    RefreshTokenUnavailable,
)
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    AGENT,
    CANARY,
    DIGEST,
    FINGERPRINT,
    FINGERPRINT_CANARY,
    NOW,
    OTHER_AGENT,
    PRIVACY_NEGATIVE_CASES,
    RECORD_CANARY,
    SECRET_CANARY,
    TENANT,
    TOKEN,
    DynamoBoundary,
    active_item,
    assert_log_privacy,
    fresh_store,
    item_key,
    log_record_text,
    privacy_negative_record,
    read_item,
    requests_for,
    seed_item,
    token_input,
    token_record,
)

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)


def _seed_pair(boundary: DynamoBoundary) -> None:
    for projection in ("DIGEST", "AGENT"):
        seed_item(
            boundary,
            active_item(projection),
        )


def _seed_corrupt_pair(boundary: DynamoBoundary, field: str, value: dict[str, str]) -> None:
    for projection in ("DIGEST", "AGENT"):
        seed_item(boundary, active_item(projection, **{field: value}))


def _failure(code: str, reasons: Any = None) -> ClientError:
    message = f"{CANARY}:{SECRET_CANARY}:{RECORD_CANARY}:{FINGERPRINT_CANARY}"
    error: dict[str, Any] = {"Error": {"Code": code, "Message": message}}
    if reasons is not None:
        error["CancellationReasons"] = reasons
    return ClientError(error, "TransactWriteItems")  # type: ignore[arg-type]


def _assert_public_clean(error: BaseException, caplog: Any) -> None:
    rendered = "".join(format_exception(error)) + repr(error)
    logs = "\n".join(log_record_text(record) for record in caplog.records)
    for value in (TOKEN, FINGERPRINT, CANARY, SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY):
        assert value not in rendered
        assert value not in logs
    assert error.__cause__ is None


def _assert_strong_pair_reads(boundary: DynamoBoundary, agent_id: str = AGENT) -> None:
    reads = requests_for(boundary, "get_item")
    assert len(reads) == 2
    assert all(request["TableName"] == "refresh-tokens" for request in reads)
    assert all(request["ConsistentRead"] is True for request in reads)
    assert [request["Key"] for request in reads] == [
        item_key(TENANT, DIGEST, "DIGEST"),
        item_key(TENANT, DIGEST, "AGENT", agent_id),
    ]
    assert requests_for(boundary, "scan") == []
    assert requests_for(boundary, "query") == []


def _assert_request_plaintext_free(boundary: DynamoBoundary) -> None:
    payload = repr(boundary.requests)
    for value in (SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY):
        assert value not in payload


@pytest.mark.parametrize(
    "field",
    ["agent_id", "tenant_id", "machine_fingerprint", "issued_at", "expires_at"],
)
def test_create_invalido_de_cada_campo_nao_faz_io(
    dynamodb_case: tuple[Any, Any],
    field: str,
) -> None:
    boundary, _ = dynamodb_case
    invalid: dict[str, Any] = {
        "agent_id": "bad",
        "tenant_id": "bad",
        "machine_fingerprint": object(),
        "issued_at": "bad",
        "expires_at": "bad",
    }
    calls = 0

    def secret() -> str:
        nonlocal calls
        calls += 1
        return TOKEN

    with pytest.raises(RefreshTokenInvalid):
        fresh_store(boundary, secret).create(token_input(**{field: invalid[field]}))
    assert calls == 0
    assert boundary.requests == []


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (token_record(token_sha256="G" * 64), RefreshTokenInvalid),
        (token_record(version=-1), RefreshTokenInvalid),
        (token_record(version=2**63), RefreshTokenUnavailable),
    ],
)
def test_import_classifica_digest_e_limite_de_storage(
    dynamodb_case: tuple[Any, Any],
    record: Any,
    expected: type[BaseException],
) -> None:
    boundary, _ = dynamodb_case
    with pytest.raises(expected):
        fresh_store(boundary).import_hashed(record)
    assert boundary.requests == []


@pytest.mark.parametrize(
    ("token", "context"),
    [
        (TOKEN, RefreshTokenContext("bad", NOW)),
        (TOKEN, RefreshTokenContext(TENANT, NOW.replace(tzinfo=None))),
    ],
)
def test_validate_invalido_de_contexto_nao_faz_io(
    dynamodb_case: tuple[Any, Any],
    token: str,
    context: RefreshTokenContext,
) -> None:
    boundary, _ = dynamodb_case
    with pytest.raises(RefreshTokenInvalid):
        fresh_store(boundary).validate(token, context)
    assert boundary.requests == []


def test_validate_token_vazio_eh_caso_independente(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    with pytest.raises(RefreshTokenInvalid):
        fresh_store(boundary).validate("", RefreshTokenContext(TENANT, NOW))
    assert boundary.requests == []


@pytest.mark.parametrize(
    "field_change",
    [
        ("schema_version", {"N": "2"}),
        ("entity", {"S": "OTHER"}),
        ("projection", {"S": "AGENT"}),
        ("token_sha256", {"S": "0" * 64}),
        ("tenant_id", {"S": "22222222-2222-4222-8222-222222222222"}),
        ("agent_id", {"S": "44444444-4444-4444-8444-444444444444"}),
        ("expires_at", {"N": "1767409445"}),
        ("issued_at", {"S": "2026-01-02T03:04:05.123456+01:00"}),
        ("last_used_at", {"S": "2026-01-02T03:00:00.000000+00:00"}),
        ("version", {"N": "1.5"}),
        ("version", {"N": "9223372036854775808"}),
        ("revoked_at", {"S": "2026-01-02T02:00:00.000000+00:00"}),
    ],
)
def test_corrupcao_isolada_de_um_campo_falha_fechado(
    dynamodb_case: tuple[Any, Any],
    field_change: tuple[str, dict[str, str]],
    caplog: Any,
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    field, value = field_change
    _seed_corrupt_pair(boundary, field, value)
    with pytest.raises(RefreshTokenUnavailable) as raised:
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))
    _assert_public_clean(raised.value, caplog)


@pytest.mark.parametrize(
    ("field", "value"),
    [("agent_id", {"S": OTHER_AGENT}), ("token_sha256", {"S": "0" * 64})],
)
def test_corrupcao_agente_sozinha_falha_fechado(
    dynamodb_case: tuple[Any, Any],
    field: str,
    value: dict[str, str],
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    seed_item(boundary, active_item("AGENT", **{field: value}))
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("token_sha256", {"S": "0" * 64}),
        ("agent_id", {"S": OTHER_AGENT}),
        ("tenant_id", {"S": "22222222-2222-4222-8222-222222222222"}),
        ("machine_fingerprint", {"S": "other-fingerprint"}),
        ("issued_at", {"S": "2026-01-02T03:04:05.123457+00:00"}),
        ("expires_at", {"S": "2026-01-03T03:04:05.654322+00:00"}),
        ("last_used_at", {"S": "2026-01-02T04:05:06.234568+00:00"}),
        ("revoked_at", {"S": "2026-01-04T05:06:07.345679+00:00"}),
        ("version", {"N": "1"}),
    ],
)
def test_par_decodificado_completo_discordante_falha(
    dynamodb_case: tuple[Any, Any],
    field: str,
    value: dict[str, str],
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    seed_item(boundary, active_item("AGENT", **{field: value}))
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))


def test_campo_obrigatorio_ausente_nao_e_ausencia(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    item = active_item("DIGEST")
    del item["machine_fingerprint"]
    seed_item(boundary, item)
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))


@pytest.mark.parametrize(
    "failure",
    [
        _failure("TransactionCanceledException", [{"Code": "None"}, {"Code": "None"}]),
        _failure("TransactionCanceledException", None),
        _failure("AccessDeniedException"),
        _failure("ValidationException"),
        _failure("InternalServerError"),
    ],
)
def test_falha_de_sdk_nao_vira_colisao(
    dynamodb_case: tuple[Any, Any],
    failure: ClientError,
    caplog: Any,
) -> None:
    boundary, _ = dynamodb_case
    caplog.set_level(logging.DEBUG)
    boundary.fail_operation = "transact_write_items"
    boundary.fail_error = failure
    with pytest.raises(RefreshTokenUnavailable) as raised:
        fresh_store(boundary).import_hashed(token_record())
    _assert_public_clean(raised.value, caplog)


def test_cancelamento_condicional_com_par_coerente_eh_colisao(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    boundary.fail_operation = "transact_write_items"
    boundary.fail_error = _failure(
        "TransactionCanceledException",
        [{"Code": "ConditionalCheckFailed"}, {"Code": "ConditionalCheckFailed"}],
    )
    with pytest.raises(RefreshTokenConflict):
        fresh_store(boundary).import_hashed(token_record())
    _assert_strong_pair_reads(boundary)


@pytest.mark.parametrize(
    "reasons",
    [
        [{"Code": "ConditionalCheckFailed"}, {"Code": "None"}],
        [{"Code": "None"}, {"Code": "ConditionalCheckFailed"}],
    ],
)
def test_cancelamento_condicional_puro_com_par_eh_colisao(
    dynamodb_case: tuple[Any, Any],
    reasons: list[dict[str, str]],
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    boundary.fail_operation = "transact_write_items"
    boundary.fail_error = _failure("TransactionCanceledException", reasons)
    with pytest.raises(RefreshTokenConflict):
        fresh_store(boundary).import_hashed(token_record())
    _assert_strong_pair_reads(boundary)


@pytest.mark.parametrize(
    ("occupied", "reasons"),
    [
        ("DIGEST", [{"Code": "ConditionalCheckFailed"}, {"Code": "None"}]),
        ("AGENT", [{"Code": "None"}, {"Code": "ConditionalCheckFailed"}]),
    ],
)
def test_cancelamento_condicional_sem_identidade_e_indisponivel(
    dynamodb_case: tuple[Any, Any],
    occupied: str,
    reasons: list[dict[str, str]],
) -> None:
    boundary, _ = dynamodb_case
    seed_item(boundary, active_item(occupied))
    boundary.fail_operation = "transact_write_items"
    boundary.fail_error = _failure("TransactionCanceledException", reasons)
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).import_hashed(token_record())


def test_leitura_forte_de_ocupacao_falha_eh_indisponivel(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    boundary.fail_operation = "get_item"
    boundary.fail_error = _failure("InternalServerError")
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).import_hashed(token_record())
    reads = requests_for(boundary, "get_item")
    assert len(reads) == 1
    assert reads[0]["TableName"] == "refresh-tokens"
    assert reads[0]["Key"] == item_key(TENANT, DIGEST, "DIGEST")
    assert reads[0]["ConsistentRead"] is True


@pytest.mark.parametrize(
    "failure",
    [
        _failure("ProvisionedThroughputExceededException"),
        _failure(
            "TransactionCanceledException",
            [{"Code": "ProvisionedThroughputExceededException"}, {"Code": "None"}],
        ),
        _failure("TransactionCanceledException", ["bad", {"Code": "ConditionalCheckFailed"}]),
    ],
)
def test_cancelamento_capacity_misto_ou_malformado_nao_e_colisao(
    dynamodb_case: tuple[Any, Any],
    failure: ClientError,
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    boundary.fail_operation = "transact_write_items"
    boundary.fail_error = failure
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).import_hashed(token_record())


@pytest.mark.parametrize(
    "reasons",
    [[], [{}], [{"Code": "ConditionalCheckFailed"}], [{"Code": "Other"}, {"Code": "Other"}], "bad"],
)
def test_motivos_de_cancelamento_incompletos_nao_promovem_ocupacao(
    dynamodb_case: tuple[Any, Any],
    reasons: Any,
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    boundary.fail_operation = "transact_write_items"
    boundary.fail_error = _failure("TransactionCanceledException", reasons)
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).import_hashed(token_record())


def test_projecao_de_outro_agente_corrompida_falha_fechado(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    other = active_item("AGENT", entity={"S": "CORRUPT_SYNTHETIC"})
    other.update(item_key(TENANT, DIGEST, "AGENT", OTHER_AGENT))
    seed_item(boundary, other)
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).import_hashed(token_record(agent_id=OTHER_AGENT))
    _assert_strong_pair_reads(boundary, OTHER_AGENT)


@pytest.mark.parametrize(
    "response",
    [
        None,
        [],
        "bad",
        0,
        {},
        {"ResponseMetadata": {}},
        {"ResponseMetadata": {"HTTPStatusCode": 500}},
    ],
)
def test_resposta_de_escrita_sem_metadata_200_e_indisponivel(
    dynamodb_case: tuple[Any, Any],
    response: Any,
) -> None:
    boundary, _ = dynamodb_case
    boundary.response_overrides["transact_write_items"] = response
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).import_hashed(token_record())


@pytest.mark.parametrize(
    "record",
    [
        token_record(version="bad"),
        token_record(expires_at="bad"),
        token_record(last_used_at="bad"),
    ],
)
def test_import_dto_malformado_e_invalid_sem_io(
    dynamodb_case: tuple[Any, Any],
    record: Any,
) -> None:
    boundary, _ = dynamodb_case
    with pytest.raises(RefreshTokenInvalid):
        fresh_store(boundary).import_hashed(record)
    assert boundary.requests == []


@pytest.mark.parametrize(
    ("issued_at", "expires_at", "expected_ttl"),
    [
        (
            datetime(1970, 1, 1, tzinfo=UTC),
            datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC),
            "1",
        ),
        (
            datetime(1969, 12, 31, 23, 59, 58, tzinfo=UTC),
            datetime(1969, 12, 31, 23, 59, 59, 500000, tzinfo=UTC),
            "0",
        ),
    ],
)
def test_import_persiste_ttl_exato_e_pre_epoch(
    dynamodb_case: tuple[Any, Any],
    issued_at: datetime,
    expires_at: datetime,
    expected_ttl: str,
) -> None:
    boundary, _ = dynamodb_case
    record = token_record(
        issued_at=issued_at,
        expires_at=expires_at,
        last_used_at=None,
        revoked_at=None,
        version=0,
    )
    fresh_store(boundary).import_hashed(record)
    for projection in ("DIGEST", "AGENT"):
        item = read_item(boundary, item_key(TENANT, DIGEST, projection))
        assert item is not None
        assert item["expires_at"] == {"N": expected_ttl}


def test_privacidade_separa_payload_persistido_de_superficies_publicas(
    dynamodb_case: tuple[Any, Any],
    caplog: Any,
) -> None:
    boundary, _ = dynamodb_case
    caplog.set_level(logging.DEBUG)
    boundary.fail_operation = "transact_write_items"
    boundary.fail_error = _failure("InternalServerError")
    with pytest.raises(RefreshTokenUnavailable) as raised:
        fresh_store(boundary).import_hashed(token_record())
    _assert_public_clean(raised.value, caplog)
    assert FINGERPRINT not in str(raised.value)
    _assert_request_plaintext_free(boundary)


@pytest.mark.parametrize("case", PRIVACY_NEGATIVE_CASES)
def test_oraculo_rejeita_diagnostico_fora_da_excecao(case: str, caplog: Any) -> None:
    caplog.set_level(logging.DEBUG)
    caplog.handler.handle(privacy_negative_record(case))
    with pytest.raises(AssertionError):
        assert_log_privacy(caplog.records)
