"""Validação DynamoDB por descoberta forte e snapshot atômico dos dois itens."""

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from traceback import format_exception
from typing import Any, cast

import boto3
import pytest
from botocore.awsrequest import AWSResponse
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from cnes_domain.refresh_tokens import (
    RefreshTokenContext,
    RefreshTokenInvalid,
    RefreshTokenUnavailable,
)
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    CANARY,
    DIGEST,
    EXPIRES,
    FINGERPRINT,
    FINGERPRINT_CANARY,
    ISSUED,
    NOW,
    OTHER_TENANT,
    RECORD_CANARY,
    SECRET_CANARY,
    TENANT,
    TOKEN,
    DynamoBoundary,
    active_item,
    fresh_store,
    item_key,
    literal_item,
    literal_key,
    log_record_text,
    read_item,
    requests_for,
    seed_item,
    token_record,
    ttl,
)

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)


def _seed_pair(boundary: DynamoBoundary) -> None:
    seed_item(
        boundary,
        active_item("DIGEST"),
    )
    seed_item(
        boundary,
        active_item("AGENT"),
    )


def _assert_sanitized(error: BaseException) -> None:
    rendered = "".join(format_exception(error)) + repr(error)
    for value in (
        TOKEN,
        CANARY,
        FINGERPRINT,
        SECRET_CANARY,
        RECORD_CANARY,
        FINGERPRINT_CANARY,
    ):
        assert value not in rendered
    assert error.__cause__ is None


def _assert_request_plaintext_free(boundary: DynamoBoundary) -> None:
    payload = repr(boundary.requests)
    for value in (SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY):
        assert value not in payload


def _assert_log_records_clean(records: list[logging.LogRecord]) -> None:
    for record in records:
        rendered = log_record_text(record)
        for value in (SECRET_CANARY, RECORD_CANARY, FINGERPRINT_CANARY):
            assert value not in rendered


def _assert_validation_requests(boundary: DynamoBoundary) -> None:
    assert boundary.requests == [
        (
            "get_item",
            {"TableName": "refresh-tokens", "Key": literal_key("DIGEST"), "ConsistentRead": True},
        ),
        (
            "transact_get_items",
            {
                "TransactItems": [
                    {"Get": {"TableName": "refresh-tokens", "Key": literal_key("DIGEST")}},
                    {"Get": {"TableName": "refresh-tokens", "Key": literal_key("AGENT")}},
                ],
            },
        ),
    ]


def test_valida_par_coerente_com_leitura_forte_e_novo_store(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    boundary.requests.clear()
    result = fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))
    assert result == token_record(last_used_at=None, revoked_at=None, version=0)
    assert result is not None
    _assert_validation_requests(boundary)


@pytest.mark.parametrize(
    "now",
    [ISSUED - timedelta(microseconds=1), ISSUED, EXPIRES - timedelta(microseconds=1), EXPIRES],
)
def test_precisao_temporal_decide_sem_apagar_item(
    dynamodb_case: tuple[Any, Any],
    now: datetime,
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    result = fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, now))
    assert (result is not None) is (now >= ISSUED and now < EXPIRES)
    assert read_item(boundary, item_key(TENANT, DIGEST, "DIGEST")) is not None


def test_grafias_utc_z_e_espaco_sao_lidas_sem_reescrever(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    seed_item(
        boundary,
        active_item(
            "DIGEST",
            issued_at={"S": "2026-01-02T03:04:05.123456Z"},
            token_expires_at={"S": "2026-01-03 03:04:05.654321+00:00"},
        ),
    )
    seed_item(
        boundary,
        active_item(
            "AGENT",
            issued_at={"S": "2026-01-02 03:04:05.123456+00:00"},
            token_expires_at={"S": "2026-01-03T03:04:05.654321Z"},
        ),
    )
    result = fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))
    assert result is not None
    assert result == token_record(last_used_at=None, revoked_at=None, version=0)
    assert result.issued_at == ISSUED
    assert result.expires_at == EXPIRES
    item = read_item(boundary, item_key(TENANT, DIGEST, "DIGEST"))
    assert item is not None
    assert item["issued_at"] == {"S": "2026-01-02T03:04:05.123456Z"}
    agent = read_item(boundary, item_key(TENANT, DIGEST, "AGENT"))
    assert agent is not None
    assert agent["issued_at"] == {"S": "2026-01-02 03:04:05.123456+00:00"}


@pytest.mark.parametrize(
    ("token", "tenant"),
    [(TOKEN, OTHER_TENANT), ("wrong", TENANT)],
)
def test_isola_tenant_e_segredo_sem_scan(
    dynamodb_case: tuple[Any, Any],
    token: str,
    tenant: str,
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    assert fresh_store(boundary).validate(token, RefreshTokenContext(tenant, NOW)) is None
    assert requests_for(boundary, "scan") == []


@pytest.mark.parametrize(
    "response",
    [None, {}, {"Item": []}, {"ResponseMetadata": {"HTTPStatusCode": 200}}],
)
def test_descoberta_http200_malformada_nao_e_ausencia(
    dynamodb_case: tuple[Any, Any],
    response: Any,
) -> None:
    boundary, _ = dynamodb_case
    boundary.response_overrides["get_item"] = response
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))
    request = requests_for(boundary, "get_item")[0]
    assert request["TableName"] == "refresh-tokens"
    assert request["Key"] == item_key(TENANT, DIGEST, "DIGEST")


def test_chave_literal_interna_mismatch_falha_fechado(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    item = active_item("DIGEST")
    item["pk"] = {"S": "TENANT#wrong"}
    boundary.response_overrides["get_item"] = {
        "Item": item,
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))


def test_import_max_version_e_atributo_extra_sao_preservados(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    record = token_record(version=2**63 - 1)
    fresh_store(boundary).import_hashed(record)
    for projection in ("DIGEST", "AGENT"):
        item = read_item(boundary, item_key(TENANT, DIGEST, projection))
        assert item is not None
        assert item["version"] == {"N": str(2**63 - 1)}

    _seed_pair(boundary)
    for projection in ("DIGEST", "AGENT"):
        item = read_item(boundary, item_key(TENANT, DIGEST, projection))
        assert item is not None
        item["extra"] = {"S": "allowed"}
        seed_item(boundary, item)
    result = fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))
    assert result == token_record(last_used_at=None, revoked_at=None, version=0)


@pytest.mark.parametrize(
    "mutant",
    [
        {"schema_version": {"N": "2"}},
        {"version": {"N": "-1"}},
        {"expires_at": {"N": "1.5"}},
        {"issued_at": {"S": "bad-date"}},
        {"machine_fingerprint": {"N": "9"}},
    ],
)
def test_corrupcao_precede_inatividade_eh_indisponivel(
    dynamodb_case: tuple[Any, Any],
    mutant: dict[str, Any],
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    for projection in ("DIGEST", "AGENT"):
        seed_item(boundary, active_item(projection, **mutant))
    with pytest.raises(RefreshTokenUnavailable) as raised:
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, EXPIRES))
    _assert_sanitized(raised.value)


@pytest.mark.parametrize("state", ["revoked", "future"])
def test_corrupcao_precede_par_revogado_ou_futuro(
    dynamodb_case: tuple[Any, Any],
    state: str,
) -> None:
    boundary, _ = dynamodb_case
    if state == "revoked":
        overrides = {
            "revoked_at": {"S": "2026-01-02T05:00:00.000000+00:00"},
            "schema_version": {"N": "2"},
        }
    else:
        overrides = {
            "issued_at": {"S": "2026-01-02T07:00:00.000000+00:00"},
            "version": {"N": "-1"},
        }
    for projection in ("DIGEST", "AGENT"):
        seed_item(boundary, active_item(projection, **overrides))
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))


def test_snapshot_agent_ausente_apos_digest_presente_falha_fechado(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    seed_item(boundary, literal_item("DIGEST"))
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))


def test_digest_existente_sem_contraparte_nao_vira_ausencia(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    seed_item(boundary, literal_item("DIGEST"))
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))


def test_revogacao_coerente_retorna_none(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    seed_item(
        boundary, literal_item("DIGEST", revoked_at={"S": "2026-01-02T05:00:00.000000+00:00"})
    )
    seed_item(boundary, literal_item("AGENT", revoked_at={"S": "2026-01-02T05:00:00.000000+00:00"}))
    assert fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW)) is None


def test_gc_parcial_digest_ausente_e_agent_orfao_nao_e_descoberto(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    seed_item(boundary, literal_item("AGENT"))
    assert fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW)) is None
    assert read_item(boundary, item_key(TENANT, DIGEST, "AGENT")) is not None


def test_gc_parcial_agent_ausente_falha_fechado(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    seed_item(boundary, literal_item("DIGEST"))
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))


def test_resposta_de_leitura_malformada_nao_e_ausencia(dynamodb_case: tuple[Any, Any]) -> None:
    boundary, _ = dynamodb_case
    boundary.fail_operation = "get_item"
    boundary.fail_error = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": CANARY}},
        "GetItem",
    )
    with pytest.raises(RefreshTokenUnavailable) as raised:
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))
    _assert_sanitized(raised.value)


@pytest.mark.parametrize(("token", "tenant"), [("", TENANT), (TOKEN, "bad")])
def test_contexto_invalido_precede_sdk(
    dynamodb_case: tuple[Any, Any],
    token: str,
    tenant: str,
) -> None:
    boundary, _ = dynamodb_case
    with pytest.raises(RefreshTokenInvalid):
        fresh_store(boundary).validate(token, RefreshTokenContext(tenant, NOW))
    assert requests_for(boundary, "get_item") == []


def test_snapshot_intercalado_nao_autoriza_leitura_preliminar(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    _seed_pair(boundary)
    original = boundary.transact_get_items

    def interleave(**kwargs: Any) -> Any:
        seed_item(boundary, literal_item("DIGEST", agent_id={"S": "bad"}))
        return original(**kwargs)

    boundary.transact_get_items = interleave  # type: ignore[method-assign]
    with pytest.raises(RefreshTokenUnavailable):
        fresh_store(boundary).validate(TOKEN, RefreshTokenContext(TENANT, NOW))


def test_oraculo_de_chaves_e_fixture_wire_sao_literais_e_coerentes() -> None:
    assert literal_key("DIGEST") == item_key(TENANT, DIGEST, "DIGEST")
    assert literal_key("AGENT") == item_key(TENANT, DIGEST, "AGENT")
    assert literal_key("DIGEST")["pk"]["S"].startswith("TENANT#")
    assert literal_key("AGENT")["pk"]["S"].count("#") == 3
    assert literal_item("DIGEST")["pk"] == literal_key("DIGEST")["pk"]
    assert literal_item("AGENT")["sk"] == literal_key("AGENT")["sk"]
    assert literal_item("DIGEST")["machine_fingerprint"] == {"S": FINGERPRINT}


def test_bindings_e_guarda_offline_sao_do_target(offline_process: list[str]) -> None:
    import cnes_domain.refresh_tokens as domain_module
    import cnes_infra.auth.dynamodb_refresh_tokens as infra_module

    root = Path(__file__).resolve().parents[4]
    assert root / "packages/cnes_domain/src" in Path(domain_module.__file__).resolve().parents
    assert root / "packages/cnes_infra/src" in Path(infra_module.__file__).resolve().parents
    assert offline_process == []


def test_ttl_ceiling_cobre_segundo_exato_e_pre_epoch() -> None:
    assert ttl(datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)) == 1
    assert ttl(datetime(1969, 12, 31, 23, 59, 59, 500000, tzinfo=UTC)) == 0


def test_privacidade_harness_detecta_vazamento_e_aceita_abc_no_uuid(
    dynamodb_case: tuple[Any, Any],
) -> None:
    boundary, _ = dynamodb_case
    boundary.requests.append(
        (
            "transact_write_items",
            {"ClientRequestToken": "00000000-0000-4000-8000-000000000abc"},
        )
    )
    _assert_request_plaintext_free(boundary)
    boundary.requests.append(("transact_write_items", {"plaintext": SECRET_CANARY}))
    with pytest.raises(AssertionError):
        _assert_request_plaintext_free(boundary)


def test_privacidade_harness_inspeciona_todos_os_campos_de_log() -> None:
    try:
        raise RuntimeError(RECORD_CANARY)
    except RuntimeError as error:
        exception_info = (type(error), error, error.__traceback__)
    records = [
        logging.makeLogRecord({"msg": SECRET_CANARY}),
        logging.makeLogRecord({"msg": "safe", "exc_info": exception_info}),
        logging.makeLogRecord({"msg": "safe", "exc_text": FINGERPRINT_CANARY}),
        logging.makeLogRecord({"msg": "safe", "stack_info": RECORD_CANARY}),
    ]
    for record in records:
        with pytest.raises(AssertionError):
            _assert_log_records_clean([record])
    _assert_log_records_clean(
        [logging.makeLogRecord({"msg": "00000000-0000-4000-8000-000000000abc"})]
    )


def test_sdk_reusa_token_e_corpo_na_retentativa_da_mesma_chamada() -> None:
    client = boto3.client(
        "dynamodb",
        region_name="us-east-1",
        aws_access_key_id="synthetic",
        aws_secret_access_key="synthetic-secret",  # noqa: S106
        config=Config(retries={"mode": "standard", "total_max_attempts": 2}),
    )
    bodies: list[bytes] = []

    class Raw:
        def stream(self, *_: Any, **__: Any) -> Any:
            yield b"{}"

    def send(request: Any, **_: Any) -> AWSResponse:
        bodies.append(request.body)
        if len(bodies) == 1:
            raise EndpointConnectionError(endpoint_url="https://synthetic.invalid")
        return AWSResponse(request.url, 200, {}, Raw())  # type: ignore[arg-type]

    client._endpoint.http_session.send = send  # type: ignore[method-assign]
    token = "same-invocation-token"  # noqa: S105
    call = cast("Any", client.transact_write_items)
    call(
        TransactItems=[
            {
                "Put": {
                    "TableName": "refresh-tokens",
                    "Item": {"pk": {"S": "synthetic"}, "sk": {"S": "retry"}},
                    "ConditionExpression": "attribute_not_exists(pk)",
                },
            },
        ],
        ClientRequestToken=token,
    )
    assert len(bodies) == 2
    assert bodies[0] == bodies[1]
    assert json.loads(bodies[0])["ClientRequestToken"] == token
