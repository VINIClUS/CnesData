"""CAS público com pares completos e controles independentes do oráculo."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Any

import pytest

from cnes_domain.refresh_tokens import RefreshTokenContext, RefreshTokenInvalid
from packages.cnes_infra.tests.auth.dynamodb_refresh_cas_support import (
    FIXTURE,
    MAX,
    RAW,
    USE,
    CasCase,
    assert_atomic_reads,
    assert_pair,
    assert_update_guards,
    changed,
    keys,
    mutation,
    pair,
    seed,
)
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    EXPIRES,
    ISSUED,
    OTHER_TENANT,
    TENANT,
    TOKEN,
    requests_for,
)

pytest_plugins = (
    "packages.cnes_infra.tests.auth.dynamodb_refresh_support",
    "packages.cnes_infra.tests.auth.dynamodb_refresh_cas_support",
)


def test_fixture_cas_reconstroi_bytes_e_pares_literais(cas_case: CasCase) -> None:
    assert sha256(RAW.encode()).hexdigest() == (
        "1f070f3d31eecaf74abd4b6cd76ebe1cb038d6e02c21f9b72042bfdfc8b673c4"
    )
    assert max(map(len, FIXTURE.read_text().splitlines())) <= 100
    assert json.loads(RAW)["states"]["base7"] == pair()
    assert keys()[0] != keys()[1]
    for name, group in (("base7", "target"), ("other2", "other"), ("space7", "space")):
        expected = pair(name)
        assert [{"pk": item["pk"], "sk": item["sk"]} for item in expected.values()] == keys(group)
        seed(cas_case.boundary, expected)
        assert_pair(cas_case.boundary, expected, group)


@pytest.mark.parametrize("control", ["chave", "contraparte", "timestamp"])
def test_controle_negativo_detecta_alvo_ou_estado_errado(
    cas_case: CasCase,
    control: str,
) -> None:
    boundary = cas_case.boundary
    expected = pair("used8")
    seed(boundary, expected)
    assert_pair(boundary, expected)
    wrong = deepcopy(expected)
    if control == "chave":
        wrong["digest"]["pk"] = {"S": "TENANT#bad-literal"}
        boundary.client.delete_item(TableName="refresh-tokens", Key=keys()[0])
        boundary.client.put_item(TableName="refresh-tokens", Item=wrong["digest"])
    elif control == "contraparte":
        boundary.client.delete_item(TableName="refresh-tokens", Key=keys()[1])
    else:
        wrong["agent"]["last_used_at"] = {"S": "2026-01-02T06:00:00.100003+00:00"}
        boundary.client.put_item(TableName="refresh-tokens", Item=wrong["agent"])
    with pytest.raises(AssertionError):
        assert_pair(boundary, expected)
    seed(boundary, expected)
    assert_pair(boundary, expected)


def test_uso_reutiliza_e_revogacao_nao_ressuscita(cas_case: CasCase) -> None:
    boundary = cas_case.boundary
    seed(boundary, pair())
    assert_pair(boundary, pair())
    assert cas_case.store().mark_used(TOKEN, mutation()) is True
    assert_pair(boundary, pair("used8"))
    record = cas_case.store().validate(TOKEN, RefreshTokenContext(TENANT, USE))
    assert record is not None
    assert record.version == 8
    assert record.last_used_at == USE
    assert (
        cas_case.store().mark_used(
            TOKEN,
            mutation(now=USE.replace(microsecond=100003), expected_version=8),
        )
        is True
    )
    assert_pair(boundary, pair("reused9"))
    record = cas_case.store().validate(TOKEN, RefreshTokenContext(TENANT, USE))
    assert record is not None
    assert record.version == 9
    assert (
        cas_case.store().revoke(
            TOKEN,
            mutation(now=USE.replace(microsecond=100004), expected_version=9),
        )
        is True
    )
    assert_pair(boundary, pair("revoked10"))
    assert cas_case.store().validate(TOKEN, RefreshTokenContext(TENANT, USE)) is None
    for operation, version in [("mark_used", 7), ("revoke", 7), ("mark_used", 10), ("revoke", 10)]:
        assert (
            getattr(cas_case.store(), operation)(TOKEN, mutation(expected_version=version)) is False
        )
        assert_pair(boundary, pair("revoked10"))
    assert len(requests_for(boundary, "transact_write_items")) == 3
    assert_pair(boundary, pair("revoked10"))


@pytest.mark.parametrize(
    ("operation", "case"),
    [
        ("mark_used", "ausente"),
        ("revoke", "ausente"),
        ("mark_used", "tenant"),
        ("revoke", "tenant"),
        ("mark_used", "stale"),
        ("revoke", "stale"),
        ("mark_used", "antes"),
        ("revoke", "antes"),
        ("mark_used", "expiry"),
        ("revoke", "expiry"),
        ("mark_used", "depois"),
        ("revoke", "depois"),
        ("mark_used", "revogado"),
        ("revoke", "revogado"),
    ],
    ids=[
        "mark-ausente",
        "revoke-ausente",
        "mark-tenant",
        "revoke-tenant",
        "mark-stale",
        "revoke-stale",
        "mark-antes",
        "revoke-antes",
        "mark-expiry",
        "revoke-expiry",
        "mark-depois",
        "revoke-depois",
        "mark-revogado",
        "revoke-revogado",
    ],
)
def test_cas_inativo_ou_stale_nao_altera_par(
    cas_case: CasCase,
    operation: str,
    case: str,
) -> None:
    boundary = cas_case.boundary
    before = pair("revoked8" if case == "revogado" else "base7")
    seed(boundary, before)
    context = mutation(
        tenant_id=OTHER_TENANT if case == "tenant" else TENANT,
        now={
            "antes": ISSUED - timedelta(microseconds=1),
            "expiry": EXPIRES,
            "depois": EXPIRES + timedelta(microseconds=1),
        }.get(case, USE),
        expected_version=6 if case == "stale" else int(before["digest"]["version"]["N"]),
    )
    token = "missing-opaque-token" if case == "ausente" else TOKEN
    assert getattr(cas_case.store(), operation)(token, context) is False
    assert_pair(boundary, before)
    assert requests_for(boundary, "transact_write_items") == []
    assert len(requests_for(boundary, "get_item")) == 1
    assert len(requests_for(boundary, "transact_get_items")) == int(
        case not in {"ausente", "tenant"}
    )


@pytest.mark.parametrize(
    ("operation", "instant", "unused_expected"),
    [
        ("mark_used", "2026-01-02T03:04:05.123456+00:00", (True, True)),
        ("revoke", "2026-01-02T03:04:05.123456+00:00", (True, True)),
        ("mark_used", "2026-01-03T03:04:05.654320+00:00", (True, True)),
        ("mark_used", "2026-01-02T04:05:06.234566+00:00", (False, False)),
        ("mark_used", "2026-01-02T04:05:06.234567+00:00", (False, True)),
        ("revoke", "2026-01-02T04:05:06.234566+00:00", (False, True)),
    ],
    ids=[
        "mark-emissao",
        "revoke-emissao",
        "ultimo-micro",
        "uso-anterior-menos1",
        "uso-igual",
        "revoke-anterior-menos1",
    ],
)
def test_cas_preserva_precisao_e_regra_de_uso(
    cas_case: CasCase,
    operation: str,
    instant: str,
    unused_expected: Any,
) -> None:
    unused, expected = unused_expected
    before = changed(pair(), last_used_at={"NULL": True}) if unused else pair()
    seed(cas_case.boundary, before)
    result = getattr(cas_case.store(), operation)(
        TOKEN,
        mutation(now=datetime.fromisoformat(instant)),
    )
    assert type(result) is bool
    assert result is expected
    after = (
        changed(
            before,
            version={"N": "8"},
            **{
                "last_used_at" if operation == "mark_used" else "revoked_at": {"S": instant},
            },
        )
        if expected
        else before
    )
    assert_pair(cas_case.boundary, after)
    assert_atomic_reads(cas_case.boundary)
    assert len(requests_for(cas_case.boundary, "transact_write_items")) == int(expected)


def test_cas_isola_mesmo_digest_em_dois_tenants(cas_case: CasCase) -> None:
    boundary = cas_case.boundary
    seed(boundary, pair())
    seed(boundary, pair("other2"))
    assert (
        cas_case.store().mark_used(
            TOKEN,
            mutation(tenant_id=OTHER_TENANT, expected_version=2),
        )
        is True
    )
    assert_pair(boundary, pair("other3"), "other")
    assert_pair(boundary, pair())
    assert cas_case.store().revoke(TOKEN, mutation()) is True
    assert_pair(boundary, pair("revoked8"))
    assert_pair(boundary, pair("other3"), "other")


def test_token_whitespace_e_opaco_sem_fabrica(cas_case: CasCase) -> None:
    seed(cas_case.boundary, pair("space7"))
    assert cas_case.store().mark_used(" ", mutation()) is True
    assert_pair(cas_case.boundary, pair("space8"), "space")
    assert cas_case.secret_calls == 0
    for _, request in cas_case.boundary.requests:
        assert {"S": " "} not in list(request.values())
    assert '"S": " "' not in json.dumps(cas_case.boundary.requests)


@pytest.mark.parametrize(
    ("operation", "token_changes"),
    [
        ("mark_used", (None, {})),
        ("revoke", ("", {})),
        ("mark_used", (TOKEN, {"tenant_id": "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"})),
        ("revoke", (TOKEN, {"now": USE.replace(tzinfo=None)})),
        ("mark_used", (TOKEN, {"now": USE.astimezone(timezone(timedelta(hours=1)))})),
        ("revoke", (TOKEN, {"now": "2026-01-02T06:00:00Z"})),
        ("mark_used", (TOKEN, {"expected_version": -1})),
        ("revoke", (TOKEN, {"expected_version": True})),
        ("mark_used", (TOKEN, {"expected_version": 7.0})),
    ],
    ids=[
        "token-none",
        "token-vazio",
        "tenant-maiusculo",
        "now-naive",
        "offset",
        "now-string",
        "versao-negativa",
        "versao-bool",
        "versao-float",
    ],
)
def test_mutacao_invalida_precede_sdk(
    cas_case: CasCase,
    operation: str,
    token_changes: Any,
) -> None:
    token, changes = token_changes
    with pytest.raises(RefreshTokenInvalid) as caught:
        getattr(cas_case.store(), operation)(token, mutation(**changes))
    assert str(caught.value) == "field=refresh_mutation"
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__
    assert cas_case.boundary.requests == []
    assert cas_case.secret_calls == 0


@pytest.mark.parametrize(
    ("operation", "case"),
    [
        ("mark_used", "elegivel"),
        ("revoke", "elegivel"),
        ("mark_used", "stale"),
        ("revoke", "expiry"),
        ("mark_used", "maior"),
    ],
    ids=["mark-max", "revoke-max", "stale-max", "expired-max", "caller-max-mais1"],
)
def test_limite_de_versao_respeita_elegibilidade(
    cas_case: CasCase,
    operation: str,
    case: str,
) -> None:
    before = changed(pair(), version={"N": "9223372036854775807"})
    seed(cas_case.boundary, before)
    context = mutation(
        expected_version={"stale": 7, "maior": MAX + 1}.get(case, MAX),
        now=EXPIRES if case == "expiry" else USE,
    )
    if case == "elegivel":
        cas_case.unavailable(operation, context)
    else:
        assert getattr(cas_case.store(), operation)(TOKEN, context) is False
    assert_pair(cas_case.boundary, before)
    assert_atomic_reads(cas_case.boundary)
    assert requests_for(cas_case.boundary, "transact_write_items") == []


@pytest.mark.parametrize("operation", ["mark_used", "revoke"])
def test_cas_preserva_grafias_utc_e_extras(cas_case: CasCase, operation: str) -> None:
    before = pair()
    before["digest"].update(
        issued_at={"S": "2026-01-02T03:04:05.123456Z"}, extra_digest={"S": "opaque-digest"}
    )
    before["agent"].update(
        token_expires_at={"S": "2026-01-03 03:04:05.654321+00:00"},
        last_used_at={"S": "2026-01-02 04:05:06.234567Z"},
        extra_agent={"M": {"retained": {"N": "1"}}},
    )
    seed(cas_case.boundary, before)
    assert getattr(cas_case.store(), operation)(TOKEN, mutation()) is True
    after = changed(
        before,
        version={"N": "8"},
        **{
            "last_used_at" if operation == "mark_used" else "revoked_at": {
                "S": "2026-01-02T06:00:00.100002+00:00"
            },
        },
    )
    assert_pair(cas_case.boundary, after)
    assert_update_guards(cas_case.boundary, before)
