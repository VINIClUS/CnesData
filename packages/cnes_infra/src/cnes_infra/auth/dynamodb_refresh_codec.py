"""Chaves e registros completos de refresh DynamoDB schema1."""

from collections.abc import Mapping
from datetime import UTC, datetime
from re import fullmatch
from typing import Any, Literal, cast

from cnes_domain.refresh_tokens import RefreshTokenRecord, validate_refresh_token
from cnes_infra.control_plane.dynamodb_keys import key_component

type Projection = Literal["DIGEST", "AGENT"]
type Item = dict[str, dict[str, str | bool]]
MAX_VERSION = 9223372036854775807


def refresh_key(tenant: str, digest: str, agent: str | None = None) -> Item:
    """Returns: endereço BASE do digest ou do agente informado."""
    partition = f"TENANT#{key_component(tenant)}"
    if agent is not None:
        partition += f"#AGENT#{key_component(agent)}"
    return {"pk": {"S": partition}, "sk": {"S": f"REFRESH_TOKEN#{key_component(digest)}"}}


def ttl_seconds(deadline: datetime) -> int:
    """Returns: teto inteiro em segundos UTC, inclusive antes do epoch."""
    elapsed = deadline - datetime(1970, 1, 1, tzinfo=UTC)
    return elapsed.days * 86400 + elapsed.seconds + bool(elapsed.microseconds)


def encode_record(record: RefreshTokenRecord, projection: Projection) -> Item:
    """Returns: projeção completa schema1. Raises: ValueError para versão fora do storage."""
    require(record.version <= MAX_VERSION)
    agent = record.agent_id if projection == "AGENT" else None
    item = refresh_key(record.tenant_id, record.token_sha256, agent)
    item.update(
        entity={"S": "REFRESH_TOKEN"},
        schema_version={"N": "1"},
        projection={"S": projection},
        token_sha256={"S": record.token_sha256},
        tenant_id={"S": record.tenant_id},
        agent_id={"S": record.agent_id},
        machine_fingerprint={"S": record.machine_fingerprint},
        issued_at=_timestamp(record.issued_at),
        token_expires_at=_timestamp(record.expires_at),
        last_used_at=_timestamp(record.last_used_at),
        revoked_at=_timestamp(record.revoked_at),
        version={"N": str(record.version)},
        expires_at={"N": str(ttl_seconds(record.expires_at))},
    )
    return item


def decode_record(value: object, key: Item, projection: Projection) -> RefreshTokenRecord:
    """Returns: registro validado. Raises: ValueError/Invalid para projeção corrompida."""
    item = mapping(value)
    require(_text(item, "pk") == key["pk"]["S"] and _text(item, "sk") == key["sk"]["S"])
    require(_text(item, "entity") == "REFRESH_TOKEN")
    require(_text(item, "projection") == projection and _integer(item, "schema_version") == 1)
    record = RefreshTokenRecord(
        token_sha256=_text(item, "token_sha256"),
        agent_id=_text(item, "agent_id"),
        tenant_id=_text(item, "tenant_id"),
        machine_fingerprint=_text(item, "machine_fingerprint"),
        issued_at=datetime.fromisoformat(_text(item, "issued_at")),
        expires_at=datetime.fromisoformat(_text(item, "token_expires_at")),
        last_used_at=_optional_time(item, "last_used_at"),
        revoked_at=_optional_time(item, "revoked_at"),
        version=_integer(item, "version"),
    )
    validate_refresh_token(record)
    require(record.version <= MAX_VERSION)
    require(_integer(item, "expires_at") == ttl_seconds(record.expires_at))
    agent = record.agent_id if projection == "AGENT" else None
    require(refresh_key(record.tenant_id, record.token_sha256, agent) == key)
    return record


def mapping(value: object) -> Mapping[str, Any]:
    """Returns: mapa recebido. Raises: ValueError para envelope inválido."""
    require(isinstance(value, Mapping))
    return cast("Mapping[str, Any]", value)


def require(valid: bool) -> None:
    """Raises: ValueError para campo ou envelope inconsistente."""
    if not valid:
        raise ValueError("field=refresh_storage")


def _timestamp(value: datetime | None) -> dict[str, str | bool]:
    if value is None:
        return {"NULL": True}
    return {"S": value.astimezone(UTC).isoformat(timespec="microseconds")}


def _text(item: Mapping[str, Any], name: str, kind: str = "S") -> str:
    attribute = mapping(item[name])
    require(set(attribute) == {kind} and isinstance(attribute[kind], str))
    return cast("str", attribute[kind])


def _integer(item: Mapping[str, Any], name: str) -> int:
    value = _text(item, name, "N")
    require(fullmatch(r"-?(?:0|[1-9][0-9]*)", value) is not None)
    return int(value)


def _optional_time(item: Mapping[str, Any], name: str) -> datetime | None:
    attribute = mapping(item[name])
    if set(attribute) == {"NULL"} and attribute["NULL"] is True:
        return None
    return datetime.fromisoformat(_text(item, name))
