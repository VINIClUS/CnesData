"""Criação, importação e validação de refresh em pares DynamoDB."""

from collections.abc import Callable, Mapping
from hashlib import sha256
from typing import Any, cast
from uuid import uuid4

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from cnes_domain.refresh_tokens import (
    RefreshTokenConflict,
    RefreshTokenContext,
    RefreshTokenInput,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
    validate_refresh_token,
    validate_refresh_token_context,
)
from cnes_infra.auth.dynamodb_refresh_codec import (
    Item,
    Projection,
    decode_record,
    encode_record,
    mapping,
    refresh_key,
    require,
)


class DynamoDBRefreshTokenStore:
    def __init__(
        self,
        client: BaseClient,
        table_name: str,
        token_factory: Callable[[], str],
    ) -> None:
        self._client = cast("Any", client)
        self._table_name = table_name
        self._token_factory = token_factory

    def create(self, token_input: RefreshTokenInput) -> str:
        """Returns: segredo criado. Raises: Invalid, Conflict, Unavailable."""
        validate_refresh_token(token_input)
        token, digest = self._create_secret()
        record = RefreshTokenRecord(
            token_sha256=digest,
            agent_id=token_input.agent_id,
            tenant_id=token_input.tenant_id,
            machine_fingerprint=token_input.machine_fingerprint,
            issued_at=token_input.issued_at,
            expires_at=token_input.expires_at,
            last_used_at=None,
            revoked_at=None,
            version=0,
        )
        self._insert(record)
        return token

    def import_hashed(self, record: RefreshTokenRecord) -> None:
        """Raises: Invalid, Conflict, Unavailable para importação recusada."""
        validate_refresh_token(record)
        self._insert(record)

    def validate(self, token: str, context: RefreshTokenContext) -> RefreshTokenRecord | None:
        """Returns: registro ativo ou None. Raises: Invalid, Unavailable."""
        validate_refresh_token_context(token, context)
        try:
            digest = sha256(token.encode()).hexdigest()
            record = self._discover(context.tenant_id, digest)
            if record is None:
                return None
            record = self._snapshot(record)
        except Exception:
            raise RefreshTokenUnavailable("operation=validate_refresh") from None
        if record.revoked_at is not None or not record.issued_at <= context.now < record.expires_at:
            return None
        return record

    def _create_secret(self) -> tuple[str, str]:
        try:
            token = self._token_factory()
            require(isinstance(cast("object", token), str) and bool(token))
            return token, sha256(token.encode()).hexdigest()
        except Exception:
            raise RefreshTokenUnavailable("operation=create_secret") from None

    def _insert(self, record: RefreshTokenRecord) -> None:
        occupied = False
        try:
            actions = [self._put(record, projection) for projection in ("DIGEST", "AGENT")]
            try:
                _success(
                    self._client.transact_write_items(
                        TransactItems=actions,
                        ClientRequestToken=str(uuid4()),
                    )
                )
            except ClientError as error:
                reasons = _conditional_reasons(error)
                require(reasons is not None)
                occupied = self._occupied(record, cast("list[str]", reasons))
        except Exception:
            raise RefreshTokenUnavailable("operation=insert_refresh") from None
        if occupied:
            raise RefreshTokenConflict("operation=insert_refresh") from None

    def _put(self, record: RefreshTokenRecord, projection: Projection) -> dict[str, Any]:
        return {
            "Put": {
                "TableName": self._table_name,
                "Item": encode_record(record, projection),
                "ConditionExpression": "attribute_not_exists(pk) AND attribute_not_exists(sk)",
            }
        }

    def _read(self, key: Item, projection: Projection) -> RefreshTokenRecord | None:
        response = _success(
            self._client.get_item(
                TableName=self._table_name,
                Key=key,
                ConsistentRead=True,
            )
        )
        if "Item" not in response:
            return None
        return decode_record(response["Item"], key, projection)

    def _discover(self, tenant: str, digest: str) -> RefreshTokenRecord | None:
        return self._read(refresh_key(tenant, digest), "DIGEST")

    def _snapshot(self, discovery: RefreshTokenRecord) -> RefreshTokenRecord:
        keys = [
            refresh_key(discovery.tenant_id, discovery.token_sha256),
            refresh_key(discovery.tenant_id, discovery.token_sha256, discovery.agent_id),
        ]
        response = _success(
            self._client.transact_get_items(
                TransactItems=[{"Get": {"TableName": self._table_name, "Key": key}} for key in keys]
            )
        )
        require(isinstance(response["Responses"], list))
        slots = cast("list[object]", response["Responses"])
        require(len(slots) == 2)
        digest = decode_record(mapping(slots[0])["Item"], keys[0], "DIGEST")
        agent = decode_record(mapping(slots[1])["Item"], keys[1], "AGENT")
        require(digest == agent and digest.agent_id == discovery.agent_id)
        return digest

    def _occupied(self, requested: RefreshTokenRecord, reasons: list[str]) -> bool:
        discovery = self._discover(requested.tenant_id, requested.token_sha256)
        require(discovery is not None)
        addressed = self._read(
            refresh_key(requested.tenant_id, requested.token_sha256, requested.agent_id),
            "AGENT",
        )
        occupied = self._snapshot(cast("RefreshTokenRecord", discovery))
        if requested.agent_id == occupied.agent_id or reasons[1] == "ConditionalCheckFailed":
            require(addressed == occupied)
        else:
            require(addressed is None)
        return True


def _success(value: object) -> Mapping[str, Any]:
    response = mapping(value)
    metadata = mapping(response["ResponseMetadata"])
    require(type(metadata["HTTPStatusCode"]) is int and metadata["HTTPStatusCode"] == 200)
    require(isinstance(metadata["RequestId"], str) and bool(metadata["RequestId"]))
    return response


def _conditional_reasons(error: ClientError) -> list[str] | None:
    response = mapping(error.response)
    if mapping(response["Error"])["Code"] != "TransactionCanceledException":
        return None
    reasons = response.get("CancellationReasons")
    if not isinstance(reasons, list) or len(cast("list[object]", reasons)) != 2:
        return None
    codes = [mapping(reason)["Code"] for reason in cast("list[object]", reasons)]
    if any(code not in ("None", "ConditionalCheckFailed") for code in codes):
        return None
    return codes if "ConditionalCheckFailed" in codes else None
