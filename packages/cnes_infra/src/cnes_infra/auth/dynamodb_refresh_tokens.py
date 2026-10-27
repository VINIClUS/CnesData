"""Criação, importação e validação de refresh em pares DynamoDB."""

from collections.abc import Callable, Iterator, Mapping
from datetime import UTC
from hashlib import sha256
from typing import Any, cast
from uuid import uuid4

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from cnes_domain.refresh_tokens import (
    RefreshTokenConflict,
    RefreshTokenContext,
    RefreshTokenInput,
    RefreshTokenMutation,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
    validate_refresh_token,
    validate_refresh_token_agent_context,
    validate_refresh_token_context,
    validate_refresh_token_mutation,
)
from cnes_infra.auth.dynamodb_refresh_codec import (
    MAX_VERSION,
    Item,
    Projection,
    decode_record,
    encode_record,
    mapping,
    refresh_key,
    require,
)
from cnes_infra.control_plane.dynamodb_keys import key_component

type WirePair = tuple[Item, Item]
_GUARDED_FIELDS = (
    "entity",
    "schema_version",
    "projection",
    "token_sha256",
    "tenant_id",
    "agent_id",
    "machine_fingerprint",
    "issued_at",
    "token_expires_at",
    "last_used_at",
    "revoked_at",
    "version",
    "expires_at",
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

    def mark_used(self, token: str, mutation: RefreshTokenMutation) -> bool:
        """Returns: uso confirmado por CAS. Raises: Invalid, Unavailable."""
        return self._mutate(token, mutation, False)

    def has_active_for_agent(self, agent_id: str, context: RefreshTokenContext) -> bool:
        """Returns: existência de par ativo. Raises: Invalid, Unavailable."""
        validate_refresh_token_agent_context(agent_id, context)
        try:
            active = False
            for discovery in self._agent_candidates(agent_id, context):
                record = self._snapshot(discovery)
                if (
                    record.revoked_at is None
                    and record.issued_at <= context.now < record.expires_at
                ):
                    active = True
            return active
        except Exception:
            raise RefreshTokenUnavailable("operation=has_active_refresh") from None

    def _agent_candidates(
        self, agent_id: str, context: RefreshTokenContext
    ) -> Iterator[RefreshTokenRecord]:
        partition = f"TENANT#{key_component(context.tenant_id)}#AGENT#{key_component(agent_id)}"
        request: dict[str, Any] = {
            "TableName": self._table_name,
            "KeyConditionExpression": "#pk = :pk AND begins_with(#sk, :prefix)",
            "ExpressionAttributeNames": {"#pk": "pk", "#sk": "sk"},
            "ExpressionAttributeValues": {
                ":pk": {"S": partition},
                ":prefix": {"S": "REFRESH_TOKEN#"},
            },
            "ConsistentRead": True,
        }
        previous = ""
        while True:
            response = _success(self._client.query(**request))
            require(isinstance(response["Items"], list))
            for item in cast("list[object]", response["Items"]):
                key: Item = {"pk": {"S": partition}, "sk": mapping(item)["sk"]}
                record = decode_record(item, key, "AGENT")
                require(record.tenant_id == context.tenant_id and record.agent_id == agent_id)
                yield record
            cursor = _agent_cursor(response.get("LastEvaluatedKey", {}), partition, previous)
            if not cursor:
                return
            request["ExclusiveStartKey"] = cursor
            previous = cast("str", cursor["sk"]["S"])

    def revoke(self, token: str, mutation: RefreshTokenMutation) -> bool:
        """Returns: revogação confirmada por CAS. Raises: Invalid, Unavailable."""
        return self._mutate(token, mutation, True)

    def _mutate(self, token: str, mutation: RefreshTokenMutation, revoking: bool) -> bool:
        validate_refresh_token_mutation(token, mutation)
        try:
            discovery = self._discover(mutation.tenant_id, sha256(token.encode()).hexdigest())
            if discovery is None:
                return False
            record, items = self._snapshot_wire(discovery)
            if not _eligible_mutation(record, mutation, revoking):
                return False
            require(record.version < MAX_VERSION)
            actions = [self._update(item, mutation, revoking) for item in items]
            try:
                _success(
                    self._client.transact_write_items(
                        TransactItems=actions,
                        ClientRequestToken=str(uuid4()),
                    )
                )
            except ClientError as error:
                require(_conditional_reasons(error) is not None)
                return not self._cas_lost(record)
            return True
        except Exception:
            raise RefreshTokenUnavailable("operation=mutate_refresh") from None

    def _update(self, item: Item, mutation: RefreshTokenMutation, revoking: bool) -> dict[str, Any]:
        names = {f"#g{index}": field for index, field in enumerate(_GUARDED_FIELDS)}
        values = {f":g{index}": item[field] for index, field in enumerate(_GUARDED_FIELDS)}
        names.update(
            {"#changed": "revoked_at" if revoking else "last_used_at", "#version": "version"}
        )
        values[":changed"] = {"S": mutation.now.astimezone(UTC).isoformat(timespec="microseconds")}
        values[":version"] = {"N": str(mutation.expected_version + 1)}
        return {
            "Update": {
                "TableName": self._table_name,
                "Key": {"pk": item["pk"], "sk": item["sk"]},
                "UpdateExpression": "SET #changed = :changed, #version = :version",
                "ConditionExpression": " AND ".join(
                    f"#g{index} = :g{index}" for index in range(len(_GUARDED_FIELDS))
                ),
                "ExpressionAttributeNames": names,
                "ExpressionAttributeValues": values,
            }
        }

    def _cas_lost(self, before: RefreshTokenRecord) -> bool:
        current = self._snapshot(before)
        require(_immutable_values(current) == _immutable_values(before))
        require(current.version > before.version)
        return True

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
        return self._snapshot_wire(discovery)[0]

    def _snapshot_wire(self, discovery: RefreshTokenRecord) -> tuple[RefreshTokenRecord, WirePair]:
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
        items = cast("WirePair", (mapping(slots[0])["Item"], mapping(slots[1])["Item"]))
        digest = decode_record(items[0], keys[0], "DIGEST")
        agent = decode_record(items[1], keys[1], "AGENT")
        require(digest == agent and digest.agent_id == discovery.agent_id)
        return digest, items

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


def _agent_cursor(value: object, partition: str, previous: str) -> Item:
    cursor = mapping(value)
    if not cursor:
        return {}
    require(set(cursor) == {"pk", "sk"})
    for component in cursor.values():
        attribute = mapping(component)
        require(set(attribute) == {"S"})
        require(isinstance(attribute["S"], str) and bool(attribute["S"]))
    require(cursor["pk"]["S"] == partition)
    sort = cast("str", cursor["sk"]["S"])
    require(sort.startswith("REFRESH_TOKEN#") and len(sort) > len("REFRESH_TOKEN#"))
    require(sort.encode("utf-8") > previous.encode("utf-8"))
    return cast("Item", cursor)


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


def _eligible_mutation(
    record: RefreshTokenRecord,
    mutation: RefreshTokenMutation,
    revoking: bool,
) -> bool:
    if record.version != mutation.expected_version or record.revoked_at is not None:
        return False
    if not record.issued_at <= mutation.now < record.expires_at:
        return False
    return revoking or record.last_used_at is None or mutation.now >= record.last_used_at


def _immutable_values(record: RefreshTokenRecord) -> tuple[object, ...]:
    return (
        record.token_sha256,
        record.tenant_id,
        record.agent_id,
        record.machine_fingerprint,
        record.issued_at,
        record.expires_at,
    )
