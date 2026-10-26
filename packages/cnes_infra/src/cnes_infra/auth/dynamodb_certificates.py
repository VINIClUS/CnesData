"""Registro de certificados no plano de controle DynamoDB."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from botocore.exceptions import BotoCoreError, ClientError

from cnes_domain.auth.certificates import (
    CertificateConflict,
    CertificateRecord,
    CertificateStoreUnavailable,
    validate_certificate_instant_utc,
)
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.dynamodb_codec import (
    Item,
    decode_model,
    encode_model,
    execute_transaction,
    payload,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import certificate_key, item_key

if TYPE_CHECKING:
    from datetime import datetime

    from botocore.client import BaseClient
    from types_boto3_dynamodb.client import DynamoDBClient

    from cnes_domain.auth.certificates import CertificateKey


class DynamoDBCertificateRegister:
    def __init__(self, client: BaseClient, table_name: str) -> None:
        self._client = cast("DynamoDBClient", client)
        self._table_name = table_name

    def record(self, certificate: CertificateRecord) -> None:
        item = self._encode(certificate)
        try:
            execute_transaction(self._client, (put_action(self._table_name, item, None),))
        except Conflict as error:
            raise CertificateConflict from error
        except (BotoCoreError, ClientError) as error:
            raise CertificateStoreUnavailable from error

    def get(self, key: CertificateKey) -> CertificateRecord | None:
        item = self._read(key)
        if item is None:
            return None
        record = decode_model(item, CertificateRecord)
        return record if record.agent_id == key.agent_id else None

    def is_active(
        self, key: CertificateKey, certificate_fingerprint_sha256: str, now: datetime
    ) -> bool:
        record = self.get(key)
        return (
            record is not None
            and record.certificate_fingerprint_sha256 == certificate_fingerprint_sha256
            and record.revoked_at is None
            and record.issued_at <= now < record.expires_at
        )

    def revoke(self, key: CertificateKey, revoked_at: datetime) -> bool:
        validate_certificate_instant_utc(revoked_at)
        prior_item = self._read(key)
        if prior_item is None:
            return False
        prior = decode_model(prior_item, CertificateRecord)
        if prior.agent_id != key.agent_id or prior.revoked_at is not None:
            return False
        updated = prior.model_copy(update={"revoked_at": revoked_at})
        action = put_action(self._table_name, self._encode(updated), payload(prior_item))
        try:
            execute_transaction(self._client, (action,))
        except Conflict as error:
            current = self.get(key)
            if (
                current is not None
                and current.revoked_at is not None
                and current.model_copy(update={"revoked_at": None}) == prior
            ):
                return False
            raise CertificateStoreUnavailable from error
        except (BotoCoreError, ClientError) as error:
            raise CertificateStoreUnavailable from error
        return True

    def _read(self, key: CertificateKey) -> Item | None:
        try:
            response = self._client.get_item(
                TableName=self._table_name,
                Key=item_key(*certificate_key(key.tenant_id, key.serial)),
                ConsistentRead=True,
            )
        except (BotoCoreError, ClientError) as error:
            raise CertificateStoreUnavailable from error
        return cast("Item | None", response.get("Item"))

    def _encode(self, certificate: CertificateRecord) -> Item:
        return encode_model(
            certificate, "CERTIFICATE", certificate_key(certificate.tenant_id, certificate.serial)
        )
