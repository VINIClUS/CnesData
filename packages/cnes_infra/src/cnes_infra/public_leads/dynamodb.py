"""Persistência global de leads públicos em DynamoDB."""

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError

from cnes_domain.public_leads import (
    STRING_LIMITS,
    PublicLead,
    PublicLeadConflict,
    PublicLeadInput,
    PublicLeadUnavailable,
    validate_lead_input,
    validate_lead_record,
)

_Item = dict[str, dict[str, Any]]


def _key(lead_id: UUID) -> _Item:
    return {"pk": {"S": "PUBLIC_LEADS"}, "sk": {"S": f"LEAD#{lead_id.hex}"}}


def _encode(lead: PublicLead) -> _Item:
    values = asdict(lead)
    del values["lead_id"]
    values["id"] = str(lead.lead_id)
    values["created_at"] = lead.created_at.isoformat()
    return {
        **_key(lead.lead_id),
        "entity_type": {"S": "PUBLIC_LEAD"},
        **{
            name: {"BOOL" if isinstance(value, bool) else "S": value}
            for name, value in values.items()
        },
    }


def _decode(item: _Item, lead_id: UUID) -> PublicLead:
    identity = {**_key(lead_id), "entity_type": {"S": "PUBLIC_LEAD"}}
    if any(item[name] != value for name, value in identity.items()):
        raise PublicLeadUnavailable
    record = PublicLead(
        lead_id=UUID(item["id"]["S"]),
        created_at=datetime.fromisoformat(item["created_at"]["S"]),
        newsletter_opt_in=item["newsletter_opt_in"]["BOOL"],
        **{name: item[name]["S"] for name in STRING_LIMITS},
    )
    validate_lead_record(record)
    if record.lead_id != lead_id:
        raise PublicLeadUnavailable
    return record


@dataclass(slots=True)
class DynamoDBPublicLeadStore:
    client: Any
    table_name: str
    clock: Callable[[], datetime]
    uuid_factory: Callable[[], UUID]

    def create(self, lead: PublicLeadInput) -> PublicLead:
        """Args: lead: Campos do novo lead.
        Returns: Registro persistido com identidade e instante UTC.
        Raises: PublicLeadUnavailable: Entrada inválida ou falha de persistência.
        """
        validate_lead_input(lead)
        record = PublicLead(self.uuid_factory(), self.clock(), **asdict(lead))
        validate_lead_record(record)
        try:
            self._insert(record)
        except PublicLeadConflict:
            raise PublicLeadUnavailable from None
        return record

    def import_record(self, lead: PublicLead) -> None:
        """Args: lead: Registro cuja identidade e instante serão preservados.
        Raises: PublicLeadConflict: Identidade já ocupada.
            PublicLeadUnavailable: Registro inválido ou falha de persistência.
        """
        validate_lead_record(lead)
        self._insert(lead)

    def readback(self, lead_id: UUID) -> PublicLead | None:
        """Args: lead_id: Identidade procurada no armazenamento.
        Returns: Registro obtido por leitura forte ou None quando ausente.
        Raises: PublicLeadUnavailable: Falha de leitura ou registro inválido.
        """
        try:
            response = self.client.get_item(
                TableName=self.table_name, Key=_key(lead_id), ConsistentRead=True
            )
            item = response.get("Item")
            return None if item is None else _decode(item, lead_id)
        except (BotoCoreError, ClientError, ValueError, TypeError, KeyError):
            raise PublicLeadUnavailable from None

    def _insert(self, lead: PublicLead) -> None:
        try:
            self.client.put_item(
                TableName=self.table_name,
                Item=_encode(lead),
                ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                raise PublicLeadConflict from None
            raise PublicLeadUnavailable from None
        except BotoCoreError:
            raise PublicLeadUnavailable from None
