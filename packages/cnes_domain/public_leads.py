"""Modelos do contrato de leads públicos."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import ClassVar
from uuid import UUID


@dataclass(frozen=True, slots=True)
class PublicLeadInput:
    name: str
    email: str
    interest: str
    organization: str
    municipality: str
    role: str
    message: str
    source_path: str
    source_cta: str
    privacy_notice_version: str
    newsletter_opt_in: bool


@dataclass(frozen=True, slots=True)
class PublicLead:
    lead_id: UUID
    created_at: datetime
    name: str
    email: str
    interest: str
    organization: str
    municipality: str
    role: str
    message: str
    source_path: str
    source_cta: str
    privacy_notice_version: str
    newsletter_opt_in: bool


class PublicLeadStoreError(Exception):
    """Erro neutro do contrato de armazenamento de leads."""

    code: ClassVar[str]

    def __init__(self) -> None:
        super().__init__(f"code={self.code}")


class PublicLeadConflict(PublicLeadStoreError):
    """Conflito de identidade durante importação."""

    code = "leads_conflict"


class PublicLeadUnavailable(PublicLeadStoreError):
    """Indisponibilidade do armazenamento de leads."""

    code = "leads_unavailable"


STRING_LIMITS = {
    "name": (1, 120),
    "email": (3, 254),
    "interest": (0, 20),
    "organization": (0, 200),
    "municipality": (0, 120),
    "role": (0, 120),
    "message": (0, 2000),
    "source_path": (0, 200),
    "source_cta": (0, 50),
    "privacy_notice_version": (0, 10),
}


def validate_lead_input(lead: PublicLeadInput | PublicLead) -> None:
    """Args: lead: Campos recebidos para persistência.
    Raises: PublicLeadUnavailable: Campo incompatível com o armazenamento.
    """
    for name, (minimum, maximum) in STRING_LIMITS.items():
        value = getattr(lead, name)
        if not isinstance(value, str) or not minimum <= len(value) <= maximum:
            raise PublicLeadUnavailable
    if lead.interest not in {"early_access", "pilot", "contact"}:
        raise PublicLeadUnavailable
    if type(lead.newsletter_opt_in) is not bool:
        raise PublicLeadUnavailable


def validate_lead_record(lead: PublicLead) -> None:
    """Args: lead: Registro com identidade e instante fornecidos.
    Raises: PublicLeadUnavailable: Identidade, instante UTC ou campos inválidos.
    """
    validate_lead_input(lead)
    _validate_identity(lead.lead_id, lead.created_at)


def _validate_identity(lead_id: object, created_at: object) -> None:
    if not isinstance(lead_id, UUID):
        raise PublicLeadUnavailable
    if not isinstance(created_at, datetime) or created_at.utcoffset() != timedelta(0):
        raise PublicLeadUnavailable
