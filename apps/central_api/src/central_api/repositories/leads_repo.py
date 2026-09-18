"""Persistence for public contact-form leads (marketing.leads)."""
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine

_INSERT_LEAD = text("""
    INSERT INTO marketing.leads (
        name, email, interest, organization, municipality, role, message,
        source_path, source_cta, privacy_notice_version, newsletter_opt_in
    ) VALUES (
        :name, :email, :interest, :organization, :municipality, :role, :message,
        :source_path, :source_cta, :privacy_notice_version, :newsletter_opt_in
    )
    RETURNING id
""")


@dataclass(frozen=True)
class LeadRecord:
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


class LeadsRepo:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def create(self, lead: LeadRecord) -> UUID:
        with self._engine.begin() as conn:
            row = conn.execute(_INSERT_LEAD, lead.__dict__).mappings().one()
        return row["id"]
