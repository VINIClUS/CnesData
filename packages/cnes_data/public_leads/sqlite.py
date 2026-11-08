"""Persistência global de leads públicos em SQLite."""

import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

from cnes_domain.public_leads import (
    PublicLead,
    PublicLeadConflict,
    PublicLeadInput,
    PublicLeadUnavailable,
    validate_lead_input,
    validate_lead_record,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS public_leads (
    id TEXT PRIMARY KEY NOT NULL,
    created_at TEXT NOT NULL,
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    email TEXT NOT NULL CHECK (length(email) BETWEEN 3 AND 254),
    interest TEXT NOT NULL CHECK (interest IN ('early_access', 'pilot', 'contact')),
    organization TEXT NOT NULL CHECK (length(organization) <= 200),
    municipality TEXT NOT NULL CHECK (length(municipality) <= 120),
    role TEXT NOT NULL CHECK (length(role) <= 120),
    message TEXT NOT NULL CHECK (length(message) <= 2000),
    source_path TEXT NOT NULL CHECK (length(source_path) <= 200),
    source_cta TEXT NOT NULL CHECK (length(source_cta) <= 50),
    privacy_notice_version TEXT NOT NULL CHECK (length(privacy_notice_version) <= 10),
    newsletter_opt_in INTEGER NOT NULL CHECK (newsletter_opt_in IN (0, 1))
)
"""
_INSERT = """
INSERT INTO public_leads (
    id, created_at, name, email, interest, organization, municipality, role,
    message, source_path, source_cta, privacy_notice_version, newsletter_opt_in
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _decode(row: sqlite3.Row, lead_id: UUID) -> PublicLead:
    values = dict(row)
    record = PublicLead(
        lead_id=UUID(values.pop("id")),
        created_at=datetime.fromisoformat(values.pop("created_at")),
        newsletter_opt_in=bool(values.pop("newsletter_opt_in")),
        **values,
    )
    validate_lead_record(record)
    if record.lead_id != lead_id:
        raise PublicLeadUnavailable
    return record


@dataclass(slots=True)
class SQLitePublicLeadStore:
    database_path: Path
    clock: Callable[[], datetime]
    uuid_factory: Callable[[], UUID]

    def initialize(self) -> None:
        """Returns: Tabela própria criada de forma idempotente.
        Raises: PublicLeadUnavailable: Falha ao preparar o armazenamento.
        """
        try:
            with closing(sqlite3.connect(self.database_path)) as connection, connection:
                connection.execute(_SCHEMA)
        except sqlite3.Error:
            raise PublicLeadUnavailable from None

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
        Returns: Registro lido de conexão nova ou None quando ausente.
        Raises: PublicLeadUnavailable: Falha de leitura ou registro inválido.
        """
        try:
            with closing(sqlite3.connect(self.database_path)) as connection:
                connection.row_factory = sqlite3.Row
                row = connection.execute(
                    "SELECT * FROM public_leads WHERE id = ?", (str(lead_id),)
                ).fetchone()
            return None if row is None else _decode(row, lead_id)
        except (sqlite3.Error, ValueError, TypeError):
            raise PublicLeadUnavailable from None

    def _insert(self, lead: PublicLead) -> None:
        values = asdict(lead)
        values["lead_id"] = str(lead.lead_id)
        values["created_at"] = lead.created_at.isoformat()
        values["newsletter_opt_in"] = int(lead.newsletter_opt_in)
        try:
            with closing(sqlite3.connect(self.database_path)) as connection, connection:
                connection.execute(_INSERT, tuple(values.values()))
        except sqlite3.Error as error:
            if getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY:
                raise PublicLeadConflict from None
            raise PublicLeadUnavailable from None
