"""Porta de armazenamento de leads públicos."""

from typing import Protocol
from uuid import UUID

from cnes_domain.public_leads import PublicLead, PublicLeadInput


class PublicLeadStore(Protocol):
    def create(self, lead: PublicLeadInput) -> PublicLead:  # pragma: no cover - Protocol stub
        """Args: lead: Campos do novo lead.
        Returns: Registro persistido com identidade e instante UTC.
        Raises: PublicLeadUnavailable: Entrada inválida ou falha de persistência.
        """
        ...

    def import_record(self, lead: PublicLead) -> None:  # pragma: no cover - Protocol stub
        """Args: lead: Registro cuja identidade e instante serão preservados.
        Raises: PublicLeadConflict: Identidade já ocupada.
            PublicLeadUnavailable: Registro inválido ou falha de persistência.
        """
        ...

    def readback(self, lead_id: UUID) -> PublicLead | None:  # pragma: no cover - Protocol stub
        """Args: lead_id: Identidade do registro persistido.
        Returns: Registro lido ou None quando ausente.
        Raises: PublicLeadUnavailable: Falha de leitura ou registro inválido.
        """
        ...
