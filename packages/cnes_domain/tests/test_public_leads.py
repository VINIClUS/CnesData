"""Testes do contrato de domínio dos leads públicos."""

from dataclasses import FrozenInstanceError, fields
from datetime import UTC, datetime
from inspect import signature
from uuid import UUID

import pytest

from cnes_domain import public_leads as domain
from cnes_domain.ports import public_leads as ports

PublicLead = domain.PublicLead
PublicLeadInput = domain.PublicLeadInput
PublicLeadConflict = domain.PublicLeadConflict
PublicLeadStoreError = domain.PublicLeadStoreError
PublicLeadUnavailable = domain.PublicLeadUnavailable
PublicLeadStore = ports.PublicLeadStore

_FIELDS = {
    "name": "Pessoa Sintética",
    "email": "Pessoa.Sintetica+Leads@EXAMPLE.ORG",
    "interest": "contact",
    "organization": "Org UTF-8 — saúde",
    "municipality": "Cidade",
    "role": "Gestão",
    "message": "Linha 1\nLinha 2",
    "source_path": "/contato",
    "source_cta": "fale conosco",
    "privacy_notice_version": "1",
    "newsletter_opt_in": True,
}
_LEAD_ID = UUID("11111111-1111-4111-8111-111111111111")
_CREATED_AT = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)


def _input(**changes: object) -> PublicLeadInput:
    values = {**_FIELDS, **changes}
    return PublicLeadInput(**values)


def _record(**changes: object) -> PublicLead:
    values = {**_FIELDS, **changes}
    return PublicLead(lead_id=_LEAD_ID, created_at=_CREATED_AT, **values)


def test_input_tem_exatamente_os_onze_campos() -> None:
    assert tuple(field.name for field in fields(PublicLeadInput)) == tuple(_FIELDS)
    assert hasattr(PublicLeadInput, "__slots__")
    assert hasattr(PublicLead, "__slots__")
    assert _input().email == "Pessoa.Sintetica+Leads@EXAMPLE.ORG"
    assert _input(organization="", privacy_notice_version="").organization == ""


def test_lead_acrescenta_uuid_e_timestamp_utc() -> None:
    record = _record()
    assert tuple(field.name for field in fields(PublicLead)) == (
        "lead_id",
        "created_at",
        *_FIELDS,
    )
    assert record.lead_id == _LEAD_ID
    assert record.created_at == _CREATED_AT
    assert record.created_at.utcoffset() == UTC.utcoffset(record.created_at)


def test_modelos_sao_frozen() -> None:
    input_value = _input()
    record = _record()
    replacement = "alterado"
    with pytest.raises(FrozenInstanceError):
        input_value.__setattr__("name", replacement)
    with pytest.raises(FrozenInstanceError):
        record.__setattr__("name", replacement)


def test_input_preserva_valores_sem_validar_ou_normalizar() -> None:
    value = _input(name="   ", interest="other", email="  Mixed@Example.org  ")
    assert value.name == "   "
    assert value.interest == "other"
    assert value.email == "  Mixed@Example.org  "


def test_porta_expoe_somente_create_import_e_readback() -> None:
    assert tuple(signature(PublicLeadStore.create).parameters) == ("self", "lead")
    assert tuple(signature(PublicLeadStore.import_record).parameters) == ("self", "lead")
    assert tuple(signature(PublicLeadStore.readback).parameters) == ("self", "lead_id")
    assert {name for name in dir(PublicLeadStore) if not name.startswith("_")} == {
        "create",
        "import_record",
        "readback",
    }


def test_erros_de_store_sao_neutros_e_codificados() -> None:
    assert issubclass(PublicLeadConflict, PublicLeadStoreError)
    assert issubclass(PublicLeadUnavailable, PublicLeadStoreError)
    conflict = PublicLeadConflict()
    unavailable = PublicLeadUnavailable()
    assert conflict.code == "leads_conflict"
    assert unavailable.code == "leads_unavailable"
    assert str(conflict) == "code=leads_conflict"
    assert str(unavailable) == "code=leads_unavailable"
