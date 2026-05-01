"""Stub repos raise NotImplementedError (deferred to future SIHD work)."""
from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from cnes_contracts.fatos import Internacao, ProcedimentoAIH
from cnes_infra.storage.repositories import (
    internacao_repo,
    procedimento_aih_repo,
)


def test_internacao_levanta_notimplemented():
    i = Internacao(
        num_aih="1234567890123",
        sk_estabelecimento=1,
        sk_competencia=1,
        dt_internacao=date(2026, 1, 1),
        job_id=uuid4(),
        extracao_ts=datetime.now(UTC),
    )
    with pytest.raises(NotImplementedError, match="SIHD"):
        internacao_repo.gravar(MagicMock(), i)


def test_procedimento_aih_levanta_notimplemented():
    pa = ProcedimentoAIH(
        sk_aih=1,
        sk_procedimento=1,
        sk_competencia=1,
        qtd=1,
        job_id=uuid4(),
        extracao_ts=datetime.now(UTC),
    )
    with pytest.raises(NotImplementedError, match="SIHD"):
        procedimento_aih_repo.gravar(MagicMock(), pa)
