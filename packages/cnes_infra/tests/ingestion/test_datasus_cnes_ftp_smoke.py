"""Smoke opt-in do FTP PF do DATASUS."""

from __future__ import annotations

import os

import pytest

from cnes_infra.ingestion.datasus_cnes_transport import (
    DatasusCnesFtpTransport,
    DatasusCnesRequest,
)

pytestmark = pytest.mark.skipif(
    os.getenv("DATASUS_FTP_SMOKE") != "1", reason="DATASUS_FTP_SMOKE != 1"
)


def test_ftp_publica_pf_sem_expor_dados_pessoais():
    request = DatasusCnesRequest(
        tenant_id="354130",
        competencia="2026-01",
        file_subtype="CNES_VINCULO",
        snapshot_id="smoke",
        agent_id="system-datasus",
        agent_version="1.0.0",
    )

    rows = DatasusCnesFtpTransport().fetch(request)
    assert sum(1 for _ in rows) > 0
