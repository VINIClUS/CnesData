"""Tenant do enqueue admin vem do corpo validado, não do header."""
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from central_api.app import create_app
from central_api.deps import get_engine
from cnes_domain.tenant import tenant_id_ctx


def test_enqueue_define_tenant_do_corpo_antes_de_gravar() -> None:
    app = create_app()
    app.dependency_overrides[get_engine] = lambda: MagicMock()
    seen: list[str | None] = []

    def _enqueue(*_args, **_kwargs):
        seen.append(tenant_id_ctx.get(None))
        return uuid4()

    token = tenant_id_ctx.set("000000")
    try:
        with patch("central_api.routes.extractions.extractions_repo.enqueue", _enqueue):
            resp = TestClient(app).post(
                "/api/v1/extractions/enqueue",
                json={"source_type": "CNES_LOCAL", "tenant_id": "354130",
                      "competencia": "2026-02-01"},
                headers={"X-Admin-Token": "test-admin", "X-Tenant-Id": "999999"},
            )
    finally:
        tenant_id_ctx.reset(token)
    assert resp.status_code == 201
    assert seen == ["354130"]
