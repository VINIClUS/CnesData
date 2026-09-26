from hashlib import sha256

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from central_api.deps import local_edge_identity


def _request(headers: dict[str, str]) -> Request:
    return Request({
        "type": "http", "method": "GET", "path": "/api/v1/edge/jobs/next",
        "headers": [(name.lower().encode(), value.encode()) for name, value in headers.items()],
    })


def test_local_raw_exige_token_proprio(monkeypatch) -> None:
    monkeypatch.setenv("RAW_LOCAL_TOKEN", "raw-secret")
    monkeypatch.setenv("TENANT_ID", "354130")

    with pytest.raises(HTTPException) as error:
        local_edge_identity(_request({"X-Raw-Agent-Id": "agent-1"}))
    assert error.value.status_code == 401

    identity = local_edge_identity(_request({
        "X-Raw-Token": "raw-secret", "X-Raw-Agent-Id": "agent-1",
    }))
    assert identity.tenant_id == "354130"
    assert identity.agent_id == "agent-1"
    assert identity.certificate_fingerprint == sha256(
        b"local:354130:agent-1:raw-secret"
    ).hexdigest()
