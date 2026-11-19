"""Lifespan público discrimina despacho da API e preserva o loop do processor local."""

import asyncio
from typing import Any, cast
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from apps.central_api.tests import test_aws_no_outbox as harness
from central_api import deps
from central_api.composition import build_runtime
from packages.cnes_infra.tests.control_plane.outbox_cursor_support import OutboxHarness, evento

_isolar_rede = harness._isolar_rede
aws = harness.aws
oidc = harness.oidc
scheduler = harness.scheduler


def test_lifespan_aws_nao_despacha_com_runtime_completo_isolado(
    aws: harness.AwsBoundary,
    oidc: harness.OidcBoundary,
    scheduler: harness.SchedulerProbe,
    caplog: Any,
) -> None:
    aws.deny_audit = False
    runtime = build_runtime("aws", aws.values, cast("Any", aws))
    aws.deny_audit = True
    aws.calls.clear()
    app = harness.observed_app(scheduler)
    with patch.object(deps, "build_runtime", return_value=runtime), TestClient(app) as client:
        assert client.portal is not None
        client.portal.call(scheduler.barrier)
        harness._check_routes(client, oidc)
        client.portal.call(scheduler.barrier)
    harness.assert_closed(oidc)
    harness.assert_quiet(aws, scheduler, caplog)


@pytest.mark.asyncio
async def test_loop_local_do_processor_entrega_evento_real(tmp_path: Any) -> None:
    from data_processor.main import _audit_tick, _poll_until_shutdown

    harness = OutboxHarness("sqlite", tmp_path)
    event = evento("local-control")
    harness.put(event)
    shutdown = asyncio.Event()

    class Coordinator:
        def recover(self) -> tuple[()]:
            shutdown.set()
            return ()

    await asyncio.wait_for(
        _poll_until_shutdown(
            cast("Any", Coordinator()),
            shutdown,
            interval=0,
            audit_tick=lambda: _audit_tick(harness.adapter, harness.sink()),
        ),
        timeout=5,
    )

    assert harness.stored(event.event_id).delivered_at is not None
    assert harness.local_records() == [event]
