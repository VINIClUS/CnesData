"""Verifica que dump_agent envia size_bytes no complete-upload."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_execute_job_envia_size_bytes_no_complete_upload():
    from dump_agent.worker.consumer import _execute_job

    job_data = {
        "job_id": "00000000-0000-0000-0000-000000000001",
        "upload_url": "null://test",
        "object_key": "355030/cnes/abc.parquet.gz",
        "tenant_id": "355030",
        "extraction_params": {
            "intent": "profissionais",
            "competencia": "2026-01",
            "cod_municipio": "355030",
        },
    }

    mock_post = AsyncMock()
    mock_post.return_value = MagicMock(
        status_code=200, raise_for_status=MagicMock(),
    )

    with patch(
        "dump_agent.worker.connection.conectar_firebird",
        return_value=MagicMock(close=MagicMock()),
    ), patch(
        "dump_agent.worker.streaming_executor.stream_to_storage",
        return_value=9999,
    ), patch(
        "dump_agent.worker.consumer.httpx.AsyncClient",
    ) as mock_client:
        ctx = mock_client.return_value.__aenter__.return_value
        ctx.post = mock_post

        await _execute_job("http://api", "machine-1", job_data)

    complete_calls = [
        c for c in mock_post.await_args_list
        if "complete-upload" in str(c)
    ]
    assert len(complete_calls) == 1
    body = complete_calls[0].kwargs["json"]
    assert body["size_bytes"] == 9999
    assert body["machine_id"] == "machine-1"
    assert body["object_key"] == "355030/cnes/abc.parquet.gz"
