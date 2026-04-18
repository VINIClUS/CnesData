"""Testes async para worker/consumer.py — poll loop, heartbeat e execute_job."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest


class TestAcquireJob:
    async def test_retorna_none_em_204(self):
        from dump_agent.worker.consumer import _acquire_job

        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch("dump_agent.worker.consumer.httpx.AsyncClient", return_value=mock_client):
            result = await _acquire_job("http://api", "m1")

        assert result is None

    async def test_retorna_dict_em_200(self):
        from dump_agent.worker.consumer import _acquire_job

        payload = {"job_id": "j1", "upload_url": "http://s3/x", "object_key": "k1"}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = payload
        mock_resp.raise_for_status = MagicMock()
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch("dump_agent.worker.consumer.httpx.AsyncClient", return_value=mock_client):
            result = await _acquire_job("http://api", "m1")

        assert result == payload

    async def test_levanta_em_5xx(self):
        from dump_agent.worker.consumer import _acquire_job

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=mock_resp,
        )
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with patch("dump_agent.worker.consumer.httpx.AsyncClient", return_value=mock_client):
            with pytest.raises(httpx.HTTPStatusError):
                await _acquire_job("http://api", "m1")


class TestHeartbeatLoop:
    async def test_envia_heartbeat_e_cancela(self):
        from dump_agent.worker.consumer import _heartbeat_loop

        mock_resp = MagicMock()
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_resp)

        with (
            patch("dump_agent.worker.consumer.httpx.AsyncClient", return_value=mock_client),
            patch("dump_agent.worker.consumer._HEARTBEAT_INTERVAL", 0.01),
        ):
            task = asyncio.create_task(_heartbeat_loop("http://api", "j1", "m1"))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert mock_client.post.await_count >= 1

    async def test_heartbeat_falha_sem_propagar(self):
        from dump_agent.worker.consumer import _heartbeat_loop

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=httpx.ConnectError("err"))

        with (
            patch("dump_agent.worker.consumer.httpx.AsyncClient", return_value=mock_client),
            patch("dump_agent.worker.consumer._HEARTBEAT_INTERVAL", 0.01),
        ):
            task = asyncio.create_task(_heartbeat_loop("http://api", "j1", "m1"))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert mock_client.post.await_count >= 1


class TestExecuteJob:
    def _job_data(self, extra: dict | None = None) -> dict:
        base = {
            "job_id": "j1",
            "upload_url": "http://s3/x",
            "object_key": "key1",
            "tenant_id": "t1",
            "extraction_params": {
                "intent": "profissionais",
                "competencia": "2026-03",
                "cod_municipio": "354130",
            },
        }
        if extra:
            base.update(extra)
        return base

    async def test_executa_job_com_sucesso(self):
        from dump_agent.worker.consumer import _execute_job

        mock_complete_resp = MagicMock()
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(return_value=mock_complete_resp)

        mock_con = MagicMock()

        with (
            patch("dump_agent.worker.consumer.httpx.AsyncClient", return_value=mock_client),
            patch("dump_agent.worker.consumer.set_tenant_id"),
            patch("dump_agent.worker.connection.conectar_firebird", return_value=mock_con),
            patch("dump_agent.worker.streaming_executor.stream_to_storage"),
        ):
            loop = asyncio.get_event_loop()
            with patch.object(loop, "run_in_executor", new_callable=AsyncMock) as mock_exec:
                mock_exec.return_value = mock_con
                await _execute_job("http://api", "m1", self._job_data())

        mock_client.post.assert_awaited_once()

    async def test_rejeita_params_invalidos_sem_propagar(self):
        from dump_agent.worker.consumer import _execute_job

        bad_job = self._job_data()
        bad_job["extraction_params"] = {"intent": "INVALIDO"}

        await _execute_job("http://api", "m1", bad_job)

    async def test_captura_exception_de_conexao(self):
        from dump_agent.worker.consumer import _execute_job

        with (
            patch("dump_agent.worker.consumer.set_tenant_id"),
            patch("dump_agent.worker.consumer.asyncio.get_running_loop") as mock_loop,
        ):
            mock_loop_inst = AsyncMock()
            mock_loop_inst.run_in_executor = AsyncMock(
                side_effect=OSError("db_connection_failed"),
            )
            mock_loop.return_value = mock_loop_inst
            await _execute_job("http://api", "m1", self._job_data())


class TestRunWorkerJobPath:
    async def test_executa_job_e_continua_loop(self):
        from dump_agent.worker.consumer import run_worker

        job = {
            "job_id": "j1",
            "upload_url": "http://s3",
            "object_key": "k",
            "tenant_id": "t1",
            "extraction_params": {
                "intent": "profissionais",
                "competencia": "2026-03",
                "cod_municipio": "354130",
            },
        }
        call_count = 0

        async def mock_acquire(url, mid):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return job
            return None

        stop_event = asyncio.Event()

        with (
            patch("dump_agent.worker.consumer._acquire_job", side_effect=mock_acquire),
            patch("dump_agent.worker.consumer._execute_job", new_callable=AsyncMock),
            patch("dump_agent.worker.consumer._heartbeat_loop", new_callable=AsyncMock),
            patch("dump_agent.worker.consumer.random.uniform", return_value=0.0),
        ):
            async def stop_after():
                await asyncio.sleep(0.1)
                stop_event.set()

            await asyncio.gather(
                run_worker(
                    api_base_url="http://api",
                    machine_id="m1",
                    stop_event=stop_event,
                    jitter_max=0.0,
                ),
                stop_after(),
            )

        assert call_count >= 1


class TestRunWorkerTimingPaths:
    async def test_poll_timeout_continua_loop(self):
        from dump_agent.worker.consumer import run_worker
        call_count = 0

        async def mock_acquire(url, mid):
            nonlocal call_count
            call_count += 1
            return None

        stop_event = asyncio.Event()
        with (
            patch("dump_agent.worker.consumer._acquire_job", side_effect=mock_acquire),
            patch("dump_agent.worker.consumer._POLL_INTERVAL", 0.01),
        ):
            async def stop_after():
                await asyncio.sleep(0.08)
                stop_event.set()

            await asyncio.gather(
                run_worker(
                    api_base_url="http://api",
                    machine_id="m1",
                    stop_event=stop_event,
                    jitter_max=0.0,
                ),
                stop_after(),
            )

        assert call_count >= 2

    async def test_inter_job_jitter_stop_evento_seta_antes_timeout(self):
        from dump_agent.worker.consumer import run_worker

        job = {
            "job_id": "j1",
            "upload_url": "http://s3",
            "object_key": "k",
            "tenant_id": "t1",
            "extraction_params": {
                "intent": "profissionais",
                "competencia": "2026-03",
                "cod_municipio": "354130",
            },
        }

        call_count = 0

        async def mock_acquire(url, mid):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return job
            return None

        stop_event = asyncio.Event()

        with (
            patch("dump_agent.worker.consumer._acquire_job", side_effect=mock_acquire),
            patch("dump_agent.worker.consumer._execute_job", new_callable=AsyncMock),
            patch("dump_agent.worker.consumer._heartbeat_loop", new_callable=AsyncMock),
            patch("dump_agent.worker.consumer.random.uniform", return_value=3600.0),
        ):
            async def stop_after():
                await asyncio.sleep(0.05)
                stop_event.set()

            await asyncio.gather(
                run_worker(
                    api_base_url="http://api",
                    machine_id="m1",
                    stop_event=stop_event,
                    jitter_max=3600.0,
                ),
                stop_after(),
            )

        assert call_count >= 1
