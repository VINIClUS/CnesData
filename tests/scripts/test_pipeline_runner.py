import io
import queue
import subprocess
import sys
import threading
import time
from datetime import date
from unittest.mock import MagicMock, patch

from pipeline_runner import competencia_atual, iniciar_leitor, iniciar_pipeline

_POPEN_SPEC = subprocess.Popen


class TestCompetenciaAtual:
    def test_retorna_ano_e_mes_de_hoje(self):
        hoje = date(2026, 3, 31)
        with patch("pipeline_runner.date") as mock_date:
            mock_date.today.return_value = hoje
            ano, mes = competencia_atual()
        assert ano == 2026
        assert mes == 3


class TestIniciarPipeline:
    def _mock_popen(self, mock_cls, returncode=None):
        mock_cls.return_value = MagicMock(spec=_POPEN_SPEC, returncode=returncode)

    @patch("pipeline_runner.subprocess.Popen")
    def test_sem_flags(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=False, skip_hr=False)
        args = mock_popen.call_args[0][0]
        assert "-c" in args
        assert "2026-03" in args
        assert "--skip-nacional" not in args
        assert "--skip-hr" not in args

    @patch("pipeline_runner.subprocess.Popen")
    def test_skip_nacional(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=True, skip_hr=False)
        args = mock_popen.call_args[0][0]
        assert "--skip-nacional" in args
        assert "--skip-hr" not in args

    @patch("pipeline_runner.subprocess.Popen")
    def test_skip_hr(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=False, skip_hr=True)
        args = mock_popen.call_args[0][0]
        assert "--skip-hr" in args
        assert "--skip-nacional" not in args

    @patch("pipeline_runner.subprocess.Popen")
    def test_ambos_skip(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=True, skip_hr=True)
        args = mock_popen.call_args[0][0]
        assert "--skip-nacional" in args
        assert "--skip-hr" in args

    @patch("pipeline_runner.subprocess.Popen")
    def test_usa_sys_executable(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=False, skip_hr=False)
        args = mock_popen.call_args[0][0]
        assert args[0] == sys.executable

    @patch("pipeline_runner.subprocess.Popen")
    def test_stdout_pipe_stderr_stdout(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=False, skip_hr=False)
        kwargs = mock_popen.call_args[1]
        assert kwargs["stdout"] == subprocess.PIPE
        assert kwargs["stderr"] == subprocess.STDOUT
        assert kwargs["text"] is True


class TestIniciarLeitor:
    def _proc_com_saida(self, linhas: list[str]) -> MagicMock:
        texto = "\n".join(linhas) + "\n"
        proc = MagicMock(spec=subprocess.Popen)
        proc.stdout = io.StringIO(texto)
        return proc

    def test_enfileira_linhas_do_stdout(self):
        proc = self._proc_com_saida(["linha um", "linha dois", "linha três"])
        q = iniciar_leitor(proc)
        time.sleep(0.2)
        linhas = []
        while not q.empty():
            linhas.append(q.get_nowait())
        assert "linha um\n" in linhas
        assert "linha dois\n" in linhas
        assert "linha três\n" in linhas

    def test_thread_e_daemon(self):
        class BlockingStdout:
            def __iter__(self):
                return self

            def __next__(self):
                time.sleep(10)
                raise StopIteration

        proc = MagicMock(spec=subprocess.Popen)
        proc.stdout = BlockingStdout()
        iniciar_leitor(proc)
        time.sleep(0.1)
        threads_daemon = [
            t for t in threading.enumerate() if t.daemon and "leitor" in t.name
        ]
        assert len(threads_daemon) >= 1

    def test_retorna_queue(self):
        proc = self._proc_com_saida([])
        resultado = iniciar_leitor(proc)
        assert isinstance(resultado, queue.Queue)
