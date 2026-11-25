"""Sonda do closed-ready: espera o artefato do keeper e expõe o hash, sem criar bytes (P27)."""

from __future__ import annotations

import io
import subprocess
import zipfile
from typing import Any

import pytest
import yaml

from scripts.promotion import _stage_windows as sw
from scripts.promotion import closed_ready_probe as probe
from scripts.promotion._closed_ready import object_hash
from tests.production.delivery.test_promote_production_v2 import WORKFLOW

READY = {"version": 1, "kind": "closed-ready", "members": []}
MEMBERS = {"close-drain.json": {"kind": "close-drain"}, "phase-b.json": {"kind": "phase-b"},
           "closed-ready.json": READY}


def _zip(members: dict[str, Any]) -> bytes:
    import json

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, value in members.items():
            archive.writestr(name, json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    return buffer.getvalue()


class Api:
    def __init__(self, *, appears_at: int = 0, members: dict[str, Any] | None = None,
                 application: str = "in_progress", extra: int = 0) -> None:
        self.calls = 0
        self.job_reads = 0
        self.appears_at = appears_at
        self.members = MEMBERS if members is None else members
        self.application = application
        self.extra = extra

    def artifacts(self, name: str) -> list[dict[str, Any]]:
        self.calls += 1
        assert name == "cnesdata-closed-ready-7-1"
        if self.calls <= self.appears_at:
            return []
        return [{"id": 9 + i, "name": name} for i in range(1 + self.extra)]

    def jobs(self) -> list[dict[str, Any]]:
        self.job_reads += 1
        return [{"name": "application / execute", "status": self.application}]

    def archive(self, artifact_id: int) -> bytes:
        assert artifact_id == 9
        return _zip(self.members)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _run(api: Api, clock: Clock | None = None, deadline: int = probe.TIMEOUT_SECONDS) -> str:
    clock = clock or Clock()
    return probe.wait_for_closed_ready(
        api, probe.Target(7, 1, "application / execute"),
        probe.Clock(sleep=clock.sleep, monotonic=clock.monotonic), deadline)


def test_devolve_o_hash_canonico_do_closed_ready_sem_criar_bytes() -> None:
    assert _run(Api()) == object_hash(READY)


def test_espera_a_cada_cinco_segundos_ate_o_keeper_publicar() -> None:
    clock = Clock()
    assert _run(Api(appears_at=3), clock) == object_hash(READY)
    assert clock.sleeps == [5, 5, 5]


@pytest.mark.parametrize("members", [
    {"close-drain.json": {}, "phase-b.json": {}},
    {**MEMBERS, "extra.json": {}},
    {**MEMBERS, "closed-ready.json": {"kind": "phase-b"}},
], ids=["falta-membro", "membro-a-mais", "tipo-errado"])
def test_recusa_artefato_fora_dos_tres_membros_exatos(members: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=r"^closed_ready_probe=members$"):
        _run(Api(members=members))


def test_recusa_mais_de_um_artefato_com_o_mesmo_nome() -> None:
    with pytest.raises(ValueError, match=r"^closed_ready_probe=artifact_count$"):
        _run(Api(extra=1))


def test_sai_com_falha_se_o_application_concluiu_sem_o_artefato() -> None:
    with pytest.raises(ValueError, match=r"^closed_ready_probe=application_concluded$"):
        _run(Api(appears_at=99, application="completed"))


def test_sai_com_falha_no_prazo_da_sonda() -> None:
    with pytest.raises(ValueError, match=r"^closed_ready_probe=deadline$"):
        _run(Api(appears_at=10**6), deadline=20)


def test_prazo_cobre_a_soma_maxima_das_janelas_mais_reserva_e_margem() -> None:
    assert probe.TIMEOUT_SECONDS >= sw.STAGE_SUM_CEILING + sw.FINAL_RESERVE + 300
    job = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]["closed-ready"]
    assert job["timeout-minutes"] * 60 >= probe.APPROVAL_CAP_SECONDS + probe.TIMEOUT_SECONDS


def test_reler_o_artefato_depois_de_ver_o_application_concluido() -> None:
    api = Api(appears_at=1, application="completed")
    assert _run(api) == object_hash(READY)
    assert api.calls == 2
    assert api.job_reads == 1


def test_nao_declara_application_concluido_se_o_artefato_ainda_nao_existe() -> None:
    api = Api(appears_at=99, application="completed")
    with pytest.raises(ValueError, match=r"^closed_ready_probe=application_concluded$"):
        _run(api)
    assert api.calls == 2


class Run:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def __call__(self, argv: list[str], **kwargs: Any) -> Any:
        self.calls += 1
        if self.calls <= self.failures:
            raise subprocess.CalledProcessError(1, argv)
        return subprocess.CompletedProcess(argv, 0, stdout=b'{"artifacts":[]}')


def test_falha_transitoria_do_gh_api_e_repetida_ate_o_limite() -> None:
    clock = Clock()
    run = Run(probe.API_ATTEMPTS - 1)
    api = probe.GhApi("VINIClUS/CnesData", 7, 1, run=run, sleep=clock.sleep)
    assert api.artifacts("cnesdata-closed-ready-7-1") == []
    assert run.calls == probe.API_ATTEMPTS
    assert clock.sleeps == [probe.API_RETRY_SECONDS] * (probe.API_ATTEMPTS - 1)


def test_falha_persistente_do_gh_api_vira_key_value_sem_traceback() -> None:
    run = Run(10**6)
    api = probe.GhApi("VINIClUS/CnesData", 7, 1, run=run, sleep=Clock().sleep)
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.jobs()
    assert run.calls == probe.API_ATTEMPTS


STARTED_AT = "2026-11-25T12:00:00Z"
STARTED_EPOCH = 1_795_608_000.0


class TimedApi(Api):
    def __init__(self, *, states: list[dict[str, Any]], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.states = states

    def jobs(self) -> list[dict[str, Any]]:
        self.job_reads += 1
        return self.states[min(self.job_reads, len(self.states)) - 1 : ][0]["jobs"]


def _state(*jobs: dict[str, Any]) -> dict[str, Any]:
    return {"jobs": list(jobs)}


def _job(status: str = "in_progress", started_at: str | None = STARTED_AT) -> dict[str, Any]:
    job: dict[str, Any] = {"name": "application / execute", "status": status}
    if started_at is not None:
        job["started_at"] = started_at
    return job


def _timed_clock(wall: float) -> tuple[Clock, probe.Clock]:
    clock = Clock()
    return clock, probe.Clock(sleep=clock.sleep, monotonic=clock.monotonic,
                              wall=lambda: wall + clock.now)


def _target() -> probe.Target:
    return probe.Target(7, 1, "application / execute")


def test_espera_o_application_em_andamento_e_devolve_os_segundos_desde_o_started_at() -> None:
    api = TimedApi(states=[_state(), _state(_job("queued", None)), _state(_job())])
    clock, timed = _timed_clock(STARTED_EPOCH + 100)
    assert probe.wait_for_application_start(api, _target(), timed) == 110
    assert clock.sleeps == [5, 5]


def test_aprovacao_alem_do_teto_de_7200_segundos_falha_fechado_com_key_value() -> None:
    api = TimedApi(states=[_state(_job("waiting", None))])
    clock, timed = _timed_clock(STARTED_EPOCH)
    with pytest.raises(ValueError, match=r"^closed_ready_probe=approval_cap seconds=7200$"):
        probe.wait_for_application_start(api, _target(), timed)
    assert probe.APPROVAL_CAP_SECONDS == 7200
    assert 7200 <= clock.now <= 7205


def test_dois_jobs_com_o_nome_do_application_falham_fechado() -> None:
    api = TimedApi(states=[_state(_job(), _job())])
    _, timed = _timed_clock(STARTED_EPOCH)
    with pytest.raises(ValueError, match=r"^closed_ready_probe=job_count$"):
        probe.wait_for_application_start(api, _target(), timed)


def test_application_em_andamento_sem_started_at_falha_fechado() -> None:
    api = TimedApi(states=[_state(_job(started_at=None))])
    _, timed = _timed_clock(STARTED_EPOCH)
    with pytest.raises(ValueError, match=r"^closed_ready_probe=started_at$"):
        probe.wait_for_application_start(api, _target(), timed)


def test_started_at_a_frente_do_relogio_local_vira_zero_segundos_e_registra_a_inclinacao(
    caplog: pytest.LogCaptureFixture,
) -> None:
    api = TimedApi(states=[_state(_job())])
    _, timed = _timed_clock(STARTED_EPOCH - 30)
    with caplog.at_level("INFO"):
        assert probe.wait_for_application_start(api, _target(), timed) == 0
    assert "closed_ready_probe=started_at_ahead skew_seconds=30" in caplog.text


def test_application_ja_concluido_ao_ser_visto_entrega_o_tempo_para_reler_o_artefato() -> None:
    api = TimedApi(states=[_state(_job("completed"))])
    _, timed = _timed_clock(STARTED_EPOCH + 60)
    assert probe.wait_for_application_start(api, _target(), timed) == 60


def test_relogio_de_2550_segundos_corre_desde_o_started_at_e_o_valor_nao_muda() -> None:
    assert probe.TIMEOUT_SECONDS == 2550
    api = TimedApi(states=[_state(_job())], appears_at=10**6)
    clock, timed = _timed_clock(STARTED_EPOCH + 2500)
    with pytest.raises(ValueError, match=r"^closed_ready_probe=deadline$"):
        probe.wait_for_published(api, _target(), timed)
    assert clock.now <= 50 + probe.POLL_SECONDS


def test_publicado_dentro_do_relogio_devolve_o_hash() -> None:
    api = TimedApi(states=[_state(_job())], appears_at=2)
    _, timed = _timed_clock(STARTED_EPOCH + 10)
    assert probe.wait_for_published(api, _target(), timed) == object_hash(READY)


def test_timeout_do_job_cobre_o_teto_mais_o_relogio_e_cabe_no_limite_hospedado() -> None:
    needed = -(-(probe.APPROVAL_CAP_SECONDS + probe.TIMEOUT_SECONDS) // 60)
    assert needed + 15 == probe.JOB_TIMEOUT_MINUTES
    job = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]["closed-ready"]
    assert job["timeout-minutes"] == probe.JOB_TIMEOUT_MINUTES
    assert job["timeout-minutes"] < 360
