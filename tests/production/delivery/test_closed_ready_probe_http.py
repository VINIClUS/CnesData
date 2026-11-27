"""Sonda do closed-ready: GET condicional, limite de taxa, cadência e linha de saída (P35c)."""

from __future__ import annotations

import contextlib
import email.message
import http.client
import http.server
import io
import json
import logging
import threading
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from scripts.promotion import closed_ready_probe as probe
from tests.production.delivery import _ia_stage_wait_consumer as ia
from tests.production.delivery.test_closed_ready_probe import (
    MEMBERS,
    READY,
    Api,
    Clock,
    TimedApi,
    _job,
    _state,
    _zip,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

TARGET = probe.Target(7, 1, "application / execute")
LISTING = "repos/VINIClUS/CnesData/actions/runs/7/artifacts?name=cnesdata-closed-ready-7-1"
RATE = {"x-ratelimit-limit": "5000", "x-ratelimit-used": "3", "x-ratelimit-remaining": "4997"}


def _headers(**values: str) -> email.message.Message:
    message = email.message.Message()
    for key, value in values.items():
        message[key.replace("_", "-")] = value
    return message


class Response:
    def __init__(self, body: bytes, **headers: str) -> None:
        self.body, self.headers = body, _headers(**headers)

    def __enter__(self) -> Response:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def read(self) -> bytes:
        return self.body


def _error(code: int, **headers: str) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "https://api.github.com/x", code, "x", _headers(**headers), io.BytesIO(b"")
    )


class Opener:
    def __init__(self, *steps: Any) -> None:
        self.steps, self.requests = list(steps), []

    def open(self, request: Any, timeout: float | None = None) -> Any:
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step


def _api(*steps: Any, follow: Any = None) -> tuple[probe.HttpApi, Opener, Clock]:
    opener, clock = Opener(*steps), Clock()
    return (
        probe.HttpApi(
            "VINIClUS/CnesData",
            TARGET,
            "t0k3n",
            probe.Transport(
                opener=opener, follow=follow or (lambda url: Response(b"")), sleep=clock.sleep
            ),
        ),
        opener,
        clock,
    )


def _listing(*ids: int) -> bytes:
    return json.dumps({"artifacts": [{"id": number} for number in ids]}).encode()


def _sent(opener: Opener, index: int) -> dict[str, str]:
    return {key.lower(): value for key, value in opener.requests[index].header_items()}


def test_primeira_leitura_nao_envia_etag_e_a_segunda_envia_o_guardado_no_mesmo_caminho() -> None:
    api, opener, _ = _api(
        Response(_listing(9), ETag='"v1"', **RATE), _error(304, ETag='"v1"', **RATE)
    )
    assert api.artifacts("cnesdata-closed-ready-7-1") == [{"id": 9}]
    assert api.artifacts("cnesdata-closed-ready-7-1") == [{"id": 9}]
    assert "if-none-match" not in _sent(opener, 0)
    assert _sent(opener, 1)["if-none-match"] == '"v1"'
    assert (
        opener.requests[0].full_url
        == opener.requests[1].full_url
        == "https://api.github.com/" + LISTING
    )


def test_304_devolve_o_corpo_completo_em_cache_e_nunca_uma_listagem_vazia() -> None:
    api, _, _ = _api(Response(_listing(9, 10), ETag='"v1"'), _error(304), _error(304))
    assert api.artifacts("cnesdata-closed-ready-7-1") == [{"id": 9}, {"id": 10}]
    assert api.artifacts("cnesdata-closed-ready-7-1") == [{"id": 9}, {"id": 10}]
    assert api.artifacts("cnesdata-closed-ready-7-1") == [{"id": 9}, {"id": 10}]


def test_corpo_novo_substitui_o_cache_e_o_etag() -> None:
    api, opener, _ = _api(
        Response(_listing(9), ETag='"v1"'),
        Response(_listing(11), ETag='"v2"'),
        _error(304, ETag='"v2"'),
    )
    api.artifacts("n")
    assert api.artifacts("n") == [{"id": 11}]
    assert api.artifacts("n") == [{"id": 11}]
    assert _sent(opener, 2)["if-none-match"] == '"v2"'


def test_listagem_de_jobs_tambem_e_condicional_com_os_mesmos_parametros() -> None:
    body = json.dumps({"jobs": [{"name": "j"}]}).encode()
    api, opener, _ = _api(Response(body, ETag='"j1"'), _error(304, ETag='"j1"'))
    assert api.jobs() == api.jobs() == [{"name": "j"}]
    assert opener.requests[0].full_url == opener.requests[1].full_url
    assert opener.requests[0].full_url.endswith("/runs/7/attempts/1/jobs?per_page=100")
    assert _sent(opener, 1)["if-none-match"] == '"j1"'


def test_304_sem_corpo_em_cache_falha_fechado() -> None:
    api, _, _ = _api(_error(304))
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.artifacts("n")


@pytest.mark.parametrize("failure", [_error(500), _error(502), urllib.error.URLError("down")])
def test_erro_transitorio_com_corpo_em_cache_nao_devolve_o_cache_e_repete_o_limite(
    failure: Exception,
) -> None:
    api, opener, clock = _api(Response(_listing(9), ETag='"v1"'), failure, failure, failure)
    api.artifacts("n")
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.artifacts("n")
    assert len(opener.requests) == 1 + probe.API_ATTEMPTS
    assert clock.sleeps == [probe.API_RETRY_SECONDS] * (probe.API_ATTEMPTS - 1)


@pytest.mark.parametrize("code", [401, 403, 404])
def test_erro_de_cliente_com_corpo_em_cache_falha_fechado_sem_repetir(code: int) -> None:
    api, opener, clock = _api(
        Response(_listing(9), ETag='"v1"'), _error(code), Response(_listing(9))
    )
    api.artifacts("n")
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.artifacts("n")
    assert len(opener.requests) == 2
    assert clock.sleeps == []


@pytest.mark.parametrize(
    "error",
    [
        _error(403, x_ratelimit_remaining="0", x_ratelimit_limit="5000", x_ratelimit_used="5000"),
        _error(429, retry_after="60"),
        _error(403, retry_after="60"),
    ],
)
def test_limite_de_taxa_falha_fechado_sem_repetir_nem_devolver_o_cache(
    error: urllib.error.HTTPError,
) -> None:
    api, opener, clock = _api(Response(_listing(9), ETag='"v1"'), error, Response(_listing(9)))
    api.artifacts("n")
    with pytest.raises(ValueError, match=r"^github=rate_limited$"):
        api.artifacts("n")
    assert len(opener.requests) == 2
    assert clock.sleeps == []


def test_numeros_de_limite_sao_os_da_ultima_resposta_incluindo_304_e_erro() -> None:
    api, _, _ = _api(
        Response(
            _listing(9),
            ETag='"v1"',
            x_ratelimit_limit="5000",
            x_ratelimit_used="3",
            x_ratelimit_remaining="4997",
        ),
        _error(304, x_ratelimit_limit="5000", x_ratelimit_used="4", x_ratelimit_remaining="4996"),
        _error(403, x_ratelimit_limit="5000", x_ratelimit_used="5000", x_ratelimit_remaining="0"),
    )
    assert api.rate is None
    api.artifacts("n")
    assert api.rate == {"limit": 5000, "used": 3, "remaining": 4997}
    api.artifacts("n")
    assert api.rate == {"limit": 5000, "used": 4, "remaining": 4996}
    with pytest.raises(ValueError, match=r"rate_limited"):
        api.artifacts("n")
    assert api.rate == {"limit": 5000, "used": 5000, "remaining": 0}


def test_cada_requisicao_leva_o_token_do_job_no_cabecalho() -> None:
    api, opener, _ = _api(Response(_listing()))
    api.artifacts("n")
    assert _sent(opener, 0)["authorization"] == "Bearer t0k3n"


def test_arquivo_segue_o_redirecionamento_sem_o_token_e_so_para_hosts_conhecidos() -> None:
    followed: list[str] = []

    def follow(url: str) -> Response:
        followed.append(url)
        return Response(b"ZIP")

    good = "https://x.blob.core.windows.net/a?sig=1"
    api, opener, _ = _api(_error(302, Location=good), follow=follow)
    assert api.archive(9) == b"ZIP"
    assert followed == [good]
    assert "if-none-match" not in _sent(opener, 0)
    for bad in (
        "http://x.blob.core.windows.net/a",
        "https://evil.example/a",
        "https://blob.core.windows.net.evil/a",
    ):
        api, _, _ = _api(_error(302, Location=bad), follow=follow)
        with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
            api.archive(9)
    assert followed == [good]


def _ok_api() -> Api:
    return Api()


def test_limite_abaixo_do_assumido_recusa_antes_de_qualquer_espera_ou_leitura_de_artefato() -> None:
    api = TimedApi(states=[_state(_job("queued", None))])
    api.rate = {"limit": 1000, "used": 3, "remaining": 997}
    clock = Clock()
    with pytest.raises(ValueError, match=r"^github=rate_limit value=1000$"):
        probe.wait_for_published(
            api, TARGET, probe.Clock(sleep=clock.sleep, monotonic=clock.monotonic, wall=lambda: 0.0)
        )
    assert clock.sleeps == []
    assert api.calls == 0


def test_limite_do_assumido_exatamente_e_aceito() -> None:
    assert ia.ASSUMED_RATE_LIMIT == probe.ASSUMED_RATE_LIMIT == 5000
    api = TimedApi(states=[_state(_job("completed", "2026-11-25T12:00:00Z"))])
    api.rate = {"limit": 5000, "used": 3, "remaining": 4997}
    clock = Clock()
    timed = probe.Clock(
        sleep=clock.sleep, monotonic=clock.monotonic, wall=lambda: 1_795_608_000.0 + 5
    )
    assert probe.wait_for_published(api, TARGET, timed)


def test_primeira_resposta_sem_numeros_de_limite_recusa_com_na() -> None:
    api = TimedApi(states=[_state(_job("queued", None))])
    api.rate = None
    clock = Clock()
    with pytest.raises(ValueError, match=r"^github=rate_limit value=na$"):
        probe.wait_for_published(
            api, TARGET, probe.Clock(sleep=clock.sleep, monotonic=clock.monotonic)
        )


def test_cadencia_da_espera_do_artefato_e_de_uma_leitura_a_cada_5_s_e_de_jobs_a_cada_60_s() -> None:
    api, clock = Api(appears_at=10**6), Clock()
    with pytest.raises(ValueError, match=r"^closed_ready_probe=deadline$"):
        probe.wait_for_closed_ready(
            api, TARGET, probe.Clock(sleep=clock.sleep, monotonic=clock.monotonic), 600
        )
    assert api.calls == 600 // ia.PROBE_POLL_SECONDS * ia.PROBE_ARTIFACT_READS_PER_POLL + 1
    assert api.job_reads == 600 // 60 * ia.PROBE_JOBS_READS_PER_MINUTE


def test_conclusao_do_application_sem_artefato_e_detectada_em_ate_60_s() -> None:
    api, clock = Api(appears_at=10**6), Clock()
    api.application = "completed"
    with pytest.raises(ValueError, match=r"^closed_ready_probe=application_concluded$"):
        probe.wait_for_closed_ready(
            api, TARGET, probe.Clock(sleep=clock.sleep, monotonic=clock.monotonic)
        )
    assert clock.now <= 60


def test_fase_de_inicio_le_a_lista_de_jobs_a_cada_5_s() -> None:
    api = TimedApi(states=[_state(_job("queued", None))] * 12 + [_state(_job())])
    clock = Clock()
    timed = probe.Clock(
        sleep=clock.sleep, monotonic=clock.monotonic, wall=lambda: 1_795_608_000.0 + clock.now
    )
    probe.wait_for_application_start(api, TARGET, timed)
    assert api.job_reads == 13
    assert clock.sleeps == [ia.PROBE_POLL_SECONDS] * 12 == [probe.POLL_SECONDS] * 12


def test_constantes_da_sonda_sao_as_do_modelo_de_rest_do_infra_ansible_no_pin() -> None:
    assert probe.POLL_SECONDS == ia.PROBE_POLL_SECONDS
    assert probe.PROBE_ARTIFACT_READS_PER_POLL == ia.PROBE_ARTIFACT_READS_PER_POLL
    assert probe.PROBE_JOBS_READS_PER_MINUTE == ia.PROBE_JOBS_READS_PER_MINUTE
    assert probe.CONCLUDED_CHECK_SECONDS == 60 // ia.PROBE_JOBS_READS_PER_MINUTE
    assert probe.PROBE_END_STAGES == ia.PROBE_END_STAGES


class _Main:
    def __init__(self, api: Any) -> None:
        self.api = api

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.api


def _env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    output = tmp_path / "out"
    for key, value in {
        "RUN_ID": "7",
        "RUN_ATTEMPT": "1",
        "REPOSITORY": "VINIClUS/CnesData",
        "APPLICATION_JOB": "application / execute",
        "GH_TOKEN": "t0k3n",
        "GITHUB_OUTPUT": str(output),
    }.items():
        monkeypatch.setenv(key, value)
    return output


def _exit_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.getMessage().startswith("closed_ready_probe=exit")
    ]


def _recent_job() -> dict[str, Any]:
    return _job("completed", datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))


@pytest.mark.parametrize("case", ["ok", "limit", "rate_limited", "deadline", "unexpected"])
def test_toda_saida_registra_uma_linha_com_os_numeros_da_ultima_resposta(
    case: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _env(monkeypatch, tmp_path)
    api = TimedApi(states=[_state(_recent_job())])
    api.rate = {"limit": 5000, "used": 3, "remaining": 4997}
    if case == "limit":
        api.rate = {"limit": 1000, "used": 8, "remaining": 992}
    if case == "rate_limited":
        api.jobs = lambda: (_ for _ in ()).throw(ValueError("github=rate_limited"))  # type: ignore[method-assign]
    if case == "unexpected":
        api.jobs = lambda: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]
    if case == "deadline":
        monkeypatch.setattr(
            probe,
            "wait_for_published",
            lambda *a, **k: (_ for _ in ()).throw(ValueError("closed_ready_probe=deadline")),
        )
    monkeypatch.setattr(probe, "HttpApi", _Main(api))
    caplog.set_level(logging.INFO)
    if case == "unexpected":
        with pytest.raises(RuntimeError):
            probe.main()
    else:
        assert probe.main() == (0 if case == "ok" else 1)
    (line,) = _exit_lines(caplog)
    limit, used = (1000, 8) if case == "limit" else (5000, 3)
    assert line == (
        f"closed_ready_probe=exit x_ratelimit_limit={limit} x_ratelimit_used={used} "
        f"x_ratelimit_remaining={limit - used}"
    )


def test_linha_de_saida_sem_numeros_registra_na_e_nunca_o_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _env(monkeypatch, tmp_path)
    api = TimedApi(states=[_state(_recent_job())])
    api.rate = None
    monkeypatch.setattr(probe, "HttpApi", _Main(api))
    caplog.set_level(logging.INFO)
    assert probe.main() == 1
    (line,) = _exit_lines(caplog)
    unknown = "x_ratelimit_limit=na x_ratelimit_used=na x_ratelimit_remaining=na"
    assert line == f"closed_ready_probe=exit {unknown}"
    assert "t0k3n" not in caplog.text


def test_sucesso_escreve_o_hash_no_output_do_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = _env(monkeypatch, tmp_path)
    monkeypatch.setattr(probe, "HttpApi", _Main(TimedApi(states=[_state(_recent_job())])))
    assert probe.main() == 0
    assert output.read_text(encoding="utf-8").startswith("closed_ready_sha256=")


def test_membros_do_zip_do_cliente_real_passam_pela_mesma_validacao() -> None:
    api, _, _ = _api(Response(_listing(9)), Response(_zip(MEMBERS)))
    assert probe._published(api, "n") is not None
    assert READY["kind"] == "closed-ready"


class _Recorder(http.server.BaseHTTPRequestHandler):
    seen: list[dict[str, str]]
    location: str | None = None

    def do_GET(self) -> None:
        self.seen.append({key.lower(): value for key, value in self.headers.items()})
        if self.location is None:
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        self.send_response(302)
        self.send_header("Location", self.location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return


@contextlib.contextmanager
def _loopback(location: str | None = None) -> Iterator[tuple[str, list[dict[str, str]]]]:
    seen: list[dict[str, str]] = []
    handler = type("Handler", (_Recorder,), {"seen": seen, "location": location})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/target", seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_opener_real_recusa_o_redirecionamento_e_o_token_nunca_chega_ao_destino() -> None:
    with _loopback() as (target, target_seen), _loopback(location=target) as (origin, origin_seen):
        request = urllib.request.Request(origin, headers={"Authorization": "Bearer t0k3n"})
        with pytest.raises(urllib.error.HTTPError) as refused:
            probe.Transport().opener.open(request, timeout=5)
        assert refused.value.code == 302
        assert [seen["authorization"] for seen in origin_seen] == ["Bearer t0k3n"]
        assert target_seen == []


def test_follow_padrao_busca_o_destino_sem_cabecalho_de_autorizacao() -> None:
    with _loopback() as (target, target_seen):
        with probe.Transport().follow(target) as response:
            assert response.read() == b"ok"
        assert len(target_seen) == 1
        assert "authorization" not in target_seen[0]


def test_dois_304_seguidos_reenviam_o_mesmo_etag_sem_perder_o_cache() -> None:
    api, opener, _ = _api(Response(_listing(9), ETag='"v1"'), _error(304), _error(304))
    assert api.artifacts("n") == api.artifacts("n") == api.artifacts("n") == [{"id": 9}]
    assert _sent(opener, 1)["if-none-match"] == '"v1"'
    assert _sent(opener, 2)["if-none-match"] == '"v1"'


@pytest.mark.parametrize(
    "url",
    [
        "https://evilblob.core.windows.net/a",
        "https://evilactions.githubusercontent.com/a",
        "https://blob.core.windows.net/a",
        "https://actions.githubusercontent.com/a",
    ],
)
def test_host_de_download_so_vale_como_subdominio_com_ponto_inicial(url: str) -> None:
    api, _, _ = _api(_error(302, Location=url), follow=lambda _url: Response(b"ZIP"))
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.archive(9)


def test_host_de_download_com_subdominio_legitimo_e_aceito() -> None:
    for host in ("x.blob.core.windows.net", "pipelines.actions.githubusercontent.com"):
        api, _, _ = _api(
            _error(302, Location=f"https://{host}/a"), follow=lambda _url: Response(b"ZIP")
        )
        assert api.archive(9) == b"ZIP"


def test_tentativas_e_espera_da_api_sao_as_declaradas() -> None:
    assert (probe.API_ATTEMPTS, probe.API_RETRY_SECONDS) == (3, 2)
    api, opener, _ = _api(*[_error(500)] * 5)
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.artifacts("n")
    assert len(opener.requests) == 3


@pytest.mark.parametrize(
    "headers",
    [
        {"x-ratelimit-limit": "abc", "x-ratelimit-used": "3", "x-ratelimit-remaining": "4997"},
        {"x-ratelimit-limit": "5000", "x-ratelimit-used": "-3", "x-ratelimit-remaining": "4997"},
        {"x-ratelimit-limit": "5000", "x-ratelimit-used": "3"},
    ],
)
def test_cabecalhos_de_limite_nao_numericos_ou_ausentes_deixam_o_registro_vazio(
    headers: dict[str, str],
) -> None:
    api, _, _ = _api(Response(_listing(), **headers))
    api.artifacts("n")
    assert api.rate is None


def test_cabecalhos_de_limite_invalidos_nao_apagam_o_ultimo_registro_valido() -> None:
    api, _, _ = _api(Response(_listing(), **RATE), Response(_listing(), x_ratelimit_limit="abc"))
    api.artifacts("n")
    api.artifacts("n")
    assert api.rate == {"limit": 5000, "used": 3, "remaining": 4997}


BLOB = "https://x.blob.core.windows.net/a?sig="


class Follower:
    def __init__(self, *steps: Any) -> None:
        self.steps, self.urls = list(steps), []

    def __call__(self, url: str) -> Response:
        self.urls.append(url)
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step


def _blob_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(BLOB, code, "x", _headers(), io.BytesIO(b""))


def test_download_com_5xx_transitorio_refaz_a_partir_do_endpoint_da_api() -> None:
    follower = Follower(_blob_error(503), Response(b"ZIP"))
    api, opener, clock = _api(
        _error(302, Location=BLOB + "1"), _error(302, Location=BLOB + "2"), follow=follower
    )
    assert api.archive(9) == b"ZIP"
    assert follower.urls == [BLOB + "1", BLOB + "2"]
    assert len(opener.requests) == 2
    assert clock.sleeps == [probe.API_RETRY_SECONDS]
    assert all("authorization" in _sent(opener, i) for i in (0, 1))


@pytest.mark.parametrize("code", [500, 502, 503, 504])
def test_download_repete_cada_5xx_transitorio(code: int) -> None:
    follower = Follower(_blob_error(code), Response(b"ZIP"))
    api, opener, _ = _api(_error(302, Location=BLOB), _error(302, Location=BLOB), follow=follower)
    assert api.archive(9) == b"ZIP"
    assert len(opener.requests) == 2


def test_download_5xx_em_toda_tentativa_falha_apos_o_limite_com_chave_valor() -> None:
    follower = Follower(*[_blob_error(503)] * probe.API_ATTEMPTS)
    api, opener, clock = _api(
        *[_error(302, Location=BLOB)] * probe.API_ATTEMPTS, follow=follower
    )
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.archive(9)
    assert len(opener.requests) == probe.API_ATTEMPTS
    assert len(follower.urls) == probe.API_ATTEMPTS
    assert clock.sleeps == [probe.API_RETRY_SECONDS] * (probe.API_ATTEMPTS - 1)


@pytest.mark.parametrize("code", [400, 403, 404])
def test_download_4xx_permanente_nao_repete(code: int) -> None:
    follower = Follower(_blob_error(code))
    api, opener, clock = _api(_error(302, Location=BLOB), follow=follower)
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.archive(9)
    assert len(opener.requests) == 1
    assert clock.sleeps == []


@pytest.mark.parametrize(
    "failure",
    [TimeoutError(), ConnectionResetError(), ConnectionRefusedError(),
     http.client.RemoteDisconnected("closed")],
)
def test_download_com_falha_de_rede_repete(failure: OSError) -> None:
    follower = Follower(failure, Response(b"ZIP"))
    api, opener, clock = _api(
        _error(302, Location=BLOB), _error(302, Location=BLOB), follow=follower
    )
    assert api.archive(9) == b"ZIP"
    assert len(opener.requests) == 2
    assert clock.sleeps == [probe.API_RETRY_SECONDS]


class BrokenBody(Response):
    def __init__(self, failure: BaseException, **headers: str) -> None:
        super().__init__(b"", **headers)
        self.failure = failure

    def read(self) -> bytes:
        raise self.failure


def _body_failures() -> list[BaseException]:
    return [
        TimeoutError(),
        ConnectionResetError(),
        http.client.IncompleteRead(b"par"),
        urllib.error.URLError(TimeoutError()),
    ]


@pytest.mark.parametrize("call", ["artifacts", "jobs"])
@pytest.mark.parametrize("failure", _body_failures())
def test_leitura_do_corpo_da_listagem_falha_e_repete_ate_o_sucesso(
    call: str, failure: BaseException
) -> None:
    good = Response(json.dumps({"artifacts": [], "jobs": []}).encode())
    api, opener, clock = _api(BrokenBody(failure), good)
    getattr(api, call)(*(["n"] if call == "artifacts" else []))
    assert len(opener.requests) == 2
    assert clock.sleeps == [probe.API_RETRY_SECONDS]


@pytest.mark.parametrize("call", ["artifacts", "jobs"])
def test_leitura_do_corpo_em_toda_tentativa_falha_apos_o_limite(call: str) -> None:
    api, opener, clock = _api(*[BrokenBody(TimeoutError()) for _ in range(probe.API_ATTEMPTS)])
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        getattr(api, call)(*(["n"] if call == "artifacts" else []))
    assert len(opener.requests) == probe.API_ATTEMPTS
    assert clock.sleeps == [probe.API_RETRY_SECONDS] * (probe.API_ATTEMPTS - 1)


def test_corpo_parcial_nao_grava_etag_no_cache_condicional() -> None:
    steps = [BrokenBody(TimeoutError(), ETag='"v1"') for _ in range(probe.API_ATTEMPTS)]
    api, opener, _ = _api(*steps, Response(_listing(1), ETag='"v2"'))
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.artifacts("n")
    api.artifacts("n")
    assert "if-none-match" not in _sent(opener, probe.API_ATTEMPTS)


@pytest.mark.parametrize("failure", _body_failures())
def test_leitura_do_corpo_do_download_repete(failure: BaseException) -> None:
    follower = Follower(BrokenBody(failure), Response(b"ZIP"))
    api, opener, clock = _api(
        _error(302, Location=BLOB), _error(302, Location=BLOB), follow=follower
    )
    assert api.archive(9) == b"ZIP"
    assert len(opener.requests) == 2
    assert clock.sleeps == [probe.API_RETRY_SECONDS]


def test_abertura_da_api_com_urlerror_de_timeout_repete() -> None:
    api, opener, _ = _api(urllib.error.URLError(TimeoutError()), Response(_listing()))
    api.artifacts("n")
    assert len(opener.requests) == 2


@pytest.mark.parametrize("on_download", [False, True])
@pytest.mark.parametrize(
    "failure", [http.client.IncompleteRead(b"par"), http.client.BadStatusLine("x")]
)
def test_http_exception_em_toda_tentativa_termina_como_chave_valor_sem_traceback(
    on_download: bool, failure: http.client.HTTPException
) -> None:
    attempts = probe.API_ATTEMPTS
    if on_download:
        follower = Follower(*[BrokenBody(failure) for _ in range(attempts)])
        api, opener, clock = _api(*[_error(302, Location=BLOB)] * attempts, follow=follower)
    else:
        api, opener, clock = _api(*[BrokenBody(failure) for _ in range(attempts)])
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.archive(9) if on_download else api.jobs()
    assert len(opener.requests) == attempts
    assert clock.sleeps == [probe.API_RETRY_SECONDS] * (attempts - 1)


def test_download_429_e_permanente_porque_o_azure_limita_com_503() -> None:
    follower = Follower(_blob_error(429))
    api, opener, clock = _api(_error(302, Location=BLOB), follow=follower)
    with pytest.raises(ValueError, match=r"^closed_ready_probe=api$"):
        api.archive(9)
    assert len(opener.requests) == 1
    assert len(follower.urls) == 1
    assert clock.sleeps == []
