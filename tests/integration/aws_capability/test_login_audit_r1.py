"""Rodada 1 da auditoria: booleanos que viram falso, reserva de troca, temp do handoff, repr."""

from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._journal import Journal
from tests.integration.aws_capability._login_handoff import handoff_path, write_handoff
from tests.integration.aws_capability._login_http import Http
from tests.integration.aws_capability._login_lab_testing import (
    LoginWorld,
    create_root,
    make_login_world,
)
from tests.integration.aws_capability._login_oauth import Grant
from tests.integration.aws_capability._probe_testing import START, janitor_ports
from tests.integration.aws_capability.contracts import Limits, RequestBudget, RequestContext


def observe(world: LoginWorld) -> dict[str, object]:
    lab = world.lab
    root = create_root(lab)
    return dict(lab.services.probe([root], lab.spec, lab.context))


@pytest.mark.parametrize(
    ("option", "key"),
    [
        ({"wrong_client_id": True}, "a_client_id_ok"),
        ({"wrong_id_aud": True}, "a_id_aud_is_client"),
        ({"bad_issuer": True}, "discovery_issuer_ok"),
        ({"http_jwks": True}, "discovery_jwks_https"),
    ],
)
def test_cada_booleano_vira_falso_quando_o_idp_diverge(
    tmp_path: Path, option: dict[str, bool], key: str
) -> None:
    observed = observe(make_login_world(tmp_path, **option))
    assert observed[key] is False


def test_negativo_de_reuso_envia_o_codigo_a_e_nenhum_outro(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    observe(world)
    assert world.idp.token_codes == [
        "code-1", "code-2", "code-3", "code-3", "code-4", "code-1",
    ]


def test_adaptador_http_segue_redirect_na_prontidao() -> None:
    answers = {"/oauth2/authorize": httpx.Response(302, headers={"Location": "https://x/login"})}

    def handler(request: httpx.Request) -> httpx.Response:
        return answers.get(request.url.path, httpx.Response(200, text="ok"))

    budget = RequestBudget(Limits(), 5000.0, lambda: 1000.0)
    client = httpx.Client(transport=httpx.MockTransport(handler))
    http = Http(client, RequestContext(budget, "work"))
    assert http.get("https://x/oauth2/authorize?a=b").status == 200


def test_segundos_de_prontidao_e_de_login_sao_os_do_modelo(tmp_path: Path) -> None:
    observed = observe(make_login_world(tmp_path, ready_after=2, human_seconds=45.0))
    assert observed["domain_ready"] == 15.2
    assert observed["domain_ready_polls"] == 3
    assert observed["login_seconds"] == 48.0


@pytest.mark.parametrize("extra", [-100.0, 0.0])
def test_espera_do_login_e_limitada_pelo_restante_menos_a_reserva_de_troca(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra: float
) -> None:
    world = make_login_world(tmp_path, human_seconds=10.0)
    lab = world.lab
    lab.budget.deadline = START + 700.0 + extra
    seen: list[tuple[float, float]] = []
    original = world.listener.wait

    def spy(timeout: float):
        seen.append((timeout, lab.budget.deadline - lab.clock[0]))
        return original(timeout)

    monkeypatch.setattr(world.listener, "wait", spy)
    observe(world)
    timeout, remaining = seen[0]
    assert remaining < 750
    assert timeout == pytest.approx(remaining - 150.0)


def test_espera_do_login_nao_passa_de_600_com_folga(tmp_path: Path) -> None:
    world = make_login_world(tmp_path, human_seconds=10.0)
    observe(world)
    assert world.listener.waits[0] == 600.0


def test_codigo_vazio_com_state_correto_e_rejeitado(tmp_path: Path) -> None:
    world = make_login_world(tmp_path, empty_code_at=2)
    observed = observe(world)
    assert observed["login"] == "missing_code"
    assert "a_status" not in observed
    assert ("POST", "/oauth2/token") not in world.idp.requests


def test_repr_de_grant_nao_mostra_codigo_nem_verifier() -> None:
    text = repr(Grant("CODE-CANARY", "VERIFIER-CANARY"))
    assert "CODE-CANARY" not in text
    assert "VERIFIER-CANARY" not in text


def temp_handoff(world: LoginWorld, run_id: str) -> Path:
    path = world.lab.spec.journal_dir / f".handoff-{run_id}-orphan"
    path.write_text("password=PASSWORD-CANARY")
    return path


def test_janitor_remove_temp_orfao_do_proprio_run_e_preserva_o_de_outro(tmp_path: Path) -> None:
    world = make_login_world(tmp_path)
    root = create_root(world.lab)
    journal = Journal(world.lab.spec)
    journal.created = [root]
    journal.phase = "cleanup_requested"
    journal.save()
    mine = temp_handoff(world, "a1b2c3d4")
    other = temp_handoff(world, "ffffffff")
    outcome = lifecycle.janitor(world.lab.spec, janitor_ports(world.lab))
    assert outcome.status == "succeeded"
    assert not mine.exists()
    assert other.exists()


def test_escrita_do_handoff_usa_o_run_no_prefixo_do_temporario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = make_login_world(tmp_path)
    seen: list[str] = []
    import tempfile

    original = tempfile.mkstemp

    def spy(*args: Any, **kwargs: Any) -> tuple[int, str]:
        seen.append(str(kwargs.get("prefix")))
        return original(*args, **kwargs)

    monkeypatch.setattr(tempfile, "mkstemp", spy)
    write_handoff(replace(world.lab.spec), "u", "n", "p")
    assert seen == [".handoff-a1b2c3d4-"]
    assert handoff_path(world.lab.spec).exists()
