"""Ciclo completo do cognito-login com orçamento medido e crashes em cada criação de filho."""

from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_types_testing import denied
from tests.integration.aws_capability._login_handoff import handoff_path
from tests.integration.aws_capability._login_lab_testing import LoginWorld, make_login_world
from tests.integration.aws_capability._probe_testing import (
    cleanup_calls,
    cleanup_seconds,
    janitor_ports,
    work_seconds,
)
from tests.integration.aws_capability._profiles import PROFILES, Root
from tests.integration.aws_capability._service_contracts import Reply
from tests.integration.aws_capability.contracts import Outcome

PROFILE = PROFILES["cognito-login"]
SCENARIOS = {
    "tipico": {"ready_after": 3, "human_seconds": 45.0},
    "lento": {"ready_after": 30, "human_seconds": 580.0, "policy_lag": 0},
    "pior_caso": {"ready_after": 34, "human_seconds": 592.0, "silent_seconds": 2.0},
}
LIMITS = (512, 256, 360, 1100.0)


class Crash(BaseException):
    pass


def probe_of(outcome: Outcome) -> dict[str, object]:
    value = outcome.evidence["probe"]
    assert isinstance(value, dict)
    return value


def run_world(tmp_path: Path, scenario: str) -> tuple[LoginWorld, Outcome]:
    world = make_login_world(tmp_path, **SCENARIOS[scenario])
    outcome = lifecycle.run(world.lab.spec, janitor_ports(world.lab, supervised=True))
    return world, outcome


def test_perfil_declara_um_root_cognito_com_janela_e_limites_dimensionados() -> None:
    assert PROFILE.services == ("cognito",)
    assert PROFILE.planned_roots() == (Root("cognito"),)
    limits = PROFILE.limits
    assert (limits.calls, limits.cleanup_reserve, limits.cleanup_seconds, PROFILE.window) == LIMITS
    assert PROFILE.window + limits.cleanup_seconds < 2400


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_run_completo_remove_tudo_dentro_do_orcamento_e_da_janela(
    tmp_path: Path, scenario: str
) -> None:
    world, outcome = run_world(tmp_path, scenario)
    lab, limits = world.lab, PROFILE.limits
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.pools == {}
    assert probe_of(outcome)["login"] == "ok"
    assert lab.budget.work_used <= limits.calls - limits.cleanup_reserve
    assert cleanup_calls(lab) <= limits.cleanup_reserve
    assert work_seconds(lab) * 1.2 <= PROFILE.window
    assert cleanup_seconds(lab) * 1.2 <= limits.cleanup_seconds
    assert outcome.evidence["final_counts_all_cap"] == {
        "cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0,
    }
    assert not handoff_path(lab.spec).exists()


def test_limpeza_com_falha_transitoria_em_filho_continua_dentro_do_orcamento(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = make_login_world(tmp_path, **SCENARIOS["pior_caso"])
    original = world.lab.aws._cognito_idp_delete_user_pool_client
    failures = [True]

    def flaky(payload: dict, who: str) -> Reply:
        if failures:
            failures.pop()
            return denied("InternalErrorException")
        return original(payload, who)

    monkeypatch.setattr(world.lab.aws, "_cognito_idp_delete_user_pool_client", flaky)
    outcome = lifecycle.run(world.lab.spec, janitor_ports(world.lab, supervised=True))
    lab = world.lab
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert cleanup_calls(lab) <= PROFILE.limits.cleanup_reserve
    assert cleanup_seconds(lab) * 1.2 <= PROFILE.limits.cleanup_seconds


CREATIONS = {
    "server": "_cognito_idp_create_resource_server",
    "client": "_cognito_idp_create_user_pool_client",
    "usuario": "_cognito_idp_admin_create_user",
    "domain": "_cognito_idp_create_user_pool_domain",
    "branding": "_cognito_idp_create_managed_login_branding",
}


@pytest.mark.parametrize("step", list(CREATIONS))
def test_crash_apos_cada_criacao_de_filho_com_journal_incerto_remove_tudo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, step: str
) -> None:
    world = make_login_world(tmp_path, **SCENARIOS["tipico"])
    lab = world.lab
    original = getattr(lab.aws, CREATIONS[step])

    def crash_after(payload: dict, who: str) -> Reply:
        original(payload, who)
        raise Crash

    monkeypatch.setattr(lab.aws, CREATIONS[step], crash_after)
    with pytest.raises(Crash):
        lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert lab.aws.pools
    pool = next(iter(lab.aws.pools.values()))
    assert pool.servers or pool.clients or pool.users or pool.domain or pool.brandings
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.pools == {}
    assert not handoff_path(lab.spec).exists()
    assert cleanup_calls(lab) <= PROFILE.limits.cleanup_reserve
    assert set(lab.aws.lanes[-cleanup_calls(lab) :]) == {"cleanup"}


def test_crash_com_handoff_escrito_e_janitor_remove_pool_filhos_e_arquivo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = make_login_world(tmp_path, **SCENARIOS["tipico"])
    lab = world.lab

    def crash(timeout: float) -> None:
        assert handoff_path(lab.spec).exists()
        raise Crash

    monkeypatch.setattr(world.listener, "wait", crash)
    with pytest.raises(Crash):
        lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert not handoff_path(lab.spec).exists()
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.pools == {}
