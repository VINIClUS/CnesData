"""Ciclo completo do iam-trust sobre a fronteira com estado, com orçamento medido."""

from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_types_testing import ACCESS_KEY, SECRET, TOKEN
from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._probe_testing import (
    LATENCY,
    ProbeLab,
    cleanup_calls,
    cleanup_seconds,
    janitor_ports,
    make_probe_lab,
    work_calls,
    work_seconds,
)
from tests.integration.aws_capability._profiles import PROFILES, Root
from tests.integration.aws_capability.contracts import Outcome

PROFILE = PROFILES["iam-trust"]
SCENARIOS = {
    "tipico": {"policy_lag": 3, "trust_lag": 3},
    "lento": {"assume_denials": 6, "policy_lag": 7, "trust_lag": 7},
    "pior_caso_limitado": {
        "assume_denials": 11, "policy_lag": 12, "trust_lag": 12, "malformed_creates": 3,
    },
}
KEYS = {
    "assume_seconds", "chain_seconds", "self_assume", "duration_3601", "duration_3601_category",
    "session_tags_before", "source_identity_before", "session_tags_after",
    "session_tags_ready_seconds", "source_identity_after", "source_identity_ready_seconds",
}


def cycle(tmp_path: Path, scenario: str, **options: object) -> ProbeLab:
    return make_probe_lab(
        tmp_path / "private",
        "iam-trust",
        seeded=False,
        latency=LATENCY,
        **{**SCENARIOS[scenario], **options},
    )


def probe_of(outcome: Outcome) -> dict[str, object]:
    value = outcome.evidence["probe"]
    assert isinstance(value, dict)
    return value


def test_perfil_declara_duas_raizes_iam_e_limites_dimensionados() -> None:
    assert PROFILE.services == ("iam",)
    assert PROFILE.planned_roots() == (Root("iam"), Root("iam", "r2"))
    assert PROFILE.limits.calls == 176
    assert PROFILE.limits.cleanup_seconds == 150
    assert PROFILE.limits.cleanup_reserve == 104
    assert PROFILE.window == 180.0


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_run_completo_remove_os_dois_roles_dentro_do_orcamento_e_da_janela(
    tmp_path: Path, scenario: str
) -> None:
    lab = cycle(tmp_path, scenario)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    limits = PROFILE.limits
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.roles == {}
    assert set(probe_of(outcome)) == KEYS
    assert work_calls(lab) <= limits.calls - limits.cleanup_reserve
    assert cleanup_calls(lab) <= limits.cleanup_reserve
    assert work_seconds(lab) * 1.2 <= PROFILE.window
    assert cleanup_seconds(lab) * 1.2 <= limits.cleanup_seconds
    assert outcome.evidence["final_counts_all_cap"] == {
        "cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0,
    }


def test_role_com_trust_ampliado_e_policy_inline_e_removido_sem_ownership_mismatch(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, "tipico")
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert outcome.reason == "complete"
    deletes = [
        (r.operation, r.payload["RoleName"])
        for r in lab.aws.requests
        if r.operation in {"delete-role-policy", "delete-role"}
    ]
    assert deletes[0] == ("delete-role-policy", NAME)
    assert {name for op, name in deletes if op == "delete-role"} == {NAME, f"{NAME}-r2"}
    assert set(lab.aws.lanes[lab.aws.operations().index("delete-role-policy") :]) == {"cleanup"}


def test_journal_e_evidencia_nunca_guardam_credenciais_nem_constantes_de_tag(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, "tipico")
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    text = lab.spec.journal_dir.joinpath("a1b2c3d4.json").read_text() + repr(outcome)
    secrets = (ACCESS_KEY, SECRET, TOKEN, "cap-tag", "cap-source")
    assert not any(secret in text for secret in secrets)


@pytest.mark.parametrize("option", [{"trust_lag": 10**6}, {"policy_lag": 10**6}])
def test_controle_que_falha_nao_deixa_nada_e_nenhuma_negacao_vira_evidencia(
    tmp_path: Path, option: dict
) -> None:
    lab = cycle(tmp_path, "tipico", **option)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert probe_of(outcome) == {}
    assert lab.aws.roles == {}
    assert cleanup_seconds(lab) <= PROFILE.limits.cleanup_seconds
    assert cleanup_calls(lab) <= PROFILE.limits.cleanup_reserve
