"""Ciclo completo do iam-leadingkeys sobre a fronteira com estado, com orçamento medido."""

from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_types_testing import (
    ACCESS_KEY,
    SECRET,
    TOKEN,
    FakeRole,
)
from tests.integration.aws_capability._b1_testing import NAME, TRUST
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._probe_testing import (
    LATENCY,
    ROLE_API,
    ROLE_AUDIT,
    TABLE,
    ProbeLab,
    cleanup_calls,
    cleanup_seconds,
    janitor_ports,
    make_probe_lab,
    work_calls,
    work_seconds,
)
from tests.integration.aws_capability._profiles import PROFILES, Root
from tests.integration.aws_capability._safety import tags
from tests.integration.aws_capability.contracts import Outcome
from tests.integration.aws_capability.test_leadingkeys_probe import EXPECTED, SECONDS

PROFILE = PROFILES["iam-leadingkeys"]
SCENARIOS = {
    "tipico": {"ddb_ready_describes": 9, "ddb_deleting_describes": 8, "policy_lag": 3},
    "lento": {
        "ddb_ready_describes": 15, "ddb_deleting_describes": 16,
        "assume_denials": 6, "policy_lag": 7,
    },
    "pior_caso_limitado": {
        "ddb_ready_describes": 29, "ddb_deleting_describes": 28,
        "assume_denials": 11, "policy_lag": 12, "malformed_creates": 3,
    },
}
LIMITS = (270, 150, 285, 330.0)


def cycle(tmp_path: Path, scenario: str, **options: object) -> ProbeLab:
    return make_probe_lab(
        tmp_path / "private",
        "iam-leadingkeys",
        seeded=False,
        latency=LATENCY,
        **{**SCENARIOS[scenario], **options},
    )


def probe_of(outcome: Outcome) -> dict[str, object]:
    value = outcome.evidence["probe"]
    assert isinstance(value, dict)
    return value


def test_perfil_declara_tabela_e_dois_roles_com_limites_dimensionados() -> None:
    assert PROFILE.services == ("dynamodb", "iam")
    assert PROFILE.planned_roots() == (Root("dynamodb"), Root("iam", "audit"), Root("iam", "api"))
    limits = PROFILE.limits
    assert (limits.calls, limits.cleanup_reserve, limits.cleanup_seconds, PROFILE.window) == LIMITS


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_run_completo_remove_tabela_e_os_dois_roles_dentro_do_orcamento_e_da_janela(
    tmp_path: Path, scenario: str
) -> None:
    lab = cycle(tmp_path, scenario)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    limits = PROFILE.limits
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert (lab.aws.tables, lab.aws.roles) == ({}, {})
    assert set(probe_of(outcome)) == set(EXPECTED) | SECONDS
    assert work_calls(lab) <= limits.calls - limits.cleanup_reserve
    assert cleanup_calls(lab) <= limits.cleanup_reserve
    assert work_seconds(lab) * 1.2 <= PROFILE.window
    assert cleanup_seconds(lab) * 1.2 <= limits.cleanup_seconds
    assert outcome.evidence["final_counts_all_cap"] == {
        "cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0,
    }


def test_roles_com_policy_inline_sao_removidos_sem_ownership_mismatch(tmp_path: Path) -> None:
    lab = cycle(tmp_path, "tipico")
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert outcome.reason == "complete"
    deletes = [
        (r.operation, r.payload["RoleName"])
        for r in lab.aws.requests
        if r.operation in {"delete-role-policy", "delete-role"}
    ]
    names = {f"{NAME}-audit", f"{NAME}-api"}
    assert {name for op, name in deletes if op == "delete-role-policy"} == names
    assert {name for op, name in deletes if op == "delete-role"} == names
    for name in names:
        order = [op for op, target in deletes if target == name]
        assert order == ["delete-role-policy", "delete-role"]
    assert set(lab.aws.lanes[lab.aws.operations().index("delete-role-policy") :]) == {"cleanup"}


def test_journal_e_evidencia_nunca_guardam_credenciais(tmp_path: Path) -> None:
    lab = cycle(tmp_path, "tipico")
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    text = lab.spec.journal_dir.joinpath("a1b2c3d4.json").read_text() + repr(outcome)
    assert not any(secret in text for secret in (ACCESS_KEY, SECRET, TOKEN))


@pytest.mark.parametrize("option", [{"policy_lag": 10**6}, {"assume_denials": 10**6}])
def test_controle_que_falha_nao_deixa_nada_e_nenhuma_negacao_vira_evidencia(
    tmp_path: Path, option: dict
) -> None:
    lab = cycle(tmp_path, "tipico", **option)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert probe_of(outcome) == {}
    assert (lab.aws.tables, lab.aws.roles) == ({}, {})
    assert cleanup_seconds(lab) * 1.2 <= PROFILE.limits.cleanup_seconds
    assert cleanup_calls(lab) <= PROFILE.limits.cleanup_reserve


def test_crash_com_journal_incerto_no_meio_da_criacao_remove_todas_as_raizes(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, "tipico")
    prepare_directory(lab.spec.journal_dir)
    lab.aws.tables[NAME] = make_probe_lab(tmp_path / "x", "iam-leadingkeys").aws.tables[NAME]
    lab.aws.tables[NAME].tags = tags(lab.spec)
    for role in (ROLE_AUDIT, ROLE_API):
        lab.aws.roles[role.name] = FakeRole(tags(lab.spec), TRUST)
    audit = lab.aws.roles[ROLE_AUDIT.name]
    audit.policies = {"cap-audit-ddb": (frozenset({"dynamodb:Query"}), 0, {"Version": "x"})}
    journal = Journal(lab.spec)
    journal.created = [TABLE, ROLE_AUDIT]
    journal.phase = "creating"
    journal.pending_service = "iam"
    journal.pending_name = ROLE_API.name
    journal.operation_state = "unknown"
    journal.save()
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert (lab.aws.tables, lab.aws.roles) == ({}, {})
    assert cleanup_calls(lab) <= PROFILE.limits.cleanup_reserve
    assert set(lab.aws.lanes) == {"cleanup"}
