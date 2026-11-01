"""Ciclo completo do iam-s3 sobre a fronteira com estado, com orçamento medido."""

from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
from tests.integration.aws_capability._aws_fake_types_testing import (
    ACCESS_KEY,
    SECRET,
    TOKEN,
    FakeBucket,
    FakeRole,
    FakeVersion,
)
from tests.integration.aws_capability._b1_testing import NAME, TRUST
from tests.integration.aws_capability._journal import Journal, prepare_directory
from tests.integration.aws_capability._probe_testing import (
    BUCKET,
    LATENCY,
    ROLE_API,
    ROLE_AUDIT,
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
from tests.integration.aws_capability.test_iam_s3_probe import CODES, SECONDS

PROFILE = PROFILES["iam-s3"]
SCENARIOS = {
    "tipico": {"policy_lag": 3},
    "lento": {"assume_denials": 6, "policy_lag": 7},
    "pior_caso_limitado": {"assume_denials": 11, "policy_lag": 12, "malformed_creates": 3},
}
LIMITS = (310, 220, 335, 220.0)


def cycle(tmp_path: Path, scenario: str, **options: object) -> ProbeLab:
    return make_probe_lab(
        tmp_path / "private",
        "iam-s3",
        seeded=False,
        latency=LATENCY,
        **{**SCENARIOS[scenario], **options},
    )


def probe_of(outcome: Outcome) -> dict[str, object]:
    value = outcome.evidence["probe"]
    assert isinstance(value, dict)
    return value


def test_perfil_declara_bucket_e_dois_roles_com_limites_dimensionados() -> None:
    assert PROFILE.services == ("s3", "iam")
    assert PROFILE.planned_roots() == (Root("s3"), Root("iam", "audit"), Root("iam", "api"))
    limits = PROFILE.limits
    assert (limits.calls, limits.cleanup_reserve, limits.cleanup_seconds, PROFILE.window) == LIMITS
    assert limits.cleanup_seconds >= 240


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_run_completo_remove_bucket_e_os_dois_roles_dentro_do_orcamento_e_da_janela(
    tmp_path: Path, scenario: str
) -> None:
    lab = cycle(tmp_path, scenario)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    limits = PROFILE.limits
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert (lab.aws.buckets, lab.aws.roles) == ({}, {})
    assert set(probe_of(outcome)) == set(CODES) | SECONDS
    assert work_calls(lab) <= limits.calls - limits.cleanup_reserve
    assert cleanup_calls(lab) <= limits.cleanup_reserve
    assert work_seconds(lab) * 1.2 <= PROFILE.window
    assert cleanup_seconds(lab) * 1.2 <= limits.cleanup_seconds
    assert outcome.evidence["final_counts_all_cap"] == {
        "cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0,
    }


def test_versoes_retidas_sao_removidas_com_bypass_e_roles_depois_das_policies(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, "tipico")
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert outcome.reason == "complete"
    deletes = [
        r for r, lane in zip(lab.aws.requests, lab.aws.lanes, strict=True)
        if r.operation == "delete-object" and lane == "cleanup"
    ]
    assert len(deletes) == 6
    assert all(r.payload["BypassGovernanceRetention"] is True for r in deletes)
    for name in (f"{NAME}-audit", f"{NAME}-api"):
        removals = {"delete-role-policy", "delete-role"}
        order = [
            r.operation for r in lab.aws.requests
            if r.operation in removals and r.payload["RoleName"] == name
        ]
        assert order == ["delete-role-policy", "delete-role"]


def test_journal_e_evidencia_nunca_guardam_credenciais_nem_ids_de_versao(tmp_path: Path) -> None:
    lab = cycle(tmp_path, "tipico")
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    text = lab.spec.journal_dir.joinpath("a1b2c3d4.json").read_text() + repr(outcome)
    assert not any(s in text for s in (ACCESS_KEY, SECRET, TOKEN, "VERSION_ID"))


@pytest.mark.parametrize("option", [{"policy_lag": 10**6}, {"assume_denials": 10**6}])
def test_controle_que_falha_nao_deixa_nada_e_nenhuma_negacao_vira_evidencia(
    tmp_path: Path, option: dict
) -> None:
    lab = cycle(tmp_path, "tipico", **option)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert probe_of(outcome) == {}
    assert (lab.aws.buckets, lab.aws.roles) == ({}, {})
    assert cleanup_seconds(lab) * 1.2 <= PROFILE.limits.cleanup_seconds
    assert cleanup_calls(lab) <= PROFILE.limits.cleanup_reserve


def test_crash_com_journal_incerto_no_meio_da_criacao_remove_todas_as_raizes(
    tmp_path: Path,
) -> None:
    lab = cycle(tmp_path, "tipico")
    prepare_directory(lab.spec.journal_dir)
    bucket = FakeBucket(tags(lab.spec))
    bucket.versions = [
        FakeVersion("audit/b", "V1", "ON", lab.clock[0] + 600),
        FakeVersion("serving/x", "V2", None, 0.0),
    ]
    lab.aws.buckets[NAME] = bucket
    for role in (ROLE_AUDIT, ROLE_API):
        lab.aws.roles[role.name] = FakeRole(tags(lab.spec), TRUST)
    lab.aws.roles[ROLE_AUDIT.name].policies = {
        "cap-audit-s3": (frozenset({"s3:GetObject"}), 0, {"Version": "x"})
    }
    journal = Journal(lab.spec)
    journal.created = [BUCKET, ROLE_AUDIT]
    journal.phase = "creating"
    journal.pending_service = "iam"
    journal.operation_state = "unknown"
    journal.save()
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert (lab.aws.buckets, lab.aws.roles) == ({}, {})
    assert cleanup_calls(lab) <= PROFILE.limits.cleanup_reserve
    assert set(lab.aws.lanes) == {"cleanup"}
