"""Ciclo completo run/janitor dos perfis S3 sobre a fronteira com estado, com orçamento medido."""

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
    ROLE,
    cleanup_calls,
    cleanup_seconds,
    janitor_ports,
    make_probe_lab,
    work_seconds,
)
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability._safety import tags


@pytest.mark.parametrize("probe", ["s3-hold", "s3-retention"])
def test_run_completo_remove_tudo_dentro_do_orcamento_do_perfil(tmp_path: Path, probe: str) -> None:
    lab = make_probe_lab(tmp_path / "private", probe, seeded=False, latency=LATENCY)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    limits = PROFILES[probe].limits
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.buckets == {}
    assert lab.aws.roles == {}
    assert lab.aws.lanes.count("work") <= limits.calls - limits.cleanup_reserve
    assert cleanup_calls(lab) <= limits.cleanup_reserve
    assert cleanup_seconds(lab) < limits.cleanup_seconds * 0.7
    assert work_seconds(lab) <= PROFILES[probe].window
    assert outcome.evidence["final_counts_all_cap"] == {
        "cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0,
    }


@pytest.mark.parametrize("probe", ["s3-hold", "s3-retention"])
def test_journal_e_evidencia_nunca_guardam_credenciais_nem_ids_de_versao(
    tmp_path: Path, probe: str
) -> None:
    lab = make_probe_lab(tmp_path / "private", probe, seeded=False)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    text = lab.spec.journal_dir.joinpath("a1b2c3d4.json").read_text() + repr(outcome)
    assert not any(secret in text for secret in (ACCESS_KEY, SECRET, TOKEN, "VERSION_ID"))


def test_retencao_falha_no_controle_e_a_cleanup_ainda_remove_tudo(tmp_path: Path) -> None:
    lab = make_probe_lab(
        tmp_path / "private", "s3-retention", seeded=False, latency=LATENCY, policy_lag=10**6
    )
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert lab.aws.buckets == {}
    assert lab.aws.roles == {}
    assert cleanup_seconds(lab) < PROFILES["s3-retention"].limits.cleanup_seconds


@pytest.mark.parametrize("probe", ["s3-hold", "s3-retention"])
def test_janitor_remove_versao_retida_com_hold_ligado_e_policies(
    tmp_path: Path, probe: str
) -> None:
    lab = make_probe_lab(tmp_path / "private", probe, seeded=False, latency=LATENCY)
    prepare_directory(lab.spec.journal_dir)
    bucket = FakeBucket(tags(lab.spec))
    bucket.versions = [
        FakeVersion("k", "V1", "ON", lab.clock[0] + 600),
        FakeVersion("k", "V2", None, lab.clock[0] + 900),
        FakeVersion("j", "V3", None, 0.0),
    ]
    lab.aws.buckets[NAME] = bucket
    created = [BUCKET]
    if probe == "s3-retention":
        role = FakeRole(tags(lab.spec), TRUST)
        role.policies = {
            "cap-p0": (frozenset({"s3:GetObject"}), 0, {"Version": "2012-10-17"}),
            "cap-p1": (frozenset({"s3:PutObject"}), 0, {"Version": "2012-10-17"}),
        }
        lab.aws.roles[NAME] = role
        created.append(ROLE)
    journal = Journal(lab.spec)
    journal.created = created
    journal.phase = "cleanup_requested"
    journal.save()
    outcome = lifecycle.janitor(lab.spec, janitor_ports(lab))
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.buckets == {}
    assert lab.aws.roles == {}
    assert cleanup_calls(lab) <= PROFILES[probe].limits.cleanup_reserve
    assert cleanup_seconds(lab) < PROFILES[probe].limits.cleanup_seconds
    assert set(lab.aws.lanes) == {"cleanup"}
    assert "put-object-legal-hold" in lab.aws.operations()
    deletes = [r for r in lab.aws.requests if r.operation == "delete-object"]
    assert deletes
    assert all(r.payload["BypassGovernanceRetention"] is True for r in deletes)


def test_delete_de_versao_negado_ao_papel_falha_e_a_cleanup_remove_tudo(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "s3-retention", seeded=False, latency=LATENCY)
    original = lab.aws.allowed
    lab.aws.allowed = lambda who, action: (
        False if action == "s3:DeleteObjectVersion" and who == "role"
        else original(who, action)
    )
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert lab.aws.buckets == {}
    assert lab.aws.roles == {}


def test_put_negado_ao_papel_falha_e_a_cleanup_remove_tudo(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path / "private", "s3-retention", seeded=False, latency=LATENCY)
    original = lab.aws.allowed
    lab.aws.allowed = lambda who, action: (
        False if action == "s3:PutObject" and who == "role" else original(who, action)
    )
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert lab.aws.buckets == {}
    assert lab.aws.roles == {}
