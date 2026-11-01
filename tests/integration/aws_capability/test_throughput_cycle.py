"""Ciclo completo do ddb-throughput sobre a fronteira com estado, com orçamento medido."""

from pathlib import Path

import pytest

from tests.integration.aws_capability import lifecycle
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
from tests.integration.aws_capability._profiles import PROFILES
from tests.integration.aws_capability.contracts import Outcome

PROFILE = PROFILES["ddb-throughput"]
SCENARIOS = {"tipico": (9, 8), "pior_caso_limitado": (29, 28)}
PROBE_KEYS = {
    "tx_code", "tx_reason_codes", "batch_accepted_items", "batch_elapsed_seconds",
    "batch_rate_items_per_second", "readback_code", "readback_count",
}


def probe_of(outcome: Outcome) -> dict[str, object]:
    value = outcome.evidence["probe"]
    assert isinstance(value, dict)
    return value


def cycle(tmp_path: Path, scenario: str, **options: object) -> ProbeLab:
    ready, deleting = SCENARIOS[scenario]
    return make_probe_lab(
        tmp_path / "private",
        "ddb-throughput",
        seeded=False,
        latency=LATENCY,
        ddb_ready_describes=ready,
        ddb_deleting_describes=deleting,
        **options,
    )


def test_perfil_declara_teto_de_limites_e_janela_dimensionados() -> None:
    assert PROFILE.services == ("dynamodb",)
    assert PROFILE.window == 140.0
    assert (PROFILE.limits.calls, PROFILE.limits.cleanup_seconds) == (128, 150)
    assert PROFILE.limits.cleanup_reserve == 64


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_run_completo_remove_tudo_dentro_do_orcamento_e_da_janela(
    tmp_path: Path, scenario: str
) -> None:
    lab = cycle(tmp_path, scenario)
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    limits = PROFILE.limits
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert lab.aws.tables == {}
    assert work_calls(lab) <= limits.calls - limits.cleanup_reserve
    assert cleanup_calls(lab) <= limits.cleanup_reserve
    assert work_seconds(lab) <= PROFILE.window
    assert cleanup_seconds(lab) <= limits.cleanup_seconds
    assert set(probe_of(outcome)) >= PROBE_KEYS
    assert outcome.evidence["final_counts_all_cap"] == {
        "cognito": 0, "dynamodb": 0, "s3": 0, "iam": 0,
    }


def test_cleanup_desliga_protecao_antes_de_apagar_a_tabela_criada_com_ela(tmp_path: Path) -> None:
    lab = cycle(tmp_path, "tipico")
    lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    operations = lab.aws.operations()
    assert operations.index("create-table") < operations.index("transact-write-items")
    assert operations.index("update-table") < operations.index("delete-table")
    update = next(r for r in lab.aws.requests if r.operation == "update-table")
    assert update.payload == {"TableName": NAME, "DeletionProtectionEnabled": False}
    assert set(lab.aws.lanes[operations.index("update-table") :]) == {"cleanup"}


def test_divergencia_de_teto_falha_o_probe_e_a_cleanup_remove_tudo(tmp_path: Path) -> None:
    lab = cycle(tmp_path, "tipico")
    original = lab.aws._dynamodb_create_table

    def lower_ceiling(payload: dict, who: str):
        reply = original(payload, who)
        lab.aws.tables[NAME].throughput = {"MaxReadRequestUnits": 5, "MaxWriteRequestUnits": 5}
        return reply

    lab.aws._dynamodb_create_table = lower_ceiling  # type: ignore[method-assign]
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert lab.aws.tables == {}
    assert "transact-write-items" not in lab.aws.operations()


def test_acesso_negado_na_carga_falha_o_probe_e_a_cleanup_remove_tudo(tmp_path: Path) -> None:
    lab = cycle(tmp_path, "tipico", ddb_errors={"scan": "AccessDeniedException"})
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    assert (outcome.status, outcome.reason) == ("failed", "operation_failed")
    assert lab.aws.tables == {}


def test_throttle_e_observacao_e_o_run_termina_com_sucesso(tmp_path: Path) -> None:
    lab = cycle(
        tmp_path,
        "tipico",
        ddb_tx_reasons={"None": 90, "ThrottlingError": 10},
        batch_unprocessed={3: 7},
    )
    outcome = lifecycle.run(lab.spec, janitor_ports(lab, supervised=True))
    probe = probe_of(outcome)
    assert (outcome.status, outcome.reason) == ("succeeded", "complete")
    assert probe["tx_reason_codes"] == {"None": 90, "ThrottlingError": 10}
    assert probe["batch_3_unprocessed"] == 7
    assert probe["readback_count"] == 93
