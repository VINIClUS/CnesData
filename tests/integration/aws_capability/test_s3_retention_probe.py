"""Probe s3-retention: GOVERNANCE, bypass e permissões aditivas com controles positivos."""

import json
import traceback
from pathlib import Path

import pytest

from tests.integration.aws_capability._aws_fake_types_testing import ACCESS_KEY, SECRET, TOKEN
from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._probe_testing import (
    BUCKET,
    ProbeLab,
    make_probe_lab,
    payload_text,
)
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure

SECONDS = (
    "assume_seconds",
    "p0_ready_seconds",
    "p1_ready_seconds",
)
EXPECTED = {
    "retained_delete": "AccessDenied",
    "retained_shorten": "AccessDenied",
    "retained_extend": "ok",
    "suspend_versioning": "InvalidBucketState",
    "overwrite_new_version": True,
    "new_version_retention": "NoSuchObjectLockConfiguration",
    "p0_delete_version": "AccessDenied",
    "p0_put_lock_headers": "AccessDenied",
    "p1_delete_bypass": "AccessDenied",
    "p1_delete_retained": "AccessDenied",
    "operator_bypass_delete": "ok",
}


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    return make_probe_lab(tmp_path, "s3-retention")


def run(lab: ProbeLab) -> dict:
    return dict(lab.services.probe(lab.resources, lab.spec, lab.context))


def test_observa_codigos_esperados_e_segundos(lab: ProbeLab) -> None:
    observed = run(lab)
    assert {k: v for k, v in observed.items() if k not in SECONDS} == EXPECTED
    assert set(observed) == set(EXPECTED) | set(SECONDS)
    assert all(isinstance(observed[key], float) and observed[key] >= 0 for key in SECONDS)


def test_so_valores_simples_nas_observacoes(lab: ProbeLab) -> None:
    observed = run(lab)
    assert all(isinstance(value, str | bool | float) for value in observed.values())
    text = json.dumps(observed)
    assert "VERSION_ID_CANARY" not in text
    assert NAME not in text
    assert "arn:" not in text


def test_policies_aditivas_p0_depois_p1_sem_remocao(lab: ProbeLab) -> None:
    run(lab)
    puts = [r for r in lab.aws.requests if r.operation == "put-role-policy"]
    assert [r.payload["PolicyName"] for r in puts] == ["cap-p0", "cap-p1"]
    actions = [
        set(json.loads(str(r.payload["PolicyDocument"]))["Statement"][0]["Action"]) for r in puts
    ]
    assert actions == [
        {"s3:GetObject", "s3:DeleteObject", "s3:PutObject"},
        {"s3:DeleteObjectVersion", "s3:PutObjectRetention"},
    ]
    resource = json.loads(str(puts[0].payload["PolicyDocument"]))["Statement"][0]["Resource"]
    assert resource == f"arn:aws:s3:::{NAME}/*"
    assert "delete-role-policy" not in lab.aws.operations()
    assert "put-role-policy" not in lab.aws.operations("role")


def test_operador_usa_retencao_governance_curta_e_so_o_papel_nega(lab: ProbeLab) -> None:
    run(lab)
    first = next(r for r in lab.aws.requests if r.operation == "put-object")
    assert first.payload["ObjectLockMode"] == "GOVERNANCE"
    assert "Body" in first.payload
    text = payload_text(lab)
    assert "compliance" not in text.lower()
    assert "put-object-lock-configuration" not in lab.aws.operations()


def test_controle_positivo_antecede_cada_negacao_do_papel(lab: ProbeLab) -> None:
    run(lab)
    role_ops = lab.aws.operations("role")
    assert role_ops == [
        "head-object",
        "delete-object",
        "put-object",
        "put-object-retention",
        "delete-object",
        "delete-object",
    ]
    assert lab.aws.operations("operator").count("put-role-policy") == 2


def test_propagacao_lenta_repete_controle_e_registra_segundos(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "s3-retention", policy_lag=4)
    observed = run(lab)
    assert observed["p0_delete_version"] == "AccessDenied"
    assert float(str(observed["p0_ready_seconds"])) > 0
    assert lab.pauses
    assert lab.aws.operations("role").count("head-object") > 1


def test_trust_lento_repete_assume_role(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "s3-retention", assume_denials=3)
    observed = run(lab)
    assert float(str(observed["assume_seconds"])) > 0
    assert lab.aws.operations("operator").count("assume-role") == 4


def test_controle_positivo_que_nunca_fica_verde_nao_registra_negacao(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "s3-retention", policy_lag=10**6)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=control_failed step=p0",)
    assert "delete-object" not in lab.aws.operations("role")
    assert "put-object" not in lab.aws.operations("role")
    assert len(lab.pauses) < 20


def test_assume_role_que_nunca_funciona_falha_com_limite(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "s3-retention", assume_denials=10**6)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=control_failed step=assume",)
    assert lab.aws.operations("operator").count("assume-role") <= 12


def test_erro_diferente_de_acesso_negado_no_controle_nao_repete(lab: ProbeLab) -> None:
    lab.aws.buckets[NAME].versions.clear()
    original = lab.aws.allowed

    def missing_object_after_policy(who: str, action: str) -> bool:
        return original(who, action)

    lab.aws.allowed = missing_object_after_policy
    lab.aws._s3api_head_object = lambda payload, who: __import__(  # type: ignore[method-assign]
        "tests.integration.aws_capability._aws_fake_types_testing", fromlist=["denied"]
    ).denied("NotFound") if who == "role" else None
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=control_failed step=p0",)
    assert lab.aws.operations("role") == ["head-object"]


def test_prazo_curto_interrompe_a_espera_do_controle(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "s3-retention", policy_lag=10**6)
    lab.budget.deadline = lab.clock[0] + 30
    with pytest.raises(ServiceFailure):
        run(lab)
    assert lab.clock[0] <= lab.budget.deadline + 2


def test_bypass_do_operador_negado_vira_observacao(lab: ProbeLab) -> None:
    original = lab.aws.allowed
    lab.aws.allowed = lambda who, action: (
        False if action == "s3:BypassGovernanceRetention" and who == "operator"
        else original(who, action)
    )
    observed = run(lab)
    assert observed["operator_bypass_delete"] == "AccessDenied"
    assert observed["retained_extend"] == "ok"


def test_credenciais_da_sessao_ficam_so_na_memoria(lab: ProbeLab) -> None:
    observed = run(lab)
    secrets = (ACCESS_KEY, SECRET, TOKEN)
    public = json.dumps(observed) + payload_text(lab) + repr(lab.services) + repr(lab.pauses)
    assert not any(secret in public for secret in secrets)


def test_falha_nao_vaza_credenciais_nem_ids(tmp_path: Path) -> None:
    lab = make_probe_lab(tmp_path, "s3-retention", policy_lag=10**6)
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    text = "".join(traceback.format_exception(error.value)) + repr(error.value)
    assert not any(secret in text for secret in (ACCESS_KEY, SECRET, TOKEN, "VERSION_ID"))


@pytest.mark.parametrize("case", ["sem_iam", "ordem", "lane", "extra"])
def test_recusa_recursos_invalidos_sem_io(lab: ProbeLab, case: str) -> None:
    resources, context = lab.resources, lab.context
    if case == "sem_iam":
        resources = [BUCKET]
    elif case == "ordem":
        resources = list(reversed(resources))
    elif case == "extra":
        resources = [*resources, BUCKET]
    else:
        context = RequestContext(lab.budget, "cleanup")
    with pytest.raises(ServiceFailure) as error:
        lab.services.probe(resources, lab.spec, context)
    assert error.value.args == ("probe=s3_invalid",)
    assert lab.aws.calls == []


def test_sessao_ausente_falha_sem_io(lab: ProbeLab) -> None:
    ports = lab.services.ports.__class__(
        lab.services.ports.commands, lab.services.ports.now, lab.services.ports.pause, None, None
    )
    services = lab.services.__class__(lab.spec, ports)
    with pytest.raises(ServiceFailure) as error:
        services.probe(lab.resources, lab.spec, lab.context)
    assert error.value.args[0].startswith("probe=")
