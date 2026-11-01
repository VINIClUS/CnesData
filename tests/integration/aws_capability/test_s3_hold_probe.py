"""Probe s3-hold: legal hold ligado bloqueia delete mesmo com bypass até ser desligado."""

import json
import traceback
from pathlib import Path

import pytest

from tests.integration.aws_capability._b1_testing import NAME
from tests.integration.aws_capability._probe_testing import (
    BUCKET,
    ROLE,
    ProbeLab,
    make_probe_lab,
    payload_text,
)
from tests.integration.aws_capability._service_contracts import ApiError, Reply
from tests.integration.aws_capability.contracts import RequestContext, ServiceFailure


@pytest.fixture
def lab(tmp_path: Path) -> ProbeLab:
    return make_probe_lab(tmp_path, "s3-hold")


def run(lab: ProbeLab) -> dict:
    return dict(lab.services.probe(lab.resources, lab.spec, lab.context))


def test_observa_apenas_codigos_do_hold(lab: ProbeLab) -> None:
    assert run(lab) == {
        "hold_delete": "AccessDenied",
        "hold_delete_bypass": "AccessDenied",
        "hold_off_delete": "ok",
    }
    assert lab.aws.buckets[NAME].versions == []


def test_sequencia_de_operacoes_e_cabecalhos(lab: ProbeLab) -> None:
    run(lab)
    steps = [(r.operation, dict(r.payload)) for r in lab.aws.requests]
    assert [op for op, _ in steps] == [
        "put-object",
        "put-object-legal-hold",
        "delete-object",
        "delete-object",
        "put-object-legal-hold",
        "delete-object",
    ]
    assert steps[1][1]["LegalHold"] == {"Status": "ON"}
    assert "BypassGovernanceRetention" not in steps[2][1]
    assert steps[3][1]["BypassGovernanceRetention"] is True
    assert steps[4][1]["LegalHold"] == {"Status": "OFF"}
    assert "BypassGovernanceRetention" not in steps[5][1]
    assert all("ObjectLockMode" not in payload for _, payload in steps)


def test_nunca_constroi_compliance_nem_configura_lock(lab: ProbeLab) -> None:
    run(lab)
    text = payload_text(lab)
    assert "compliance" not in text.lower()
    assert "put-object-lock-configuration" not in lab.aws.operations()


def test_observacoes_nao_expoem_ids_de_versao(lab: ProbeLab) -> None:
    observed = run(lab)
    assert "VERSION_ID_CANARY" not in json.dumps(observed)
    assert NAME not in json.dumps(observed)


def test_delete_final_que_falha_invalida_o_probe_sem_observacoes(lab: ProbeLab) -> None:
    lab.aws.allowed = lambda who, action: action != "s3:DeleteObjectVersion"
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=control_failed step=hold_off_delete",)


def test_falha_ao_gravar_o_objeto_inicial_aborta(lab: ProbeLab) -> None:
    lab.aws.allowed = lambda who, action: action != "s3:PutObject"
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    assert error.value.args == ("probe=step_failed step=put",)
    assert lab.aws.operations() == ["put-object"]


def test_falha_de_transporte_nao_vira_negacao(lab: ProbeLab) -> None:
    class Broken:
        def call(self, request: object, context: object) -> Reply:
            return Reply(False, {}, ApiError("operation_failed", {}, "local"))

    ports = lab.services.ports.__class__(
        Broken(), lab.services.ports.now, lab.services.ports.pause, None, None
    )
    services = lab.services.__class__(lab.spec, ports)
    with pytest.raises(ServiceFailure) as error:
        services.probe(lab.resources, lab.spec, lab.context)
    assert error.value.args == ("probe=transport_failed",)


@pytest.mark.parametrize("case", ["extra", "iam", "outro_nome", "spec", "lane"])
def test_recusa_entrada_invalida_sem_io(lab: ProbeLab, case: str) -> None:
    resources, spec, context = lab.resources, lab.spec, lab.context
    if case == "extra":
        resources = [BUCKET, BUCKET]
    elif case == "iam":
        resources = [ROLE]
    elif case == "outro_nome":
        resources = [BUCKET.__class__("s3", "outro", "outro")]
    elif case == "spec":
        spec = lab.spec.__class__(**{**lab.spec.__dict__, "run_id": "ffffffff"})
    else:
        context = RequestContext(lab.budget, "cleanup")
    with pytest.raises(ServiceFailure) as error:
        lab.services.probe(resources, spec, context)
    assert error.value.args == ("probe=s3_invalid",)
    assert lab.aws.calls == []


def test_falha_nao_vaza_ids_de_versao(lab: ProbeLab) -> None:
    lab.aws.allowed = lambda who, action: action != "s3:DeleteObjectVersion"
    with pytest.raises(ServiceFailure) as error:
        run(lab)
    text = "".join(traceback.format_exception(error.value))
    assert "VERSION_ID_CANARY" not in text
