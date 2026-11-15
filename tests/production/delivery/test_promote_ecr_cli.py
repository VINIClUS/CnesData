"""CLI Phase A: promote-ecr grava o recibo local e phase-inputs o consome sem S3."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

from release.verify_candidate import canonical
from scripts.promotion.phase_a import prepare_inputs
from scripts.promotion.promote_ecr import promote_candidate
from tests.production.delivery import test_candidate_manifest as candidate_fixtures
from tests.production.delivery._phase_a_fixtures import (
    ACCOUNT,
    NOW,
    REGION,
    SHA,
    RegistryBoundary,
    _candidate_reference,
    _creator_pin,
)

github = candidate_fixtures.github
ROOT = Path(__file__).resolve().parents[3]
PRINCIPAL = f"arn:aws:iam::{ACCOUNT}:role/canary-trusted"
BASELINE = {
    "processor_active_unit_task_definition_arn": "arn:aws:ecs:us-east-2:1:task-definition/u:3",
    "processor_retained_unit_task_definition_arns": ["arn:aws:ecs:us-east-2:1:task-definition/u:2"],
}
LEAK = "ghs_cli_secret_value"


def _cli():
    return import_module("scripts.promotion.phase_a_cli")


@pytest.fixture
def staged(github, tmp_path):
    registry = RegistryBoundary()
    reference = _candidate_reference(github, registry)
    for name, value in (
        ("reference", reference),
        ("creator", _creator_pin()),
        ("baseline", BASELINE),
    ):
        (tmp_path / f"{name}.json").write_text(json.dumps(value), encoding="utf-8")
    return SimpleNamespace(
        registry=registry,
        github=github,
        repositories=[],
        reference=reference,
        dir=tmp_path,
        receipt=tmp_path / "promotion.json",
        inputs=tmp_path / "phase-inputs.json",
    )


def _promote(staged, creator=None, registry=None):
    seams = {
        "api_factory": lambda repository: staged.repositories.append(repository) or staged.github,
        "registry_factory": lambda: registry or staged.registry,
        "clock": lambda: NOW,
    }
    argv = [
        "promote-ecr",
        "--reference",
        str(staged.dir / "reference.json"),
        "--creator",
        str(creator or staged.dir / "creator.json"),
        "--account-id",
        ACCOUNT,
        "--region",
        REGION,
        "--output",
        str(staged.receipt),
    ]
    return _cli().main(argv, **seams)


def _inputs(staged, source_sha=SHA):
    argv = [
        "phase-inputs",
        "--promotion",
        str(staged.receipt),
        "--baseline",
        str(staged.dir / "baseline.json"),
        "--account-id",
        ACCOUNT,
        "--region",
        REGION,
        "--source-sha",
        source_sha,
        "--canary-trusted-principal-arn",
        PRINCIPAL,
        "--output",
        str(staged.inputs),
    ]
    return _cli().main(argv)


def _governed():
    return {
        "account_id": ACCOUNT,
        "region": REGION,
        "source_sha": SHA,
        "canary_trusted_principal_arn": PRINCIPAL,
    }


def test_promote_ecr_grava_o_recibo_que_promote_candidate_produz(staged):
    expected = promote_candidate(
        staged.reference,
        staged.github,
        RegistryBoundary(),
        {"account_id": ACCOUNT, "region": REGION, "now": NOW, "candidate_creator": _creator_pin()},
    )
    assert _promote(staged) == 0
    assert json.loads(staged.receipt.read_bytes()) == json.loads(canonical(expected))


def test_promote_ecr_grava_bytes_canonicos_e_copia_para_o_registry(staged):
    assert _promote(staged) == 0
    data = staged.receipt.read_bytes()
    assert data == canonical(json.loads(data))
    assert staged.registry.copied
    assert staged.repositories == ["VINIClUS/CnesData"]
    assert list(staged.registry.tags) == [json.loads(data)["candidate"]["release_id"]]


def test_promote_ecr_recusa_criador_divergente_sem_copiar_nem_gravar(staged):
    other = staged.dir / "other.json"
    other.write_text(json.dumps({**_creator_pin(), "workflow_sha": "c" * 40}), encoding="utf-8")
    assert _promote(staged, creator=other) == 2
    assert staged.registry.copied == []
    assert not staged.receipt.exists()


def test_promote_ecr_nao_expoe_o_token_no_recibo_nem_no_log(staged, monkeypatch, caplog):
    monkeypatch.setenv("GITHUB_TOKEN", LEAK)
    with caplog.at_level(logging.DEBUG):
        assert _promote(staged) == 0
    assert LEAK not in staged.receipt.read_text(encoding="utf-8")
    assert LEAK not in caplog.text


@pytest.mark.parametrize(
    "error",
    [
        ClientError({"Error": {"Code": "AccessDenied", "Message": "m"}}, "PutImage"),
        OSError("rede"),
        KeyError("campo"),
    ],
    ids=lambda error: type(error).__name__,
)
def test_promote_ecr_falha_fechada_com_codigo_2_e_sem_recibo(staged, caplog, error):
    def broken():
        raise error

    with caplog.at_level(logging.ERROR):
        status = _cli().main(
            [
                "promote-ecr",
                "--reference",
                str(staged.dir / "reference.json"),
                "--creator",
                str(staged.dir / "creator.json"),
                "--account-id",
                ACCOUNT,
                "--region",
                REGION,
                "--output",
                str(staged.receipt),
            ],
            api_factory=lambda repository: staged.github,
            registry_factory=broken,
            clock=lambda: NOW,
        )
    assert status == 2
    assert f"promote_ecr=failed reason={type(error).__name__}" in caplog.text
    assert not staged.receipt.exists()


def test_phase_inputs_le_o_recibo_gravado_e_monta_os_inputs_do_prepare_inputs(staged):
    assert _promote(staged) == 0
    assert _inputs(staged) == 0
    receipt = json.loads(staged.receipt.read_bytes())
    expected = prepare_inputs(receipt, BASELINE, _governed())
    written = json.loads(staged.inputs.read_bytes())
    assert written == expected
    assert written["image_digest"] == receipt["image_digest"]
    assert (
        written["processor_active_unit_task_definition_arn"]
        == (BASELINE["processor_active_unit_task_definition_arn"])
    )
    assert staged.inputs.read_bytes() == canonical(written)


def test_phase_inputs_recusa_recibo_adulterado_sem_gravar(staged, caplog):
    assert _promote(staged) == 0
    receipt = json.loads(staged.receipt.read_bytes())
    receipt["image_digest"] = "sha256:" + "e" * 64
    staged.receipt.write_text(json.dumps(receipt), encoding="utf-8")
    with caplog.at_level(logging.ERROR):
        assert _inputs(staged) == 2
    assert "promotion=image_binding" in caplog.text
    assert not staged.inputs.exists()


def test_phase_inputs_recusa_source_sha_divergente_do_candidato(staged):
    assert _promote(staged) == 0
    assert _inputs(staged, source_sha="d" * 40) == 2
    assert not staged.inputs.exists()


def test_phase_inputs_recusa_baseline_com_chaves_extras(staged):
    assert _promote(staged) == 0
    (staged.dir / "baseline.json").write_text(json.dumps({**BASELINE, "extra": 1}))
    assert _inputs(staged) == 2
    assert not staged.inputs.exists()


def test_phase_inputs_sem_recibo_falha_fechada(staged):
    assert _inputs(staged) == 2
    assert not staged.inputs.exists()


@pytest.mark.parametrize(
    "argv", [[], ["outro"], ["promote-ecr"], ["phase-inputs"], ["phase-inputs", "--x", "1"]]
)
def test_argumentos_invalidos_saem_com_codigo_2_do_argparse(argv):
    with pytest.raises(SystemExit) as stopped:
        _cli().main(argv)
    assert stopped.value.code == 2


def test_modulo_executa_por_python_m_com_os_dois_subcomandos():
    done = subprocess.run(
        [sys.executable, "-m", "scripts.promotion.phase_a_cli", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        stdin=subprocess.DEVNULL,
    )
    assert done.returncode == 0
    assert "promote-ecr" in done.stdout
    assert "phase-inputs" in done.stdout


def test_modulo_propaga_o_codigo_de_saida_da_recusa(tmp_path):
    arguments = ["promotion", "baseline", "account-id", "region", "source-sha"]
    arguments += ["canary-trusted-principal-arn", "output"]
    argv = [item for name in arguments for item in (f"--{name}", str(tmp_path / name))]
    done = subprocess.run(  # noqa: S603 -- Executes the checked-in CLI with fixed arguments.
        [sys.executable, "-m", "scripts.promotion.phase_a_cli", "phase-inputs", *argv],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        stdin=subprocess.DEVNULL,
    )
    assert done.returncode == 2
