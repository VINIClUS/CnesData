"""Contrato do serviço Compose e do job de CI da camada de emulação Floci (PROD-013)."""
from __future__ import annotations

import configparser
from pathlib import Path
from typing import Any

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW = _ROOT / ".github/workflows/python-quality.yml"
_COMPOSE = _ROOT / "docker-compose.yml"
_PYTEST_INI = _ROOT / "pytest.ini"
_IMAGE = (
    "floci/floci:2.2.0"
    "@sha256:e97cd0c1dc2aa14e7697fb5ef5018404c4d6345169dbd276315f0bed2c7b0520"
)
_ENDPOINT = "http://127.0.0.1:4577"
_CONTRACT = "Floci CI contract"
_START = "Start Floci"
_RUN = "Run Floci integration suite"
_SKIP_CHECK = "Fail when Floci tests were skipped"
_TEARDOWN = "Stop Floci"
_JUNIT = "floci.xml"
_AWS_ENV = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_ENDPOINT_URL",
    "DYNAMODB_ENDPOINT_URL",
)


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _service() -> dict[str, Any]:
    return _load(_COMPOSE)["services"]["floci"]


def _jobs() -> dict[str, Any]:
    return _load(_WORKFLOW)["jobs"]


def _job() -> dict[str, Any]:
    return _jobs()["floci-integration"]


def _names() -> list[str]:
    return [step.get("name", "") for step in _job()["steps"]]


def _step(name: str) -> dict[str, Any]:
    return next(step for step in _job()["steps"] if step.get("name") == name)


def test_servico_floci_fixa_imagem_por_tag_e_digest() -> None:
    assert _service()["image"] == _IMAGE


def test_servico_floci_publica_somente_loopback_sem_colidir_com_localstack() -> None:
    assert _service()["ports"] == ["127.0.0.1:4577:4566"]


def test_servico_floci_fica_isolado_no_proprio_profile() -> None:
    services = _load(_COMPOSE)["services"]
    floci_services = {
        name for name, definition in services.items() if "floci" in definition.get("profiles", ())
    }

    assert _service()["profiles"] == ["floci"]
    assert floci_services == {"floci"}


def test_servico_floci_roda_em_memoria_sem_volume_nem_socket_docker() -> None:
    service = _service()

    assert "volumes" not in service
    assert "docker.sock" not in str(service)
    assert service["environment"]["FLOCI_STORAGE_MODE"] == "memory"


def test_servico_floci_liga_enforcement_iam_e_assinatura_sigv4() -> None:
    environment = _service()["environment"]

    assert environment["FLOCI_SERVICES_IAM_ENFORCEMENT_ENABLED"] == "true"
    assert environment["FLOCI_AUTH_VALIDATE_SIGNATURES"] == "true"
    assert environment["FLOCI_DEFAULT_REGION"] == "us-east-2"
    assert environment["FLOCI_DEFAULT_ACCOUNT_ID"] == "000000000000"
    assert environment["FLOCI_BASE_URL"] == _ENDPOINT


def test_servico_floci_usa_healthcheck_embutido_da_imagem() -> None:
    healthcheck = _service()["healthcheck"]

    assert healthcheck["test"] == ["CMD", "/usr/local/bin/healthcheck.sh"]


def test_emuladores_do_profile_aws_test_permanecem_intactos() -> None:
    services = _load(_COMPOSE)["services"]

    assert services["dynamodb-local"]["ports"] == ["127.0.0.1:18000:8000"]
    assert services["localstack"]["ports"] == ["127.0.0.1:4566:4566"]
    for name in ("dynamodb-local", "localstack"):
        assert services[name]["profiles"] == ["aws-test"]


def test_ci_floci_roda_em_runner_hospedado_com_permissao_minima() -> None:
    job = _job()

    assert job["runs-on"] == "ubuntu-latest"
    assert job["permissions"] == {"contents": "read"}


def test_ci_floci_nao_usa_secrets_id_token_nem_environment() -> None:
    job = _job()

    assert "secrets." not in str(job)
    assert "id-token" not in str(job)
    assert "environment" not in job


def test_ci_floci_usa_as_mesmas_actions_fixadas_do_job_aws() -> None:
    sibling = _jobs()["aws-runtime-integration"]["steps"]
    pinned = [(step["uses"], step.get("with")) for step in sibling if "uses" in step]

    assert [(step["uses"], step.get("with")) for step in _job()["steps"] if "uses" in step] == (
        pinned
    )
    assert "uv sync --locked --all-packages" in [step.get("run") for step in _job()["steps"]]


def test_ci_floci_valida_o_proprio_contrato_antes_de_subir_o_emulador() -> None:
    names = _names()

    assert _step(_CONTRACT)["run"] == "uv run --no-build pytest -q tests/scripts/test_ci_floci.py"
    assert names.index(_CONTRACT) < names.index(_START)


def test_ci_floci_sobe_somente_o_servico_floci_aguardando_saude() -> None:
    assert _step(_START)["run"] == "docker compose --profile floci up -d --wait floci"


def test_ci_floci_executa_suite_com_endpoint_isolado_e_junit() -> None:
    step = _step(_RUN)

    assert step["env"] == {"FLOCI_ENDPOINT_URL": _ENDPOINT}
    assert step["run"] == (
        f"uv run --no-build pytest -rs -m floci tests/integration/floci --junitxml={_JUNIT}"
    )


def test_ci_floci_nao_herda_credenciais_nem_endpoints_aws() -> None:
    job = str(_job())

    for name in _AWS_ENV:
        assert name not in job


def test_ci_floci_falha_quando_algum_teste_e_pulado() -> None:
    check = _step(_SKIP_CHECK)
    names = _names()

    assert _JUNIT in check["run"]
    assert "skipped" in check["run"]
    assert "sys.exit" in check["run"]
    assert names.index(_RUN) < names.index(_SKIP_CHECK) < names.index(_TEARDOWN)


def test_ci_floci_sempre_derruba_o_emulador_por_ultimo() -> None:
    teardown = _step(_TEARDOWN)

    assert teardown["if"] == "always()"
    assert teardown["run"] == "docker compose --profile floci down -v"
    assert _names()[-1] == _TEARDOWN


def test_pytest_ini_declara_marker_floci() -> None:
    parser = configparser.ConfigParser()
    parser.read(_PYTEST_INI, encoding="utf-8")
    lines = parser["pytest"]["markers"].splitlines()

    assert any(line.strip().startswith("floci:") for line in lines)
