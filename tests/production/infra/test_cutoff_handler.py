import importlib.util
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

REPO_ROOT = Path(__file__).resolve().parents[3]
HANDLER = REPO_ROOT / "infra" / "opentofu" / "modules" / "athena" / "cutoff_handler.py"
WORKGROUP = "cnesdata-prod-operator"
VARIABLE = "ATHENA_WORKGROUP"
COVERAGE_CHILD = "CNESDATA_CUTOFF_COVERAGE_CHILD"
SNS_EVENT = {
    "Records": [
        {
            "EventSource": "aws:sns",
            "Sns": {
                "Subject": "ALARM: cnesdata-prod-athena-cutoff",
                "Message": '{"WorkGroup": "primary", "State": "ENABLED"}',
            },
        }
    ]
}
FOREIGN_EVENT = {"WorkGroup": "primary", "workgroup": "primary", "State": "ENABLED"}
EXPECTED_PARAMS = {"WorkGroup": WORKGROUP, "State": "DISABLED"}


def _load() -> ModuleType:
    assert HANDLER.is_file(), f"handler_missing path={HANDLER}"
    spec = importlib.util.spec_from_file_location("cutoff_handler_under_test", HANDLER)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setenv(VARIABLE, WORKGROUP)
    return _load()


@pytest.fixture(name="athena")
def fixture_athena() -> Iterator[tuple[object, Stubber]]:
    client = boto3.client("athena", region_name="us-east-2")
    with Stubber(client) as stubber:
        yield client, stubber


def test_handler_desabilita_o_workgroup_exato(
    monkeypatch: pytest.MonkeyPatch, athena: tuple[object, Stubber]
) -> None:
    client, stubber = athena
    stubber.add_response("update_work_group", {}, EXPECTED_PARAMS)

    result = _module(monkeypatch).handler(SNS_EVENT, None, client)

    assert result == {"workgroup": WORKGROUP, "state": "DISABLED"}
    stubber.assert_no_pending_responses()


@pytest.mark.parametrize("event", [SNS_EVENT, FOREIGN_EVENT, {}, None])
def test_handler_ignora_o_workgroup_do_evento(
    monkeypatch: pytest.MonkeyPatch, athena: tuple[object, Stubber], event: object
) -> None:
    client, stubber = athena
    stubber.add_response("update_work_group", {}, EXPECTED_PARAMS)

    _module(monkeypatch).handler(event, None, client)

    stubber.assert_no_pending_responses()


def test_handler_falha_fechado_em_erro_da_api(
    monkeypatch: pytest.MonkeyPatch, athena: tuple[object, Stubber]
) -> None:
    client, stubber = athena
    stubber.add_client_error(
        "update_work_group",
        service_error_code="AccessDeniedException",
        http_status_code=400,
        expected_params=EXPECTED_PARAMS,
    )

    with pytest.raises(ClientError) as raised:
        _module(monkeypatch).handler(SNS_EVENT, None, client)

    assert raised.value.response["Error"]["Code"] == "AccessDeniedException"
    stubber.assert_no_pending_responses()


@pytest.mark.parametrize("value", [None, ""])
def test_handler_levanta_sem_variavel_de_ambiente(
    monkeypatch: pytest.MonkeyPatch, athena: tuple[object, Stubber], value: str | None
) -> None:
    module = _load()
    if value is None:
        monkeypatch.delenv(VARIABLE, raising=False)
    else:
        monkeypatch.setenv(VARIABLE, value)
    client, _ = athena

    with pytest.raises(RuntimeError, match=f"env={VARIABLE} reason=missing"):
        module.handler(SNS_EVENT, None, client)


def test_handler_cria_o_cliente_athena_quando_nao_recebe_um(
    monkeypatch: pytest.MonkeyPatch,
    athena: tuple[object, Stubber],
) -> None:
    client, stubber = athena
    stubber.add_response("update_work_group", {}, EXPECTED_PARAMS)
    created: list[tuple[object, ...]] = []

    def factory(*arguments: object, **options: object) -> object:
        created.append((*arguments, *sorted(options.items())))
        return client

    monkeypatch.setattr(boto3, "client", factory)

    _module(monkeypatch).handler(SNS_EVENT, None)

    assert created
    assert created[0][0] == "athena"
    stubber.assert_no_pending_responses()


def test_handler_tem_cobertura_total_de_branch(tmp_path: Path) -> None:
    if os.environ.get(COVERAGE_CHILD):
        pytest.skip("coverage_child")
    assert HANDLER.is_file(), f"handler_missing path={HANDLER}"
    data_file = tmp_path / "coverage.data"
    environment = {**os.environ, COVERAGE_CHILD: "1", "PYTHONPATH": str(REPO_ROOT)}
    for key in [name for name in environment if name.startswith("PYTEST_")]:
        del environment[key]
    run = [
        sys.executable, "-P", "-m", "coverage", "run", "--branch", f"--data-file={data_file}",
        f"--include={HANDLER}", "--rcfile=/dev/null", "-m", "pytest", "-q", "-p",
        "no:cacheprovider", str(Path(__file__).resolve()),
    ]
    executed = subprocess.run(  # noqa: S603
        run, check=False, capture_output=True, cwd=tmp_path, env=environment, text=True
    )
    assert executed.returncode == 0, f"child_failed stdout={executed.stdout!r}"
    report = [
        sys.executable, "-P", "-m", "coverage", "report", f"--data-file={data_file}",
        "--rcfile=/dev/null", "--fail-under=100", "--show-missing", f"--include={HANDLER}",
    ]
    reported = subprocess.run(  # noqa: S603
        report, check=False, capture_output=True, cwd=tmp_path, env=environment, text=True
    )
    assert reported.returncode == 0, f"branch_coverage={reported.stdout!r}"
