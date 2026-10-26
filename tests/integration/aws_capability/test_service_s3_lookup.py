"""Lookup S3 distingue ausência conclusiva, races e falhas locais."""

import copy
import logging
import subprocess
import sys
import traceback
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest

from tests.integration.aws_capability._service_contracts import ApiError, Reply
from tests.integration.aws_capability._service_read_assertions import assert_private, assert_process
from tests.integration.aws_capability._service_read_testing import CANARY
from tests.integration.aws_capability._service_roots_testing import (
    LIST,
    LOCATION,
    ROOT,
    TAGS,
    ReadLab,
    TeardownProbe,
    fixture_data,
    make_read_lab,
    respond,
    respond_error,
    teardown_probe_source,
)
from tests.integration.aws_capability.cli import Request
from tests.integration.aws_capability.contracts import Ownership, ServiceFailure


@pytest.fixture
def read_lab(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[ReadLab]:
    lab = make_read_lab(monkeypatch, tmp_path, caplog, capsys)
    yield lab
    lab.check_private()


def test_lookup_none_exige_no_such_bucket_remoto_em_location_exata(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_no_bucket")
    owner = read_lab.services.lookup(ROOT, read_lab.context)
    assert owner is None
    read_lab.check_private(owner)
    assert_process(read_lab, [LOCATION])


@pytest.mark.parametrize(
    "failure",
    [
        "local_no_bucket",
        "remote_denied",
        "remote_403",
        "remote_404",
        "remote_no_tags",
        "bad_json",
        "empty_stdout",
        "empty_object",
        "bad_stderr",
        "wrong_operation",
    ],
)
def test_location_inconclusiva_nunca_retorna_none(
    read_lab: ReadLab,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    expected = (
        "service=describe_invalid" if failure == "empty_object" else "service=describe_failed"
    )
    assert error.value.args == (expected,)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [LOCATION])


@pytest.mark.parametrize(
    "failure",
    [
        "remote_no_bucket",
        "local_no_bucket",
        "local_no_tags",
        "remote_denied",
        "remote_403",
        "remote_404",
        "bad_json",
        "empty_stdout",
        "bad_stderr",
    ],
)
def test_erro_em_tags_depois_de_location_valida_nunca_e_ausencia(
    read_lab: ReadLab,
    failure: str,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respond(read_lab, fixture_data("s3")["location_east"])
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert error.value.args == ("service=request_failed",)
    captured = capsys.readouterr()
    assert_private(error.value, read_lab, caplog.text + captured.out + captured.err)
    assert_process(read_lab, [LOCATION, TAGS])


def test_timeout_em_location_falha_sem_retry_ou_ausencia(read_lab: ReadLab) -> None:
    read_lab.process.outcomes.append(subprocess.TimeoutExpired("aws", 7))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert error.value.args == ("service=describe_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [LOCATION])


def test_timeout_em_tags_nao_apaga_presenca_confirmada(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["location_east"])
    read_lab.process.outcomes.append(subprocess.TimeoutExpired("aws", 7))
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.lookup(ROOT, read_lab.context)
    assert error.value.args == ("service=request_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [LOCATION, TAGS])


def test_cli_congelado_colapsa_timeout_e_json_invalido_na_mesma_reply_local(
    read_lab: ReadLab,
) -> None:
    request = Request(
        "s3api",
        "get-bucket-location",
        {
            "Bucket": "cnesdata-cap-a1b2c3d4",
            "ExpectedBucketOwner": "111111111111",
        },
    )
    read_lab.process.outcomes.append(subprocess.TimeoutExpired("aws", 7))
    timeout_reply = read_lab.services.ports.commands.call(request, read_lab.context)
    respond_error(read_lab, "bad_json")
    json_reply = read_lab.services.ports.commands.call(request, read_lab.context)
    expected = Reply(False, {}, ApiError("operation_failed", {}, "local"))
    assert timeout_reply == expected
    assert json_reply == expected
    assert_process(read_lab, [LOCATION, LOCATION])


def test_inspect_converte_somente_ausencia_conclusiva_em_not_found(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_no_bucket")
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert error.value.args == ("service=not_found",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [LOCATION])


@pytest.mark.parametrize("failure", ["remote_404", "local_no_bucket", "remote_denied", "bad_json"])
def test_inspect_nao_converte_falha_inconclusiva_em_not_found(
    read_lab: ReadLab, failure: str
) -> None:
    respond_error(read_lab, failure)
    with pytest.raises(ServiceFailure) as error:
        read_lab.services.inspect(ROOT, read_lab.context)
    assert error.value.args == ("service=describe_failed",)
    read_lab.check_private(error.value)
    assert_process(read_lab, [LOCATION])


def test_lista_vazia_nao_decide_lookup_do_bucket(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["empty"])
    page = read_lab.services.inventory("s3", None, read_lab.context)
    assert page.resources == ()
    assert page.cursor is None
    respond(read_lab, fixture_data("s3")["location_east"])
    respond(read_lab, fixture_data("s3")["tags_empty"])
    assert read_lab.services.lookup(ROOT, read_lab.context) == Ownership(
        "111111111111",
        {},
        "us-east-2",
    )
    assert_process(read_lab, [LIST, LOCATION, TAGS])


def test_lookup_rele_presenca_depois_de_ausencia(read_lab: ReadLab) -> None:
    respond_error(read_lab, "remote_no_bucket")
    assert read_lab.services.lookup(ROOT, read_lab.context) is None
    respond(read_lab, fixture_data("s3")["location_east"])
    respond(read_lab, fixture_data("s3")["tags_empty"])
    assert read_lab.services.lookup(ROOT, read_lab.context) == Ownership(
        "111111111111",
        {},
        "us-east-2",
    )
    assert_process(read_lab, [LOCATION, LOCATION, TAGS])


def test_lookup_rele_ausencia_depois_de_presenca(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["location_east"])
    respond(read_lab, fixture_data("s3")["tags_empty"])
    assert read_lab.services.lookup(ROOT, read_lab.context) is not None
    respond_error(read_lab, "remote_no_bucket")
    assert read_lab.services.lookup(ROOT, read_lab.context) is None
    assert_process(read_lab, [LOCATION, TAGS, LOCATION])


def _record_location(read_lab: ReadLab) -> None:
    respond(read_lab, fixture_data("s3")["location_east"])
    request = Request(
        "s3api",
        "get-bucket-location",
        {
            "Bucket": "cnesdata-cap-a1b2c3d4",
            "ExpectedBucketOwner": "111111111111",
        },
    )
    reply = read_lab.services.ports.commands.call(request, read_lab.context)
    assert reply == Reply(True, {"LocationConstraint": "us-east-2"}, None)


def test_oraculo_request_ambiente_e_privacidade_tem_base_positiva_real(read_lab: ReadLab) -> None:
    _record_location(read_lab)
    assert_process(read_lab, [LOCATION])
    read_lab.check_private(ServiceFailure("service=describe_failed"))


@pytest.mark.parametrize(
    "credential", ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
)
def test_oraculo_rejeita_credencial_trocada_em_copia_real(
    read_lab: ReadLab, credential: str
) -> None:
    _record_location(read_lab)
    assert_process(read_lab, [LOCATION])
    calls = copy.deepcopy(read_lab.process.calls)
    calls[0][1]["env"][credential] = "changed-credential"
    copied = replace(read_lab, process=replace(read_lab.process, calls=calls))
    with pytest.raises(AssertionError):
        assert_process(copied, [LOCATION])
    assert_process(read_lab, [LOCATION])


@pytest.mark.parametrize("channel", ["exception", "captured", "file"])
def test_oraculo_rejeita_canary_vazado_em_copia_do_canal(read_lab: ReadLab, channel: str) -> None:
    _record_location(read_lab)
    assert_process(read_lab, [LOCATION])
    safe = ServiceFailure("service=describe_failed")
    read_lab.check_private(safe)
    error = ServiceFailure(CANARY) if channel == "exception" else safe
    captured = CANARY if channel == "captured" else ""
    if channel == "file":
        (read_lab.directory / "leaked.txt").write_text(CANARY)
    with pytest.raises(AssertionError):
        assert_private(error, read_lab, captured)
    if channel == "file":
        (read_lab.directory / "leaked.txt").unlink()


def _read_remote_message(read_lab: ReadLab, failure: str) -> str:
    expected = LOCATION if failure == "remote_no_bucket" else TAGS
    code = "NoSuchBucket" if failure == "remote_no_bucket" else "NoSuchTagSet"
    respond_error(read_lab, failure)
    reply = read_lab.services.ports.commands.call(
        Request(expected[0], expected[1], expected[3]), read_lab.context
    )
    assert not reply.ok
    assert reply.error is not None
    assert (reply.error.origin, reply.error.code) == ("service", code)
    message = reply.error.details["Message"]
    assert isinstance(message, str)
    assert_process(read_lab, [expected])
    return message


@pytest.mark.parametrize("failure", ["remote_no_bucket", "remote_no_tags"])
def test_captura_de_erro_remoto_tem_base_positiva_real(read_lab: ReadLab, failure: str) -> None:
    assert _read_remote_message(read_lab, failure) == CANARY
    read_lab.check_private(None)


@pytest.mark.parametrize("failure", ["remote_no_bucket", "remote_no_tags"])
@pytest.mark.parametrize(
    "channel",
    [
        "stdout",
        "stderr",
        "log_debug",
        "log_info",
        "log_warning",
        "file",
        "exception_chain",
        "normal_result",
    ],
)
def test_captura_real_rejeita_canary_do_erro_remoto(
    read_lab: ReadLab, failure: str, channel: str
) -> None:
    message = _read_remote_message(read_lab, failure)
    read_lab.check_private(None)
    observed: object = None
    match channel:
        case "stdout":
            sys.stdout.write(message)
        case "stderr":
            sys.stderr.write(message)
        case "log_debug" | "log_info" | "log_warning":
            level = {
                "log_debug": logging.DEBUG,
                "log_info": logging.INFO,
                "log_warning": logging.WARNING,
            }
            logging.getLogger("r3.privacy").log(level[channel], "observation=%s", message)
        case "file":
            (read_lab.directory / "observed.txt").write_text(message)
        case "exception_chain":
            observed = ServiceFailure("service=not_found")
            observed.__cause__ = ServiceFailure(message)
        case _:
            observed = message
    with pytest.raises(AssertionError) as rejected:
        read_lab.check_private(observed)
    frame = traceback.extract_tb(rejected.value.__traceback__)[-1]
    assert Path(frame.filename).name == "_service_read_assertions.py"
    assert frame.name == "assert_private"
    read_lab.channels.caplog.clear()
    if channel == "file":
        (read_lab.directory / "observed.txt").unlink()
    read_lab.check_private(None)


@pytest.mark.parametrize("fixture_name", ["inventory_lab", "ownership_lab", "lookup_lab"])
@pytest.mark.parametrize(
    "channel", ["clean", "stdout", "stderr", "file", "debug", "info", "warning"]
)
def test_finalizacao_real_rejeita_vazamentos_das_tres_fixtures(
    tmp_path: Path, fixture_name: str, channel: str, capsys: pytest.CaptureFixture[str]
) -> None:
    probe = tmp_path / f"test_teardown_{fixture_name}_{channel}.py"
    probe.write_text(teardown_probe_source(fixture_name, channel))
    observer = TeardownProbe()
    result = pytest.main(
        [str(probe), "-q", "--tb=short", "-p", "no:cacheprovider", "--disable-plugin-autoload"],
        plugins=[observer],
    )
    captured = capsys.readouterr()
    assert len(observer.records) == 2, captured.out + captured.err
    body, final = observer.records
    assert body["phase"] == "call"
    assert not body["failed"]
    assert final["phase"] == "teardown"
    assert body["physical_calls"] == body["budget_used"] == 1
    assert body["debits"] == [1]
    assert final["failed"] == (channel != "clean")
    assert result == (pytest.ExitCode.OK if channel == "clean" else pytest.ExitCode.TESTS_FAILED)
    assert final["last_frame"] == (
        None if channel == "clean" else ("_service_read_assertions.py", "assert_private")
    )
    if channel in {"debug", "info", "warning"}:
        assert final["old_method_survived"] is True
