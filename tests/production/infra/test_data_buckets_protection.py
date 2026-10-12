import json
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import NamedTuple, cast

import pytest

type Tofu = tuple[str, dict[str, str]]
type Artifacts = dict[str, dict[str, object]]
type Node = dict[str, object]
type TofuEnvironment = Callable[[Mapping[str, str]], dict[str, str]]

MODULE_DIRECTORY = Path(__file__).resolve().parents[3] / "infra/opentofu/modules/data-buckets"
BUCKET_NAMES = {"data": "cnesdata-prod-data", "audit": "cnesdata-prod-audit"}
BOTH = ("data", "audit")
BPA_FLAGS = (
    "block_public_acls", "block_public_policy", "ignore_public_acls", "restrict_public_buckets",
)
FAIL_CLOSED_AWS = {
    "AWS_CONFIG_FILE": os.devnull, "AWS_EC2_METADATA_DISABLED": "true",
    "AWS_ENDPOINT_URL": "http://127.0.0.1:9", "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
}
CALLER_AWS_VARIABLES = (
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE", "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "AWS_ENDPOINT_URL_S3", *FAIL_CLOSED_AWS,
)
RUN_STATUS = re.compile(r'^\s*run "(?P<name>[^"]+)"\.\.\. (?P<status>\w+)$', re.MULTILINE)
MOCK_PROVIDER = re.compile(r'^mock_provider "aws" \{(?:\n  alias = "(\w+)"\n)?\}$', re.MULTILINE)
MOCK_PROVIDERS = 'mock_provider "aws" {}\n\nmock_provider "aws" {\n  alias = "global_edge"\n}\n'

MODULE_RUN = """
run "{name}" {{
  command = {command}
  module {{
    source = "./modules/data-buckets"
  }}
  variables {{
    bucket_names = {names}
  }}
{overrides}{body}}}
"""
OVERRIDE = """
  override_resource {{
    target = aws_s3_bucket.{name}
    values = {values}
  }}
"""


class TofuReport(NamedTuple):
    statuses: dict[str, str]
    result: subprocess.CompletedProcess[str]


def _bucket_attributes(bucket: str) -> dict[str, str]:
    return {
        "id": bucket,
        "arn": f"arn:aws:s3:::{bucket}",
        "bucket_regional_domain_name": f"{bucket}.s3.us-east-2.amazonaws.com",
    }


def _module_run(name: str, command: str, body: str) -> str:
    overrides = "".join(
        OVERRIDE.format(name=key, values=json.dumps(_bucket_attributes(bucket)))
        for key, bucket in BUCKET_NAMES.items()
    )
    return MODULE_RUN.format(
        name=name, command=command, names=json.dumps(BUCKET_NAMES), overrides=overrides, body=body
    )


def _protection_suite(name: str) -> str:
    replace = f"\n  plan_options {{\n    replace = [aws_s3_bucket.{name}]\n  }}\n"
    return (
        MOCK_PROVIDERS
        + _module_run(f"cria_bucket_{name}", "apply", "")
        + _module_run(f"substitui_bucket_{name}", "plan", replace)
    )


def _tofu(tofu: Tofu, root: Path, arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [tofu[0], f"-chdir={root}", *arguments],
        check=False, capture_output=True, env=tofu[1], text=True,
    )


def _require_success(result: subprocess.CompletedProcess[str], command: str) -> None:
    if result.returncode != 0:
        pytest.fail(
            f"tofu_failed command={command} stdout={result.stdout!r} stderr={result.stderr!r}"
        )


def _report(result: subprocess.CompletedProcess[str]) -> TofuReport:
    statuses = {match["name"]: match["status"] for match in RUN_STATUS.finditer(result.stdout)}
    return TofuReport(statuses, result)


def _assert_runs(report: TofuReport, expected: Mapping[str, str]) -> None:
    actual = {name: report.statuses.get(name) for name in expected}
    assert actual == dict(expected), (
        f"tofu_test runs={actual} stdout={report.result.stdout!r} "
        f"stderr={report.result.stderr!r}"
    )


def _write_isolated_root(root: Path, prod_root: Path) -> None:
    hidden = shutil.ignore_patterns(".*")
    shutil.copytree(MODULE_DIRECTORY, root / "modules" / "data-buckets", ignore=hidden)
    for name in ("versions.tf", ".terraform.lock.hcl"):
        shutil.copy2(prod_root / name, root / name)
    tests = root / "tests"
    tests.mkdir()
    for name in BOTH:
        suite = tests / f"protection-{name}.tftest.hcl"
        suite.write_text(_protection_suite(name), encoding="utf-8")


def _root_aws_providers(tofu_artifacts: Artifacts) -> set[str]:
    providers = cast("dict[str, Node]", tofu_artifacts["configuration"]["provider_config"])
    return {
        key for key, provider in providers.items()
        if provider["name"] == "aws" and "module_address" not in provider
    }


def _mocked_providers(suite: str) -> set[str]:
    return {f"aws.{alias}" if alias else "aws" for alias in MOCK_PROVIDER.findall(suite)}


def _aws_variables(environment: Mapping[str, str]) -> dict[str, str]:
    return {key: value for key, value in environment.items() if key.startswith("AWS_")}


@pytest.fixture(scope="module")
def isolated_report(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> TofuReport:
    root = tmp_path_factory.mktemp("data-buckets-protection")
    _write_isolated_root(root, prod_root)
    providers = Path(tofu_context[1]["TF_DATA_DIR"]) / "providers"
    tofu = (tofu_context[0], {**tofu_context[1], "TF_DATA_DIR": str(root / ".tofu-data")})
    init = ("init", "-backend=false", "-input=false", f"-plugin-dir={providers}")
    _require_success(_tofu(tofu, root, init), "init-isolated")
    return _report(_tofu(tofu, root, ("test", "-no-color")))


@pytest.mark.parametrize("bucket", BOTH)
def test_bucket_tem_prevent_destroy(isolated_report: TofuReport, bucket: str) -> None:
    result = isolated_report.result
    output = " ".join((result.stdout + result.stderr).split())

    _assert_runs(
        isolated_report, {f"cria_bucket_{bucket}": "pass", f"substitui_bucket_{bucket}": "fail"}
    )
    assert f"Resource instance aws_s3_bucket.{bucket} has prevent_destroy set" in output


def test_tofu_roda_sem_credencial_aws_e_com_endpoint_morto(
    tofu_context: Tofu, tofu_environment: TofuEnvironment
) -> None:
    caller = {**dict.fromkeys(CALLER_AWS_VARIABLES, "canary"), "PATH": "/usr/bin"}
    sanitized = tofu_environment(caller)

    assert _aws_variables(tofu_context[1]) == FAIL_CLOSED_AWS
    assert _aws_variables(sanitized) == FAIL_CLOSED_AWS
    assert sanitized["PATH"] == "/usr/bin"


def test_suites_de_protecao_mockam_todo_provider_aws_da_raiz(tofu_artifacts: Artifacts) -> None:
    roots = _root_aws_providers(tofu_artifacts)
    suites = [_protection_suite(name) for name in BOTH]

    assert {"aws", "aws.global_edge"} <= roots
    assert [_mocked_providers(suite) for suite in suites] == [roots] * len(suites)


def test_suites_do_conftest_mockam_todo_provider_aws_da_raiz(
    tofu_artifacts: Artifacts, generated_suite_headers: tuple[str, ...]
) -> None:
    roots = _root_aws_providers(tofu_artifacts)
    mocked = [_mocked_providers(suite) for suite in generated_suite_headers]

    assert mocked == [roots] * len(generated_suite_headers)


def test_schema_v2_exige_os_quatro_flags_de_bpa_verdadeiros(prod_root: Path) -> None:
    node = cast("Node", json.loads((prod_root / "shared-outputs.schema.json").read_text("utf-8")))
    for key in ("account_baseline", "s3_public_access_block"):
        node = cast("Node", cast("Node", node["properties"])[key])
    flags = cast("dict[str, Node]", node["properties"])
    consts = {name: flag.get("const") for name, flag in flags.items()}

    assert consts == dict.fromkeys(BPA_FLAGS, True)
