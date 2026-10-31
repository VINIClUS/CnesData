import json
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import NamedTuple, TypedDict, cast

import pytest

from tests.production.infra._aws_mock_defaults import (
    BUCKET_REGIONAL_DOMAIN_NAME,
    MOCK_AWS_PROVIDERS,
)
from tests.production.infra._processing_native import (  # noqa: F401
    _provider_schema,
    processing_native,
    processing_plan,
)
from tests.production.infra._root_native_identities import (
    _override_hcl,
    preserve_root_mock_defaults,
    root_identity_seeds,
)
from tests.production.infra._root_native_observation import NativeRootReport, observe_root_result

ROOT_FIXTURE_CONTRACT = {
    "account_id": "123456789012", "name_prefix": "cnesdata-prod", "region": "us-east-2",
}

REPO_ROOT = Path(__file__).resolve().parents[3]
PROD_ROOT = REPO_ROOT / "infra" / "opentofu" / "env" / "prod"

ToolLookup = Callable[[str], str | None]
ValidationCase = tuple[str, object, bool]
SharedValidator = Callable[[Sequence[ValidationCase], frozenset[str]], NativeRootReport]


class TofuArtifacts(TypedDict):
    validation: dict[str, object]
    configuration: dict[str, object]
    version: dict[str, object]
    schema: dict[str, object]
    root_identities: dict[str, object]
    evidence: dict[str, object]


class TofuContext(NamedTuple):
    executable: str
    environment: dict[str, str]


VALID_SHARED_OUTPUTS: dict[str, object] = {
    "schema_version": 2,
    "account_id": "123456789012",
    "kms_key_arn": "arn:aws:kms:us-east-2:123456789012:key/example",
    "roles_anywhere": {
        "api_role_arn": "arn:aws:iam::123456789012:role/cnesdata-api",
        "profile_arn": ("arn:aws:rolesanywhere:us-east-2:123456789012:profile/0123456789abcdef0"),
        "trust_anchor_arn": (
            "arn:aws:rolesanywhere:us-east-2:123456789012:trust-anchor/0123456789abcdef0"
        ),
        "crl": {
            "crl_id": "crl-0123456789abcdef0",
            "enabled": True,
            "next_update": "2026-12-31T23:59:59Z",
            "sha256": "a" * 64,
        },
    },
    "github": {
        "infrastructure_plan_role_arn": (
            "arn:aws:iam::123456789012:role/github-infrastructure-plan"
        ),
        "infrastructure_deploy_role_arn": (
            "arn:aws:iam::123456789012:role/github-infrastructure-deploy"
        ),
        "host_deploy_role_arn": "arn:aws:iam::123456789012:role/github-host-deploy",
    },
    "budget_freeze": {
        "budget_arn": "arn:aws:budgets::123456789012:budget/cnesdata-prod",
        "action_arn": "arn:aws:budgets::123456789012:budget/cnesdata-prod/action/freeze",
        "policy_arn": "arn:aws:iam::123456789012:policy/cnesdata-budget-freeze",
    },
    "storage": {
        "state_bucket_name": "shared-opentofu-state-example",
        "state_bucket_arn": "arn:aws:s3:::shared-opentofu-state-example",
        "evidence_bucket_name": "shared-infra-evidence-example",
        "evidence_bucket_arn": "arn:aws:s3:::shared-infra-evidence-example",
    },
    "account_baseline": {
        "s3_public_access_block": {
            "block_public_acls": True,
            "block_public_policy": True,
            "ignore_public_acls": True,
            "restrict_public_buckets": True,
        },
    },
}


EXPECTED_SHARED_TYPE: list[object] = [
    "object",
    {
        "schema_version": "number",
        "account_id": "string",
        "kms_key_arn": "string",
        "roles_anywhere": [
            "object",
            {
                "api_role_arn": "string",
                "profile_arn": "string",
                "trust_anchor_arn": "string",
                "crl": [
                    "object",
                    {
                        "crl_id": "string",
                        "enabled": "bool",
                        "next_update": "string",
                        "sha256": "string",
                    },
                ],
            },
        ],
        "github": [
            "object",
            {
                "infrastructure_plan_role_arn": "string",
                "infrastructure_deploy_role_arn": "string",
                "host_deploy_role_arn": "string",
            },
        ],
        "budget_freeze": [
            "object",
            {
                "budget_arn": "string",
                "action_arn": "string",
                "policy_arn": "string",
            },
        ],
        "storage": [
            "object",
            {
                "state_bucket_name": "string",
                "state_bucket_arn": "string",
                "evidence_bucket_name": "string",
                "evidence_bucket_arn": "string",
            },
        ],
        "account_baseline": [
            "object",
            {
                "s3_public_access_block": [
                    "object",
                    {
                        "block_public_acls": "bool",
                        "block_public_policy": "bool",
                        "ignore_public_acls": "bool",
                        "restrict_public_buckets": "bool",
                    },
                ],
            },
        ],
    },
]


# No AWS_* variable of the caller reaches tofu and the shared files are empty: an unmocked provider
# gets no credential, profile or ~/.aws setting (ignore_configured_endpoint_urls included),
# instance metadata is off and every AWS request goes to a dead endpoint.
TOFU_AWS_ENVIRONMENT = {
    "AWS_CONFIG_FILE": os.devnull,
    "AWS_EC2_METADATA_DISABLED": "true",
    "AWS_ENDPOINT_URL": "http://127.0.0.1:9",
    "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
}


MOCK_PROVIDER = re.compile(r'^mock_provider "aws" \{(?:\n  alias = "(\w+)"\n)?', re.MULTILINE)
NATIVE_CONTRACT_TEST = (
    MOCK_AWS_PROVIDERS
    + """
run "evaluate_composition_contract" {
  command = plan

  assert {
    condition     = local.name_prefix == "cnesdata-prod"
    error_message = "name_prefix contract changed"
  }

  assert {
    condition = local.common_tags == {
      Project     = "cnesdata"
      Environment = "prod"
      ManagedBy   = "opentofu"
      Owner       = "vinisantana"
    }
    error_message = "common_tags contract changed"
  }
}
"""
)


def require_tool(name: str, lookup: ToolLookup = shutil.which) -> str:
    executable = lookup(name)
    if executable is not None:
        return executable
    message = f"tool_missing name={name}"
    if "CI" in os.environ:
        pytest.fail(message)
    pytest.skip(message)


def tofu_environment(source: Mapping[str, str]) -> dict[str, str]:
    environment = {key: value for key, value in source.items() if not key.startswith("AWS_")}
    return environment | TOFU_AWS_ENVIRONMENT


def _run_tofu(
    tofu_bin: str,
    arguments: Sequence[str],
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [tofu_bin, f"-chdir={PROD_ROOT}", *arguments],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )


def _require_success(result: subprocess.CompletedProcess[str], command: str) -> None:
    if result.returncode == 0:
        return
    pytest.fail(f"tofu_failed command={command} stdout={result.stdout!r} stderr={result.stderr!r}")


def _shared_validation_run(case: ValidationCase) -> str:
    name, value, expect_failure = case
    encoded = json.dumps(value, separators=(",", ":"))
    expected = "\n  expect_failures = [var.shared_outputs]" if expect_failure else ""
    return f'''run "{name}" {{
  command = plan
  variables {{
    shared_outputs = {encoded}
  }}{expected}
}}
'''


def shared_validation_suite(cases: Sequence[ValidationCase], seeds: Sequence[dict] = ()) -> str:
    return (MOCK_AWS_PROVIDERS + _override_hcl(list(seeds)) + "\n"
            + "\n".join(_shared_validation_run(case) for case in cases))


@pytest.fixture(name="require_tool")
def fixture_require_tool() -> Callable[[str, ToolLookup], str]:
    return require_tool


@pytest.fixture(name="generated_suite_headers")
def fixture_generated_suite_headers() -> tuple[str, ...]:
    return (NATIVE_CONTRACT_TEST, shared_validation_suite(()))


@pytest.fixture(name="tofu_environment")
def fixture_tofu_environment() -> Callable[[Mapping[str, str]], dict[str, str]]:
    return tofu_environment


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def git_ignored_paths() -> Callable[[Sequence[str]], set[str]]:
    git_bin = require_tool("git")

    def check(paths: Sequence[str]) -> set[str]:
        result = subprocess.run(  # noqa: S603
            [git_bin, "check-ignore", "--no-index", "--stdin"],
            check=False,
            capture_output=True,
            cwd=REPO_ROOT,
            input="\n".join(paths),
            text=True,
        )
        return set(result.stdout.splitlines())

    return check


@pytest.fixture(scope="session")
def prod_root() -> Path:
    return PROD_ROOT


@pytest.fixture(scope="session")
def valid_shared_outputs() -> dict[str, object]:
    return VALID_SHARED_OUTPUTS


@pytest.fixture(scope="session")
def expected_shared_type() -> list[object]:
    return EXPECTED_SHARED_TYPE


@pytest.fixture(scope="session")
def tofu_context(tmp_path_factory: pytest.TempPathFactory) -> TofuContext:
    tofu_bin = require_tool("tofu")
    environment = tofu_environment(os.environ)
    environment["TF_DATA_DIR"] = str(tmp_path_factory.mktemp("tofu-data"))
    return TofuContext(tofu_bin, environment)


@pytest.fixture(scope="session")
def tofu_artifacts(tofu_context: TofuContext) -> TofuArtifacts:
    tofu_bin, environment = tofu_context

    init_result = _run_tofu(tofu_bin, ("init", "-backend=false", "-input=false"), environment)
    _require_success(init_result, "init")

    validate_result = _run_tofu(tofu_bin, ("validate", "-json"), environment)
    validation = json.loads(validate_result.stdout)
    _require_success(validate_result, "validate")
    if validation.get("valid") is not True:
        pytest.fail(f"tofu_invalid diagnostics={validation.get('diagnostics')!r}")

    show_result = _run_tofu(tofu_bin, ("show", "-json", "-config"), environment)
    _require_success(show_result, "show-config")
    configuration = json.loads(show_result.stdout)

    version_result = _run_tofu(tofu_bin, ("version", "-json"), environment)
    _require_success(version_result, "version")
    version = json.loads(version_result.stdout)
    artifacts = {"validation": validation, "configuration": configuration, "version": version}
    schema = _provider_schema(tofu_context, PROD_ROOT, artifacts)
    seeds = root_identity_seeds(configuration, schema, ROOT_FIXTURE_CONTRACT)
    overrides = preserve_root_mock_defaults(seeds, configuration, schema, {
        "value": BUCKET_REGIONAL_DOMAIN_NAME,
        "source": "tests/production/infra/_aws_mock_defaults.py::BUCKET_REGIONAL_DOMAIN_NAME",
    })
    records = {"seeds": seeds, "overrides": overrides}
    evidence = Path(tofu_context.environment["TF_DATA_DIR"])
    (evidence / "root-identity-records.json").write_text(
        json.dumps(records, indent=2), encoding="utf-8"
    )
    return cast("TofuArtifacts", artifacts | {"schema": schema, "root_identities": records,
        "evidence": {"directory": str(evidence)}})


@pytest.fixture(scope="session")
def native_contract_result(
    tofu_context: TofuContext,
    tofu_artifacts: TofuArtifacts,
) -> Iterator[NativeRootReport]:
    ids = cast("dict[str, list[dict]]", tofu_artifacts["root_identities"])
    suite = NATIVE_CONTRACT_TEST + _override_hcl(ids["overrides"])
    test_directory = PROD_ROOT / ".terraform" / "contract-tests"
    test_directory.mkdir(parents=True)
    try:
        (test_directory / "composition.tftest.hcl").write_text(
            suite,
            encoding="utf-8",
        )
        result = _run_tofu(
            tofu_context.executable,
            (
                "test",
                "-no-color", "-json", "-verbose",
                "-test-directory=.terraform/contract-tests",
                f"-var=shared_outputs={json.dumps(VALID_SHARED_OUTPUTS, separators=(',', ':'))}",
            ),
            tofu_context.environment,
        )
        yield observe_root_result(
            result, ids["seeds"], {"test_file": ".terraform/contract-tests/composition.tftest.hcl",
                            "positive_roots": frozenset({"evaluate_composition_contract"})},
            {"directory": tofu_context.environment["TF_DATA_DIR"], "label": "composition",
             "suite": suite.encode(), "metadata": {"artifacts": tofu_artifacts}},
        )
    finally:
        shutil.rmtree(test_directory)


@pytest.fixture(scope="session")
def validate_shared_outputs(
    tofu_context: TofuContext,
    tofu_artifacts: TofuArtifacts,
) -> Iterator[SharedValidator]:
    test_directory = PROD_ROOT / ".terraform" / "shared-output-tests"
    test_directory.mkdir(parents=True)
    test_file = test_directory / "shared-outputs.tftest.hcl"

    ids = cast("dict[str, list[dict]]", tofu_artifacts["root_identities"])
    invocation = 0

    def validate(
        cases: Sequence[ValidationCase], positive_roots: frozenset[str]
    ) -> NativeRootReport:
        nonlocal invocation
        invocation += 1
        suite = shared_validation_suite(cases, ids["overrides"])
        test_file.write_text(suite, encoding="utf-8")
        result = _run_tofu(
            tofu_context.executable,
            ("test", "-no-color", "-json", "-verbose",
             "-test-directory=.terraform/shared-output-tests"),
            tofu_context.environment,
        )
        return observe_root_result(
            result, ids["seeds"],
            {"test_file": ".terraform/shared-output-tests/shared-outputs.tftest.hcl",
                            "positive_roots": positive_roots},
            {"directory": tofu_context.environment["TF_DATA_DIR"], "label": f"shared-{invocation}",
             "suite": suite.encode(), "metadata": {"artifacts": tofu_artifacts}},
        )

    yield validate
    shutil.rmtree(test_directory)
