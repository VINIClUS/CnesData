import json
import re
import shutil
import subprocess
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import NamedTuple, cast

import pytest

from cnes_infra.aws import AwsRuntimeSettings
from packages.cnes_infra.tests.aws.production_values import PRODUCTION_VALUES

type Tofu = tuple[str, dict[str, str]]
type Artifacts = dict[str, dict[str, object]]
type Node = dict[str, object]

MODULE_SOURCE = "../../modules/data-buckets"
BUCKET_NAMES = {"data": "cnesdata-prod-data", "audit": "cnesdata-prod-audit"}
BOTH = ("data", "audit")
AUDIT_RETENTION_DAYS = 365
PRODUCTION_SETTINGS = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES)
RETAINED_PREFIXES = ("raw", "normalized", "reconciliation", "serving")
DATA_PREFIXES = (*RETAINED_PREFIXES, "tmp")
TMP_RECOVERY_WINDOW = {"expire_days": 35, "noncurrent_days": 1}
TMP_EXPIRATION_FLOOR_DAYS = 35
ABORT_MULTIPART_DAYS = 1
SERVING_CORS = {
    "allowed_origins": ["https://cnesdata.com.br"], "allowed_methods": ["GET", "HEAD"],
    "allowed_headers": [], "expose_headers": [], "max_age_seconds": 300,
}
BUCKET_CONTROLS = {
    "aws_s3_bucket_public_access_block": BOTH,
    "aws_s3_bucket_ownership_controls": BOTH,
    "aws_s3_bucket_versioning": BOTH,
    "aws_s3_bucket_server_side_encryption_configuration": BOTH,
    "aws_s3_bucket_lifecycle_configuration": BOTH,
    "aws_s3_bucket_policy": BOTH,
    "aws_s3_bucket_cors_configuration": ("data",),
    "aws_s3_bucket_object_lock_configuration": ("audit",),
}
ACCOUNT_PUBLIC_ACCESS_BLOCK = "aws_s3_account_public_access_block"
EXPECTED_RESOURCES = frozenset({
    *(f"aws_s3_bucket.{name}" for name in BOTH),
    *(f"{kind}.{name}" for kind, names in BUCKET_CONTROLS.items() for name in names),
})
BUCKET_ARGUMENTS = {"data": {"bucket"}, "audit": {"bucket", "object_lock_enabled"}}
INVALID_NAME_RUNS = {
    "nomes_iguais_sao_rejeitados": dict.fromkeys(BOTH, "cnesdata-x"),
    "nome_com_ponto_e_rejeitado": {**BUCKET_NAMES, "data": "cnesdata.prod.data"},
}
ISOLATION_RUNS = ("buckets_distintos_privados_e_versionados", *INVALID_NAME_RUNS)
RUN_STATUS = re.compile(r'^\s*run "(?P<name>[^"]+)"\.\.\. (?P<status>\w+)$', re.MULTILINE)
MOCK_PROVIDER = re.compile(r'^mock_provider "aws" \{(?:\n  alias = "(\w+)"\n)?\}$', re.MULTILINE)

MODULE_RUN = """
run "{name}" {{
  command = {command}
  module {{
    source = "{source}"
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
ROOT_RUN = """
run "raiz_agenda_data_buckets" {{
  command = plan

  variables {{
    shared_outputs = {shared_outputs}
  }}
{body}}}
"""
EXPECT_INVALID_NAMES = "\n  expect_failures = [var.bucket_names]\n"
MOCK_PROVIDERS = 'mock_provider "aws" {}\n\nmock_provider "aws" {\n  alias = "global_edge"\n}\n'

BUCKETS = """{
      for name, bucket in { data = aws_s3_bucket.data, audit = aws_s3_bucket.audit } : name => {
        bucket        = bucket.bucket
        object_lock   = bucket.object_lock_enabled == true
        force_destroy = bucket.force_destroy == true
      }
    }"""
PUBLIC_ACCESS_BLOCKS = """{
      for name, block in {
        data  = aws_s3_bucket_public_access_block.data
        audit = aws_s3_bucket_public_access_block.audit
      } : name => [
        block.block_public_acls, block.block_public_policy,
        block.ignore_public_acls, block.restrict_public_buckets,
      ]
    }"""
SSE_ALGORITHMS = (
    "flatten([for rule in @.rule : rule.apply_server_side_encryption_by_default[*].sse_algorithm])"
)
OBJECT_LOCK = """{
      bucket_object_lock = aws_s3_bucket.audit.object_lock_enabled
      configuration = aws_s3_bucket_object_lock_configuration.audit.object_lock_enabled
      retention = flatten(aws_s3_bucket_object_lock_configuration.audit.rule[*].default_retention)
      retention_output = output.audit_retention_days
    }"""
CORS = """[
      for rule in aws_s3_bucket_cors_configuration.data.cors_rule : {
        allowed_origins = sort(rule.allowed_origins)
        allowed_methods = sort(rule.allowed_methods)
        allowed_headers = rule.allowed_headers == null ? [] : sort(rule.allowed_headers)
        expose_headers  = rule.expose_headers == null ? [] : sort(rule.expose_headers)
        max_age_seconds = rule.max_age_seconds
      }
    ]"""
LIFECYCLE_RULES = """{
      for rule in @.rule : try(rule.filter[0].prefix, "") => {
        id              = rule.id
        status          = rule.status
        abort_days      = try(rule.abort_incomplete_multipart_upload[0].days_after_initiation, null)
        expire_days     = try(rule.expiration[0].days, null)
        noncurrent_days = try(rule.noncurrent_version_expiration[0].noncurrent_days, null)
        transitions     = length(rule.transition) + length(rule.noncurrent_version_transition)
      }
    }"""
AUDIT_POLICY_DENY_ONLY = (
    "alltrue([for statement in jsondecode(aws_s3_bucket_policy.audit.policy).Statement : "
    'statement.Effect == "Deny"])'
)
TMP_EXPIRATION_FLOOR = (
    "one([for rule in aws_s3_bucket_lifecycle_configuration.data.rule : rule.expiration[0].days"
    f' if try(rule.filter[0].prefix, "") == "tmp/"]) >= {TMP_EXPIRATION_FLOOR_DAYS}'
)


class ModuleRun(NamedTuple):
    name: str
    body: str
    source: str = MODULE_SOURCE
    command: str = "plan"
    names: Mapping[str, str] = BUCKET_NAMES


class TofuReport(NamedTuple):
    statuses: dict[str, str]
    result: subprocess.CompletedProcess[str]


def _hcl(value: object) -> str:
    return json.dumps(value)


def _assertion(condition: str, message: str) -> str:
    return f"""
  assert {{
    condition     = {condition}
    error_message = "{message}"
  }}
"""


def _equal(actual: str, expected: object, message: str) -> str:
    return _assertion(f"jsonencode({actual}) == jsonencode({_hcl(expected)})", message)


def _bucket_attributes(bucket: str) -> dict[str, str]:
    return {
        "id": bucket,
        "arn": f"arn:aws:s3:::{bucket}",
        "bucket_regional_domain_name": f"{bucket}.s3.us-east-2.amazonaws.com",
    }


def _module_run(run: ModuleRun) -> str:
    overrides = "".join(
        OVERRIDE.format(name=name, values=_hcl(_bucket_attributes(bucket)))
        for name, bucket in BUCKET_NAMES.items()
    )
    return MODULE_RUN.format(
        name=run.name,
        command=run.command,
        source=run.source,
        names=_hcl(dict(run.names)),
        overrides=overrides,
        body=run.body,
    )


def _per_bucket(kind: str, projection: str, names: Sequence[str] = BOTH) -> str:
    entries = (f"{name} = {projection.replace('@', f'{kind}.{name}')}" for name in names)
    return "{\n" + "\n".join(entries) + "\n}"


def _for_buckets[T](value: T, names: Sequence[str] = BOTH) -> dict[str, T]:
    return dict.fromkeys(names, value)


def _control_targets() -> tuple[str, dict[str, str]]:
    controls = [(kind, name) for kind, names in BUCKET_CONTROLS.items() for name in names]
    actual = "\n".join(f'"{kind}.{name}" = {kind}.{name}.bucket' for kind, name in controls)
    expected = {f"{kind}.{name}": BUCKET_NAMES[name] for kind, name in controls}
    return "{\n" + actual + "\n}", expected


def _tls_only_policy(bucket: str) -> dict[str, object]:
    arn = _bucket_attributes(bucket)["arn"]
    statement = {
        "Sid": "DenyInsecureTransport",
        "Effect": "Deny",
        "Principal": "*",
        "Action": "s3:*",
        "Resource": [arn, f"{arn}/*"],
        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
    }
    return {"Version": "2012-10-17", "Statement": [statement]}


def _lifecycle_rule(
    rule_id: str, expire_days: int | None = None, noncurrent_days: int | None = None
) -> dict[str, object]:
    return {
        "id": rule_id, "status": "Enabled", "abort_days": ABORT_MULTIPART_DAYS,
        "expire_days": expire_days, "noncurrent_days": noncurrent_days, "transitions": 0,
    }


def _expected_outputs() -> dict[str, object]:
    data, audit = (_bucket_attributes(BUCKET_NAMES[name]) for name in BOTH)
    return {
        "data_bucket_name": BUCKET_NAMES["data"],
        "data_bucket_arn": data["arn"],
        "data_prefix_arns": {prefix: f"{data['arn']}/{prefix}/*" for prefix in DATA_PREFIXES},
        "audit_bucket_name": BUCKET_NAMES["audit"],
        "audit_bucket_arn": audit["arn"],
        "audit_prefix_arn": f"{audit['arn']}/audit/*",
        "audit_retention_days": AUDIT_RETENTION_DAYS,
        "serving_origin": f"https://{data['bucket_regional_domain_name']}",
    }


def _root_run(shared_outputs: Mapping[str, object]) -> str:
    expected = {
        "data_bucket_name": PRODUCTION_SETTINGS.data_bucket,
        "audit_bucket_name": PRODUCTION_SETTINGS.audit_bucket,
        "audit_retention_days": PRODUCTION_SETTINGS.audit_retention_days,
    }
    actual = "{ " + ", ".join(f"{name} = module.data_buckets.{name}" for name in expected) + " }"
    body = _equal(actual, expected, "root_matches_runtime_settings")
    return ROOT_RUN.format(shared_outputs=_hcl(shared_outputs), body=body)


def _isolation_body() -> str:
    buckets = {
        name: {"bucket": BUCKET_NAMES[name], "object_lock": name == "audit", "force_destroy": False}
        for name in BOTH
    }
    public_access = _for_buckets([True] * 4)
    versioning = _per_bucket("aws_s3_bucket_versioning", "@.versioning_configuration[*].status")
    targets, expected_targets = _control_targets()
    return (
        _equal(BUCKETS, buckets, "buckets")
        + _equal(PUBLIC_ACCESS_BLOCKS, public_access, "public_access_block")
        + _equal(versioning, _for_buckets(["Enabled"]), "versioning")
        + _equal(targets, expected_targets, "control_targets")
    )


def _transport_body() -> str:
    ownership = _per_bucket("aws_s3_bucket_ownership_controls", "@.rule[*].object_ownership")
    policies = _per_bucket("aws_s3_bucket_policy", "jsondecode(@.policy)")
    encryption = _per_bucket("aws_s3_bucket_server_side_encryption_configuration", SSE_ALGORITHMS)
    expected_policies = {name: _tls_only_policy(BUCKET_NAMES[name]) for name in BOTH}
    return (
        _equal(ownership, _for_buckets(["BucketOwnerEnforced"]), "ownership")
        + _equal(policies, expected_policies, "tls_only_policies")
        + _equal(encryption, _for_buckets(["AES256"]), "sse_s3")
    )


def _audit_lock_body() -> str:
    expected = {
        "bucket_object_lock": True,
        "configuration": "Enabled",
        "retention": [{"days": AUDIT_RETENTION_DAYS, "mode": "COMPLIANCE", "years": None}],
        "retention_output": AUDIT_RETENTION_DAYS,
    }
    return _equal(OBJECT_LOCK, expected, "audit_object_lock")


def _audit_immutability_body() -> str:
    lifecycle = _per_bucket("aws_s3_bucket_lifecycle_configuration", LIFECYCLE_RULES, ("audit",))
    expected = {"audit": {"audit/": _lifecycle_rule("retain-audit")}}
    return (
        _assertion(AUDIT_POLICY_DENY_ONLY, "audit_policy_deny_only")
        + _equal(lifecycle, expected, "audit_lifecycle_without_deletion")
        + _assertion("aws_s3_bucket.audit.force_destroy != true", "audit_force_destroy")
    )


def _lifecycle_body() -> str:
    lifecycle = _per_bucket("aws_s3_bucket_lifecycle_configuration", LIFECYCLE_RULES, ("data",))
    retained = {f"{prefix}/": _lifecycle_rule(f"retain-{prefix}") for prefix in RETAINED_PREFIXES}
    expected = {**retained, "tmp/": _lifecycle_rule("expire-tmp", **TMP_RECOVERY_WINDOW)}
    return _equal(lifecycle, {"data": expected}, "lifecycle_by_prefix") + _assertion(
        TMP_EXPIRATION_FLOOR, "tmp_expiration_floor"
    )


def _outputs_body() -> str:
    expected = _expected_outputs()
    actual = "{\n" + "\n".join(f"{name} = output.{name}" for name in expected) + "\n}"
    return _equal(actual, expected, "outputs")


def _suite(runs: Iterable[ModuleRun], header: str = "") -> str:
    return MOCK_PROVIDERS + header + "".join(_module_run(run) for run in runs)


def _plan_suites(shared_outputs: Mapping[str, object]) -> dict[str, str]:
    runs = (
        ModuleRun(ISOLATION_RUNS[0], _isolation_body()),
        ModuleRun("tls_obrigatorio_e_owner_enforced", _transport_body()),
        ModuleRun("audit_object_lock_compliance_na_criacao", _audit_lock_body()),
        ModuleRun("audit_sem_bypass_ou_delete", _audit_immutability_body()),
        ModuleRun("cors_exato_so_no_bucket_de_dados", _equal(CORS, [SERVING_CORS], "cors")),
        ModuleRun("tmp_expira_e_publicado_nao_e_apagado", _lifecycle_body()),
        ModuleRun("outputs_com_arns_exatos", _outputs_body()),
    )
    rejections = (
        ModuleRun(name, EXPECT_INVALID_NAMES, names=names)
        for name, names in INVALID_NAME_RUNS.items()
    )
    # A missing expected failure errors its whole file and skips every later run in it.
    return {
        "data-buckets": _suite(runs, _root_run(shared_outputs)),
        "data-buckets-validation": _suite(rejections),
    }


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


def _module_calls(tofu_artifacts: Artifacts) -> dict[str, Node]:
    root = cast("Node", tofu_artifacts["configuration"]["root_module"])
    return cast("dict[str, Node]", root.get("module_calls", {}))


def _graph_types(module: Node) -> Iterator[str]:
    for resource in cast("list[Node]", module.get("resources", [])):
        yield cast("str", resource["type"])
    for call in cast("dict[str, Node]", module.get("module_calls", {})).values():
        yield from _graph_types(cast("Node", call["module"]))


def _root_aws_providers(tofu_artifacts: Artifacts) -> set[str]:
    providers = cast("dict[str, Node]", tofu_artifacts["configuration"]["provider_config"])
    return {
        key for key, provider in providers.items()
        if provider["name"] == "aws" and "module_address" not in provider
    }


def _mocked_providers(suite: str) -> set[str]:
    return {f"aws.{alias}" if alias else "aws" for alias in MOCK_PROVIDER.findall(suite)}


def _module_resources(tofu_artifacts: Artifacts) -> dict[str, Node]:
    module = cast("Node", _module_calls(tofu_artifacts)["data_buckets"]["module"])
    resources = cast("list[Node]", module.get("resources", []))
    return {cast("str", resource["address"]): resource for resource in resources}


@pytest.fixture(scope="module")
def plan_report(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    valid_shared_outputs: dict[str, object],
) -> Iterator[TofuReport]:
    test_directory = prod_root / ".terraform" / "data-buckets-tests"
    test_directory.mkdir(parents=True)
    selector = "-test-directory=.terraform/data-buckets-tests"
    try:
        for name, suite in _plan_suites(valid_shared_outputs).items():
            (test_directory / f"{name}.tftest.hcl").write_text(suite, encoding="utf-8")
        init = _tofu(tofu_context, prod_root, ("init", "-backend=false", "-input=false", selector))
        _require_success(init, "init")
        yield _report(_tofu(tofu_context, prod_root, ("test", "-no-color", selector)))
    finally:
        shutil.rmtree(test_directory)


def test_raiz_agenda_buckets_de_dados_e_auditoria(
    plan_report: TofuReport, tofu_artifacts: Artifacts, prod_root: Path
) -> None:
    calls = _module_calls(tofu_artifacts)
    module = (prod_root / MODULE_SOURCE).resolve()
    targets = {n: (prod_root / cast("str", c["source"])).resolve() for n, c in calls.items()}

    _assert_runs(plan_report, {"raiz_agenda_data_buckets": "pass"})
    assert [n for n, target in targets.items() if target == module] == ["data_buckets"]


def test_buckets_distintos_privados_e_versionados(
    plan_report: TofuReport,
    tofu_artifacts: Artifacts,
) -> None:
    resources = _module_resources(tofu_artifacts)
    arguments = {
        name: set(cast("Node", resources[f"aws_s3_bucket.{name}"]["expressions"]))
        for name in BOTH
    }

    _assert_runs(plan_report, dict.fromkeys(ISOLATION_RUNS, "pass"))
    assert set(resources) == EXPECTED_RESOURCES
    assert arguments == BUCKET_ARGUMENTS


def test_produto_nao_declara_block_public_access_de_conta(tofu_artifacts: Artifacts) -> None:
    root = cast("Node", tofu_artifacts["configuration"]["root_module"])

    assert ACCOUNT_PUBLIC_ACCESS_BLOCK not in set(_graph_types(root))


def test_tls_obrigatorio_e_owner_enforced(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, {"tls_obrigatorio_e_owner_enforced": "pass"})


def test_audit_tem_object_lock_compliance_365_na_criacao(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, {"audit_object_lock_compliance_na_criacao": "pass"})


def test_retencao_do_audit_concorda_com_aws_audit_retention_days(
    plan_report: TofuReport,
) -> None:
    _assert_runs(
        plan_report,
        {"raiz_agenda_data_buckets": "pass", "audit_object_lock_compliance_na_criacao": "pass"},
    )
    assert PRODUCTION_SETTINGS.audit_retention_days == AUDIT_RETENTION_DAYS


def test_audit_sem_politica_de_bypass_ou_delete(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, {"audit_sem_bypass_ou_delete": "pass"})


def test_cors_exato_sem_wildcard_nem_credentials(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, {"cors_exato_so_no_bucket_de_dados": "pass"})


def test_tmp_expira_e_publicado_nao_e_apagado(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, {"tmp_expira_e_publicado_nao_e_apagado": "pass"})


def test_outputs_expoem_arns_exatos_de_bucket_e_prefixo(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, {"outputs_com_arns_exatos": "pass"})


def test_suites_gerados_mockam_todo_provider_aws_da_raiz(
    tofu_artifacts: Artifacts,
    valid_shared_outputs: dict[str, object],
) -> None:
    roots = _root_aws_providers(tofu_artifacts)
    suites = list(_plan_suites(valid_shared_outputs).values())

    assert {"aws", "aws.global_edge"} <= roots
    assert [_mocked_providers(suite) for suite in suites] == [roots] * len(suites)
