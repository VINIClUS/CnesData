import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from tests.production.infra._processing_native import _provider_schema
from tests.production.infra._root_native_identities import (
    _override_hcl,
    assert_root_identity_producers,
    merge_identity_records,
    preserve_root_mock_defaults,
    root_identity_seeds,
)
from tests.production.infra._static_web_inventory import MODULE_TYPES, OUTPUTS, STATIC_TYPES
from tests.production.infra.conftest import ROOT_FIXTURE_CONTRACT

type Node = dict[str, Any]
type Tofu = tuple[str, dict[str, str]]

APEX = "cnesdata.com.br"
API_URL = "https://api.cnesdata.com.br/api/v1"
ISSUER_ORIGIN = "https://cognito-idp.us-east-2.amazonaws.com"
LOGIN_ORIGIN = "https://cnesdata-prod54qgag.auth.us-east-2.amazoncognito.com"
STATIC_BUCKET_DEFAULTS = {
    "id": "cnesdata-fixture", "arn": "arn:aws:s3:::cnesdata-fixture",
    "bucket_regional_domain_name": "fixture-other.s3.us-east-2.amazonaws.com",
}
BOUNDARIES = {
    "a": {
        "serving_host": "fixture-a.s3.us-east-2.amazonaws.com",
        "certificate_arn": (
            "arn:aws:acm:us-east-1:123456789012:certificate/"
            "11111111-1111-4111-8111-111111111111"
        ),
        "distribution_domain": "d1111111111111.cloudfront.net", "distribution_id": "E1111111111111",
        "record_name": "_1111111111111111.cnesdata.com.br.",
        "record_value": "_aaaaaaaaaaaaaaaa.acm-validations.aws.",
    },
    "b": {
        "serving_host": "fixture-b.s3.us-east-2.amazonaws.com",
        "certificate_arn": (
            "arn:aws:acm:us-east-1:123456789012:certificate/"
            "22222222-2222-4222-8222-222222222222"
        ),
        "distribution_domain": "d2222222222222.cloudfront.net", "distribution_id": "E2222222222222",
        "record_name": "_2222222222222222.cnesdata.com.br.",
        "record_value": "_bbbbbbbbbbbbbbbb.acm-validations.aws.",
    },
}


class Workspace(NamedTuple):
    root: Path
    tofu: Tofu
    boundary: Node
    artifacts: Node


class Composition(NamedTuple):
    workspace: Workspace
    configuration: Node
    plan: Node


def _mock_provider(boundary: Node, alias: bool = False) -> str:
    defaults = {
        "aws_s3_bucket": STATIC_BUCKET_DEFAULTS,
        "aws_cognito_user_pool": {
            "id": "us-east-2_aBcD3fGh1",
            "endpoint": "cognito-idp.us-east-2.amazonaws.com/us-east-2_aBcD3fGh1",
        },
        "aws_cognito_user_pool_client": {"id": "1a2b3c4d5e6f7g8h9i0j1k2l3m"},
        "aws_cloudfront_function": {
            "arn": "arn:aws:cloudfront::123456789012:function/fixture-spa",
        },
        "aws_acm_certificate": {
            "arn": boundary["certificate_arn"], "id": boundary["certificate_arn"],
            "subject_alternative_names": [APEX],
            "domain_validation_options": [{
                "domain_name": APEX, "resource_record_name": boundary["record_name"],
                "resource_record_type": "CNAME", "resource_record_value": boundary["record_value"],
            }],
        },
        "aws_cloudfront_distribution": {
            "id": boundary["distribution_id"], "domain_name": boundary["distribution_domain"],
            "arn": f"arn:aws:cloudfront::123456789012:distribution/{boundary['distribution_id']}",
        },
    }
    blocks = "".join(
        f'  mock_resource "{kind}" {{\n    defaults = {json.dumps(values)}\n  }}\n'
        for kind, values in defaults.items()
    )
    region = "us-east-1" if alias else "us-east-2"
    blocks += f'  mock_data "aws_region" {{\n    defaults = {{ region = "{region}" }}\n  }}\n'
    alias_line = '  alias = "global_edge"\n' if alias else ""
    return f'mock_provider "aws" {{\n{alias_line}{blocks}}}\n'


def _static_overrides(boundary: Node, artifacts: Node) -> list[Node]:
    seeds = artifacts["seeds"]
    record = dict(next(item for item in seeds if item["address"].endswith("aws_s3_bucket.data")))
    data = {"id": "cnesdata-prod-data", "arn": "arn:aws:s3:::cnesdata-prod-data",
            "bucket_regional_domain_name": boundary["serving_host"]}
    record["fields"] = {name: {"value": value, "source": {"boundary": boundary}}
                        for name, value in data.items()}
    return preserve_root_mock_defaults(
        merge_identity_records(seeds, [record]), artifacts["configuration"], artifacts["schema"],
        {"value": STATIC_BUCKET_DEFAULTS["bucket_regional_domain_name"],
         "source": "tests/production/infra/test_static_web_composition.py::STATIC_BUCKET_DEFAULTS"},
    )


def _suite(boundary: Node, shared_outputs: Node, overrides: list[Node]) -> str:
    providers = _mock_provider(boundary) + _mock_provider(boundary, alias=True)
    return (providers + '\nrun "composicao_static_web" {\n  command = plan\n  variables {\n'
            + f'    shared_outputs = {json.dumps(shared_outputs)}\n  }}\n'
            + _override_hcl(overrides) + "}\n")


def _tofu(workspace: Workspace, label: str, arguments: tuple[str, ...]) -> Node:
    executable, environment = workspace.tofu
    command = [executable, f"-chdir={workspace.root}", *arguments]
    result = subprocess.run(  # noqa: S603
        command, check=False, capture_output=True, env=environment, text=True,
    )
    evidence = workspace.root / ".test-evidence"
    evidence.mkdir(exist_ok=True)
    (evidence / f"{label}.stdout").write_text(result.stdout, encoding="utf-8")
    (evidence / f"{label}.stderr").write_text(result.stderr, encoding="utf-8")
    (evidence / f"{label}.json").write_text(
        json.dumps({"command": command, "returncode": result.returncode}), encoding="utf-8",
    )
    assert result.returncode == 0, (
        f"native_setup_failed command={label} rc={result.returncode} "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    if label != "test":
        return json.loads(result.stdout) if label != "init" else {}
    messages = [json.loads(line) for line in result.stdout.splitlines()]
    statuses = {
        m["test_run"]["run"]: m["test_run"]["status"]
        for m in messages if m.get("type") == "test_run"
    }
    errors = [m for m in messages if m.get("diagnostic", {}).get("severity") == "error"]
    plans = [m["test_plan"] for m in messages if m.get("type") == "test_plan"]
    message = (
        f"native_plan_unavailable statuses={statuses} errors={errors} plans={len(plans)}"
    )
    assert statuses == {"composicao_static_web": "pass"}, message
    assert not errors, message
    assert len(plans) == 1, message
    return plans[0]


@pytest.fixture(scope="module", params=("a", "b"), ids=("fronteira_a", "fronteira_b"))
def static_web_boundary(request: pytest.FixtureRequest) -> Node:
    return BOUNDARIES[request.param]


@pytest.fixture(scope="module")
def static_web_workspace(
    static_web_boundary: Node, tofu_context: Tofu, prod_root: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> Workspace:
    directory = tmp_path_factory.mktemp("sw")
    assert len(str(directory)) < 60, f"native_tmp_path_too_long path={directory}"
    root = directory / "infra/opentofu/env/prod"
    hidden = shutil.ignore_patterns(".terraform", ".tofu-data", "*.tfstate*", "*.tfvars*")
    shutil.copytree(prod_root, root, ignore=hidden)
    shutil.copytree(prod_root.parents[1] / "modules", directory / "infra/opentofu/modules")
    environment = dict(tofu_context[1])
    mirror = environment.pop("TF_PLUGIN_CACHE_DIR", "")
    assert mirror, "native_provider_mirror_required"
    assert Path(mirror).is_dir(), "native_provider_mirror_required"
    environment.update({"TF_DATA_DIR": str(directory / "data"), "TF_CLI_CONFIG_FILE": os.devnull})
    workspace = Workspace(root, (tofu_context[0], environment), static_web_boundary, {})
    _tofu(workspace, "init", (
        "init", "-backend=false", "-input=false", "-lockfile=readonly", f"-plugin-dir={mirror}",
    ))
    workspace.artifacts.update({
        "configuration": _tofu(workspace, "configuration", ("show", "-json", "-config")),
        "version": _tofu(workspace, "version", ("version", "-json")),
    })
    schema = _provider_schema(workspace.tofu, root, workspace.artifacts, (f"-plugin-dir={mirror}",))
    workspace.artifacts["schema"] = schema
    workspace.artifacts["seeds"] = root_identity_seeds(
        workspace.artifacts["configuration"], schema, ROOT_FIXTURE_CONTRACT
    )
    workspace.artifacts["overrides"] = _static_overrides(static_web_boundary, workspace.artifacts)
    records = {name: workspace.artifacts[name] for name in ("seeds", "overrides")}
    (Path(environment["TF_DATA_DIR"]) / "root-identity-records.json").write_text(
        json.dumps(records, indent=2), encoding="utf-8",
    )
    return workspace


@pytest.fixture(scope="module")
def static_web_composition(
    static_web_workspace: Workspace, valid_shared_outputs: Node,
) -> Composition:
    root = static_web_workspace.root
    tests = root / "tests"
    tests.mkdir(exist_ok=True)
    (tests / "composition.tftest.hcl").write_text(
        _suite(static_web_workspace.boundary, valid_shared_outputs,
               static_web_workspace.artifacts["overrides"]), encoding="utf-8",
    )
    validation = _tofu(static_web_workspace, "validation", ("validate", "-json"))
    assert validation.get("valid") is True, f"native_validation_failed diagnostics={validation}"
    configuration = static_web_workspace.artifacts["configuration"]
    plan = _tofu(static_web_workspace, "test", ("test", "-no-color", "-json", "-verbose"))
    assert_root_identity_producers(plan, static_web_workspace.artifacts["seeds"])
    return Composition(static_web_workspace, configuration, plan)


def _changes(composition: Composition) -> list[Node]:
    return [c for c in composition.plan.get("resource_changes", []) if c["mode"] == "managed"]


def _resource(composition: Composition, address: str) -> Node:
    found = [c for c in _changes(composition) if c["address"] == address]
    assert len(found) == 1, f"resource_required address={address} count={len(found)}"
    values = found[0]["change"].get("after")
    assert isinstance(values, dict), f"resource_values_required address={address}"
    return values


def _configured(composition: Composition, module: str | None = None) -> Node:
    root = composition.configuration["root_module"]
    if module is None:
        return root
    calls = root.get("module_calls", {})
    assert module in calls, f"module_required name={module}"
    return calls[module]


def _configuration_resource(composition: Composition, address: str) -> Node:
    root = _configured(composition)
    if address.startswith("module."):
        _, module, address = address.split(".", 2)
        root = _configured(composition, module)["module"]
    found = [r for r in root.get("resources", []) if r["address"] == address]
    assert len(found) == 1, f"configuration_resource_required address={address}"
    return found[0]


def _block(source: str, header: str, indent: int = 0) -> str:
    prefix = " " * indent
    tokens = r"\s+".join(re.escape(token) for token in header.split())
    pattern = rf"^{prefix}{tokens}\s*\{{\n(.*?)^{prefix}\}}[ \t]*$"
    found = re.findall(pattern, source, re.MULTILINE | re.DOTALL)
    assert len(found) == 1, f"input_block_required header={header} count={len(found)}"
    return found[0]


def _cognito_references(composition: Composition) -> None:
    source = (composition.workspace.root / "main.tf").read_text(encoding="utf-8")
    module = _block(source, 'module "static_web"')
    cognito = _block(module, "cognito =", 2)
    fields = re.findall(r"^\s*(\w+)\s*=\s*([^\n]+)$", cognito, re.MULTILINE)
    assert len(fields) == 3, f"cognito_field_count count={len(fields)}"
    assert {key: "".join(value.split()) for key, value in fields} == {
        "issuer_url": "module.cognito.issuer",
        "hosted_login_origin": "module.cognito.domain",
        "client_id": "module.cognito.client_id",
    }, "cognito_input_reference_required"
    references = _configured(composition, "static_web")["expressions"]["cognito"]["references"]
    assert {"module.cognito.issuer", "module.cognito.domain", "module.cognito.client_id"} <= set(
        references
    ), "cognito_configuration_reference_required"


def _report_only_csp(composition: Composition) -> str:
    policy = _resource(composition, "module.static_web.aws_cloudfront_response_headers_policy.this")
    security = policy["security_headers_config"][0]
    assert not security.get("content_security_policy"), "enforcing_csp_forbidden"
    assert not security.get("strict_transport_security"), "hsts_requires_validation"
    headers = policy["custom_headers_config"][0]["items"]
    values = [h["value"] for h in headers if h["header"] == "Content-Security-Policy-Report-Only"]
    assert values, "report_only_csp_required"
    assert len(values) == 1, "report_only_csp_required"
    assert isinstance(values[0], str), "report_only_csp_required"
    return values[0]


def _region(composition: Composition, address: str) -> str:
    key = _configuration_resource(composition, address)["provider_config_key"]
    providers = composition.configuration["provider_config"]
    assert key in providers, f"provider_binding_unresolved address={address} key={key}"
    return providers[key]["expressions"]["region"]["constant_value"]


def test_raiz_agenda_static_web_e_acm_do_produto(static_web_composition: Composition) -> None:
    composition = static_web_composition
    static = [c for c in _changes(composition) if c["address"].startswith("module.static_web.")]
    assert {c["address"] for c in static} == {
        f"module.static_web.{kind}.{'spa' if kind == 'aws_cloudfront_function' else 'this'}"
        for kind in STATIC_TYPES
    }, "static_web_resource_inventory_incomplete"
    assert _resource(composition, "module.static_web.aws_s3_bucket.this")["bucket"] == (
        "cnesdata-prod-static-web"
    )
    oac = _resource(composition, "module.static_web.aws_cloudfront_origin_access_control.this")
    assert oac["signing_behavior"] == "always"
    assert _resource(composition, "module.static_web.aws_cloudfront_distribution.this")["enabled"]
    assert _resource(composition, "module.static_web.aws_wafv2_web_acl.this")[
        "scope"
    ] == "CLOUDFRONT"
    assert _resource(composition, "aws_acm_certificate.static_web")["domain_name"] == APEX
    calls = _configured(composition).get("module_calls", {})
    assert [n for n, c in calls.items() if c["source"] == "../../modules/static-web"] == [
        "static_web"
    ]


def test_composicao_consume_cognito_serving_e_certificado(
    static_web_composition: Composition,
) -> None:
    composition = static_web_composition
    boundary = composition.workspace.boundary
    distribution = _resource(composition, "module.static_web.aws_cloudfront_distribution.this")
    assert distribution["viewer_certificate"][0]["acm_certificate_arn"] == (
        boundary["certificate_arn"]
    )
    assert _report_only_csp(composition) == (
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
        f"img-src 'self' data:; connect-src 'self' {ISSUER_ORIGIN} "
        f"{LOGIN_ORIGIN} https://api.cnesdata.com.br https://{boundary['serving_host']}; "
        "frame-ancestors 'none'"
    )
    _cognito_references(composition)


def test_composicao_permanece_em_validacao_sem_hsts(static_web_composition: Composition) -> None:
    composition = static_web_composition
    _report_only_csp(composition)
    lifecycle = _resource(
        composition, "module.static_web.aws_s3_bucket_lifecycle_configuration.this"
    )
    assert [rule["status"] for rule in lifecycle["rule"]] == ["Disabled"]
    waf = _resource(composition, "module.static_web.aws_wafv2_web_acl.this")
    groups = [rule["statement"][0]["managed_rule_group_statement"][0] for rule in waf["rule"]]
    assert sorted((group["name"], group["vendor_name"]) for group in groups) == [
        ("AWSManagedRulesAmazonIpReputationList", "AWS"), ("AWSManagedRulesCommonRuleSet", "AWS"),
    ]


def test_acm_apex_dns_e_borda_us_east_1(static_web_composition: Composition) -> None:
    composition = static_web_composition
    acm = _resource(composition, "aws_acm_certificate.static_web")
    assert acm["domain_name"] == APEX
    assert acm["validation_method"] == "DNS"
    assert set(acm.get("subject_alternative_names") or ()) <= {APEX}
    assert all(acm.get(k) is None for k in (
        "private_key", "certificate_body", "certificate_chain", "certificate_authority_arn",
    ))
    for address in ("aws_acm_certificate.static_web", "module.static_web.aws_wafv2_web_acl.this"):
        assert _region(
            composition, address
        ) == "us-east-1", f"edge_region_required address={address}"
    assert _region(composition, "module.static_web.aws_s3_bucket.this") == "us-east-2"
    source = (composition.workspace.root / "main.tf").read_text(encoding="utf-8")
    acm_source = _block(source, 'resource "aws_acm_certificate" "static_web"')
    assert "".join(_block(acm_source, "lifecycle", 2).split()) == "create_before_destroy=true"


def test_outputs_cloudfront_e_runtime_sao_utilizaveis(static_web_composition: Composition) -> None:
    composition = static_web_composition
    outputs = composition.plan["planned_values"].get("outputs", {})
    declared = _configured(composition)["outputs"]
    assert len(OUTPUTS) == 28
    assert set(declared) == OUTPUTS, f"root_output_inventory actual={set(declared)}"
    required_outputs = OUTPUTS - {"api_loopback_contract"}
    assert required_outputs <= set(outputs), "evaluated_root_outputs_required"
    assert declared["api_loopback_contract"]["expression"] == {"constant_value": None}
    assert outputs["processor_state_machine_arn"]["value"] is None
    if "api_loopback_contract" in outputs:
        assert "value" in outputs["api_loopback_contract"]
        assert outputs["api_loopback_contract"]["value"] is None
    boundary = composition.workspace.boundary
    assert outputs["cloudfront_domain_name"].get("value") == boundary["distribution_domain"]
    assert outputs["cloudfront_distribution_id"].get("value") == boundary["distribution_id"]
    assert outputs["dashboard_runtime_env"].get("value") == {"VITE_API_BASE_URL": API_URL}


def test_records_acm_preservam_nome_tipo_e_valor(static_web_composition: Composition) -> None:
    composition = static_web_composition
    outputs = composition.plan["planned_values"].get("outputs", {})
    assert "acm_dns_validation_records" in outputs, "acm_records_output_required"
    boundary = composition.workspace.boundary
    assert outputs["acm_dns_validation_records"].get("value") == {
        APEX: {"name": boundary["record_name"], "type": "CNAME", "value": boundary["record_value"]}
    }, "acm_record_projection_required"


def test_composicao_nao_cria_dns_assinatura_ou_estado_compartilhado(
    static_web_composition: Composition,
) -> None:
    changes = _changes(static_web_composition)
    assert changes, "managed_resource_inventory_required"
    for change in changes:
        address, kind = change["address"], change["type"]
        if not address.startswith("module."):
            assert address in {
                "aws_acm_certificate.static_web",
                "aws_cloudwatch_log_group.processor",
            }, f"root_ownership address={address}"
            continue
        module = address.split(".", 2)[1]
        message = f"product_ownership address={address} type={kind}"
        assert module in MODULE_TYPES, message
        assert kind in MODULE_TYPES[module], message
    log_group = _resource(static_web_composition, "aws_cloudwatch_log_group.processor")
    assert log_group["name"] == "/ecs/cnesdata-prod-processor"
    assert log_group["retention_in_days"] == 7
