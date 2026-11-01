import json
from collections.abc import Callable, Iterator, Sequence
from copy import deepcopy
from pathlib import Path
from re import fullmatch
from typing import cast

import pytest

from tests.production.infra._athena_contracts import ROOT_OUTPUTS
from tests.production.infra._composition_contracts import (
    ARN_PATHS,
    BPA,
    INVALID_SHARED_VALUES,
    PROHIBITED_RESOURCE_TYPES,
    SHARED_RESOURCE_EXACT,
    SHARED_RESOURCE_PREFIXES,
)
from tests.production.infra._root_native_observation import NativeRootReport

ToolLookup = Callable[[str], str | None]
ValidationCase = tuple[str, object, bool]
SharedValidator = Callable[[Sequence[ValidationCase], frozenset[str]], NativeRootReport]


def _assert_native_runs(report: NativeRootReport, names: set[str]) -> None:
    assert report.result.returncode == 0, repr(report.result)
    assert report.statuses == dict.fromkeys(names, "pass")
    counts = report.observations["summary"]["test_summary"]
    expected = {"passed": len(names), "failed": 0, "errored": 0, "skipped": 0, "status": "pass"}
    assert {key: counts[key] for key in expected} == expected
    assert not any(e["diagnostic"].get("severity") == "error"
                   for e in report.observations["diagnostics"])


def _native_errors(report: NativeRootReport, run: str) -> str:
    events = [e for e in report.observations["diagnostics"]
              if e["diagnostic"].get("severity") == "error"]
    assert all(e.get("@testrun") in {None, run} for e in events)
    return report.result.stderr + "\n".join(
        e["diagnostic"].get("summary", "") + " " + e["diagnostic"].get("detail", "") for e in events
    )


def _compact_assignments(source: str) -> set[str]:
    return {"".join(line.split()) for line in source.splitlines() if line.strip()}


def _configuration_lines(source: str) -> set[str]:
    return {
        line.strip()
        for line in source.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def _provider(tofu_artifacts: dict[str, dict[str, object]], name: str) -> dict[str, object]:
    configuration = tofu_artifacts["configuration"]
    providers = cast("dict[str, object]", configuration["provider_config"])
    return cast("dict[str, object]", providers[name])


def _schema_type(schema: dict[str, object]) -> object:
    type_name = schema["type"]
    if type_name == "object":
        properties = cast("dict[str, dict[str, object]]", schema["properties"])
        return ["object", {name: _schema_type(value) for name, value in properties.items()}]
    return {"boolean": "bool", "integer": "number", "string": "string"}[cast("str", type_name)]


def _assert_objects_are_closed(schema: dict[str, object]) -> None:
    if schema.get("type") != "object":
        return
    properties = cast("dict[str, dict[str, object]]", schema["properties"])
    assert schema["additionalProperties"] is False
    assert set(cast("list[str]", schema["required"])) == set(properties)
    for child in properties.values():
        _assert_objects_are_closed(child)


def _schema_property(schema: dict[str, object], *path: str) -> dict[str, object]:
    current = schema
    for name in path:
        properties = cast("dict[str, dict[str, object]]", current["properties"])
        current = properties[name]
    return current


def _replace_value(
    shared_outputs: dict[str, object],
    path: tuple[str, ...],
    value: object,
) -> dict[str, object]:
    clone = deepcopy(shared_outputs)
    current = clone
    for name in path[:-1]:
        current = cast("dict[str, object]", current[name])
    current[path[-1]] = value
    return clone


def _synthetic_configuration(*resources: dict[str, object]) -> dict[str, object]:
    return {
        "root_module": {"module_calls": {"synthetic": {"module": {"resources": list(resources)}}}}
    }


def _synthetic_resource(
    resource_type: str,
    provider: str,
    expressions: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "address": f"{resource_type}.synthetic",
        "mode": "managed",
        "type": resource_type,
        "name": "synthetic",
        "provider_config_key": provider,
        "expressions": expressions or {},
    }


def _walk_resources(module: dict[str, object]) -> Iterator[dict[str, object]]:
    resources = cast("list[dict[str, object]]", module.get("resources", []))
    yield from resources
    module_calls = cast("dict[str, dict[str, object]]", module.get("module_calls", {}))
    for module_call in module_calls.values():
        child = module_call.get("module")
        if isinstance(child, dict):
            yield from _walk_resources(child)


def _configuration_resources(configuration: dict[str, object]) -> Iterator[dict[str, object]]:
    root_module = cast("dict[str, object]", configuration["root_module"])
    yield from _walk_resources(root_module)


def _assert_product_ownership(configuration: dict[str, object]) -> None:
    for resource in _configuration_resources(configuration):
        resource_type = cast("str", resource["type"])
        shared = resource_type in SHARED_RESOURCE_EXACT or resource_type.startswith(
            SHARED_RESOURCE_PREFIXES
        )
        assert not shared, f"shared_resource address={resource['address']} type={resource_type}"


def _assert_resource_region(resource: dict[str, object]) -> None:
    resource_type = cast("str", resource["type"])
    if not resource_type.startswith("aws_"):
        return
    provider = resource.get("provider_config_key")
    assert provider in {"aws", "aws.global_edge"}, (
        f"region_contract address={resource['address']} provider={provider}"
    )
    is_global = provider == "aws.global_edge"
    expressions = cast("dict[str, dict[str, object]]", resource.get("expressions", {}))
    if is_global:
        allowed = resource_type.startswith(("aws_acm_", "aws_wafv2_"))
        assert allowed, f"region_contract address={resource['address']} global_type={resource_type}"
    if resource_type.startswith("aws_wafv2_"):
        scope = expressions.get("scope", {}).get("constant_value")
        assert is_global == (scope == "CLOUDFRONT"), (
            f"region_contract address={resource['address']} provider={provider} waf_scope={scope}"
        )
    if "region" in expressions:
        expected = "us-east-1" if is_global else "us-east-2"
        actual = expressions["region"].get("constant_value")
        assert actual == expected, (
            f"region_contract address={resource['address']} region={actual} expected={expected}"
        )


def _assert_resource_regions(configuration: dict[str, object]) -> None:
    for resource in _configuration_resources(configuration):
        _assert_resource_region(resource)


def test_backend_usa_lockfile_sem_tabela_dynamodb(prod_root: Path) -> None:
    source = (prod_root / "versions.tf").read_text(encoding="utf-8")
    compact = _compact_assignments(source)

    assert 'key="cnesdata/prod/opentofu.tfstate"' in compact
    assert "encrypt=true" in compact
    assert "use_lockfile=true" in compact
    assert "dynamodb_table" not in source


def test_exemplo_de_backend_so_tem_placeholders(prod_root: Path) -> None:
    source = (prod_root / "backend.hcl.example").read_text(encoding="utf-8")

    assert _compact_assignments(source) == {
        'bucket="<STATE_BUCKET_NAME>"',
        'kms_key_id="<KMS_KEY_ARN>"',
        'region="<AWS_REGION>"',
    }


@pytest.mark.parametrize("ci_value", ["true", ""])
def test_ferramenta_ausente_falha_em_ci(
    monkeypatch: pytest.MonkeyPatch,
    require_tool: Callable[[str, ToolLookup], str],
    ci_value: str,
) -> None:
    monkeypatch.setenv("CI", ci_value)

    with pytest.raises(BaseException) as error:
        require_tool("tofu", lambda _name: None)
    assert isinstance(error.value, pytest.fail.Exception)
    assert "tool_missing name=tofu" in str(error.value)


def test_configuracao_opentofu_e_valida(
    tofu_artifacts: dict[str, dict[str, object]],
) -> None:
    assert tofu_artifacts["validation"]["valid"] is True


def test_versoes_do_opentofu_e_provider_estao_fixadas(
    prod_root: Path,
    tofu_artifacts: dict[str, dict[str, object]],
) -> None:
    versions = _compact_assignments((prod_root / "versions.tf").read_text(encoding="utf-8"))
    provider = _provider(tofu_artifacts, "aws")
    selections = cast(
        "dict[str, object]",
        tofu_artifacts["version"]["provider_selections"],
    )

    assert 'required_version=">=1.10.0"' in versions
    assert provider["version_constraint"] == "~> 6.33"
    assert selections["registry.opentofu.org/hashicorp/aws"] == "6.54.0"


def test_providers_definem_regioes_e_tags_nativas(
    tofu_artifacts: dict[str, dict[str, object]],
) -> None:
    default = _provider(tofu_artifacts, "aws")
    global_edge = _provider(tofu_artifacts, "aws.global_edge")

    for provider, expected_region in ((default, "us-east-2"), (global_edge, "us-east-1")):
        expressions = cast("dict[str, object]", provider["expressions"])
        region = cast("dict[str, object]", expressions["region"])
        default_tags = cast("list[dict[str, object]]", expressions["default_tags"])
        tags = cast("dict[str, object]", default_tags[0]["tags"])
        assert region["constant_value"] == expected_region
        assert tags["references"] == ["local.common_tags"]

    assert global_edge["alias"] == "global_edge"


def test_locals_de_nome_e_tags_sao_avaliados_pelo_opentofu(
    native_contract_result: NativeRootReport,
) -> None:
    _assert_native_runs(native_contract_result, {"evaluate_composition_contract"})


def test_tipo_shared_outputs_corresponde_ao_schema(
    prod_root: Path,
    tofu_artifacts: dict[str, dict[str, object]],
    expected_shared_type: list[object],
) -> None:
    configuration = tofu_artifacts["configuration"]
    root_module = cast("dict[str, object]", configuration["root_module"])
    variables = cast("dict[str, dict[str, object]]", root_module["variables"])
    shared_outputs = variables["shared_outputs"]
    schema = json.loads((prod_root / "shared-outputs.schema.json").read_text(encoding="utf-8"))

    assert shared_outputs["required"] is True
    assert shared_outputs["type"] == expected_shared_type
    assert _schema_type(schema) == expected_shared_type


def test_schema_shared_outputs_e_fechado_versionado_e_formatado(prod_root: Path) -> None:
    schema = json.loads((prod_root / "shared-outputs.schema.json").read_text(encoding="utf-8"))

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    schema_id = schema["$id"]
    assert isinstance(schema_id, str)
    assert schema_id.endswith("shared-outputs-v2.schema.json")
    assert _schema_property(schema, "schema_version")["const"] == 2
    assert _schema_property(schema, "account_id")["pattern"] == "^[0-9]{12}$"
    assert _schema_property(schema, "roles_anywhere", "crl", "sha256")["pattern"] == (
        "^[0-9a-f]{64}$"
    )
    assert _schema_property(schema, "roles_anywhere", "crl", "crl_id")["pattern"] == "\\S"
    assert _schema_property(schema, "roles_anywhere", "crl", "next_update")["format"] == (
        "date-time"
    )
    for path in ARN_PATHS:
        pattern = _schema_property(schema, *path)["pattern"]
        assert isinstance(pattern, str)
        assert pattern.startswith("^arn:")
        assert fullmatch(pattern, "arn:aws:kms:us-east-2:1:key/example") is None
    _assert_objects_are_closed(schema)


def test_validacao_nativa_aceita_shared_outputs_valido(
    validate_shared_outputs: SharedValidator,
    valid_shared_outputs: dict[str, object],
) -> None:
    cases = (
        ("enabled_true", valid_shared_outputs, False),
        (
            "enabled_false",
            _replace_value(valid_shared_outputs, ("roles_anywhere", "crl", "enabled"), False),
            False,
        ),
    )
    report = validate_shared_outputs(cases, frozenset({"enabled_true", "enabled_false"}))
    _assert_native_runs(report, {"enabled_true", "enabled_false"})


def test_validacao_nativa_rejeita_shared_outputs_invalido(
    validate_shared_outputs: SharedValidator,
    valid_shared_outputs: dict[str, object],
) -> None:
    cases = tuple(
        (
            f"invalid_{index}",
            _replace_value(valid_shared_outputs, path, invalid_value),
            True,
        )
        for index, (path, invalid_value) in enumerate(INVALID_SHARED_VALUES)
    )
    report = validate_shared_outputs(cases, frozenset())
    _assert_native_runs(report, {case[0] for case in cases})


def test_validacao_nativa_rejeita_shared_outputs_nulo(
    validate_shared_outputs: SharedValidator,
) -> None:
    report = validate_shared_outputs((("null", None, False),), frozenset())
    assert report.result.returncode != 0
    assert report.statuses == {"null": "error"}
    counts = report.observations["summary"]["test_summary"]
    assert counts["passed"] == counts["skipped"] == 0
    assert counts["failed"] + counts["errored"] == 1
    assert counts["status"] != "pass"
    assert "required variable may not be set to null" in _native_errors(report, "null")


def test_validacao_nativa_rejeita_shared_outputs_sem_account_baseline(
    validate_shared_outputs: SharedValidator, valid_shared_outputs: dict[str, object]
) -> None:
    absent = {key: value for key, value in valid_shared_outputs.items() if key != BPA[0]}
    report = validate_shared_outputs((("account_baseline_ausente", absent, False),), frozenset())

    assert report.result.returncode != 0
    errors = _native_errors(report, "account_baseline_ausente")
    assert 'attribute "account_baseline" is required' in errors


def test_inventario_de_outputs_publica_interfaces_do_static_web(
    tofu_artifacts: dict[str, dict[str, object]],
) -> None:
    root_module = cast("dict[str, object]", tofu_artifacts["configuration"]["root_module"])
    outputs = cast("dict[str, dict[str, object]]", root_module["outputs"])
    static = {
        "acm_dns_validation_records", "api_loopback_contract", "dashboard_runtime_env",
        "cloudfront_distribution_id", "cloudfront_domain_name",
    }
    names = ["repository_uri", "task_definition_arns", "container_name"]
    names += ["task_role_arns", "task_role_names", "execution_role_arn", "execution_role_name"]
    names += [
        "cluster_arn", "state_machine_arn", "step_functions_role_id", "step_functions_role_arn",
    ]
    scheduler = {
        "scheduler_schedule_arns", "scheduler_role_arns", "scheduler_freeze_target_role_names",
        "scheduler_task_hour_budget", "scheduler_alarm_names",
    }
    expected = static | scheduler | ROOT_OUTPUTS | {f"processor_{name}" for name in names}
    assert set(outputs) == expected, f"processing_public_inventory={sorted(outputs)}"
    assert outputs["api_loopback_contract"]["expression"] == {"constant_value": None}
    for name in outputs.keys() - {"api_loopback_contract"}:
        expression = cast("dict[str, object]", outputs[name]["expression"])
        assert expression.get("references"), f"output_reference_required name={name}"


def test_grafo_do_produto_nao_contem_recurso_compartilhado(
    tofu_artifacts: dict[str, dict[str, object]],
) -> None:
    _assert_product_ownership(tofu_artifacts["configuration"])


def test_todo_recurso_usa_us_east_2_exceto_borda_global(
    tofu_artifacts: dict[str, dict[str, object]],
) -> None:
    _assert_resource_regions(tofu_artifacts["configuration"])


@pytest.mark.parametrize("resource_type", PROHIBITED_RESOURCE_TYPES)
def test_guardrail_de_ownership_rejeita_recurso_compartilhado_aninhado(
    resource_type: str,
) -> None:
    configuration = _synthetic_configuration(_synthetic_resource(resource_type, "aws"))

    with pytest.raises(AssertionError, match="shared_resource"):
        _assert_product_ownership(configuration)


def test_guardrail_de_ownership_aceita_recurso_do_produto() -> None:
    configuration = _synthetic_configuration(_synthetic_resource("aws_s3_bucket", "aws"))

    _assert_product_ownership(configuration)


@pytest.mark.parametrize(
    "resource",
    [
        _synthetic_resource("aws_s3_bucket", "aws.global_edge"),
        _synthetic_resource("aws_s3_bucket", "aws.other"),
        _synthetic_resource(
            "aws_s3_bucket",
            "aws",
            {"region": {"constant_value": "us-east-1"}},
        ),
        _synthetic_resource(
            "aws_acm_certificate",
            "aws.global_edge",
            {"region": {"constant_value": "us-east-2"}},
        ),
        _synthetic_resource(
            "aws_wafv2_web_acl",
            "aws.global_edge",
            {"scope": {"constant_value": "REGIONAL"}},
        ),
        _synthetic_resource(
            "aws_wafv2_web_acl",
            "aws",
            {"scope": {"constant_value": "CLOUDFRONT"}},
        ),
    ],
)
def test_guardrail_de_regiao_rejeita_provider_ou_regiao_incorreta(
    resource: dict[str, object],
) -> None:
    with pytest.raises(AssertionError, match="region_contract"):
        _assert_resource_regions(_synthetic_configuration(resource))


def test_guardrail_de_regiao_aceita_default_e_borda_global() -> None:
    configuration = _synthetic_configuration(
        _synthetic_resource("aws_s3_bucket", "aws"),
        _synthetic_resource("aws_acm_certificate", "aws.global_edge"),
        _synthetic_resource(
            "aws_wafv2_web_acl",
            "aws.global_edge",
            {"scope": {"constant_value": "CLOUDFRONT"}},
        ),
    )

    _assert_resource_regions(configuration)


def test_gitignore_protege_artefatos_opentofu_sem_ocultar_raiz_prod(
    repo_root: Path,
    git_ignored_paths: Callable[[Sequence[str]], set[str]],
) -> None:
    lines = _configuration_lines((repo_root / ".gitignore").read_text(encoding="utf-8"))
    candidates = (
        "terraform.tfvars.json",
        "prod.auto.tfvars.json",
        "terraform.tfvars.json.example",
        "prod.auto.tfvars.json.example",
    )

    assert "/env/" in lines
    assert "env/" not in lines
    assert {
        "*.tfstate",
        "*.tfstate.*",
        "*.tfvars",
        "!*.tfvars.example",
        "*.tfvars.json",
        "!*.tfvars.json.example",
        "*.tfplan",
        "*.tfplan.*",
    } <= lines
    assert git_ignored_paths(candidates) == set(candidates[:2])


def test_codeowners_define_ownership_de_infra_e_policies(repo_root: Path) -> None:
    source = (repo_root / ".github" / "CODEOWNERS").read_text(encoding="utf-8")
    owners = {
        parts[0]: parts[1:] for line in _configuration_lines(source) if (parts := line.split())
    }

    assert owners["infra/**"] == ["@VINIClUS"]
    assert owners["policies/**"] == ["@VINIClUS"]
