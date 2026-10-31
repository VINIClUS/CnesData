import json
import re
import shutil
import subprocess
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import NamedTuple, cast

import pytest

from cnes_infra.control_plane.dynamodb_inventory import CONTROL_PLANE_INVENTORY
from tests.production.infra.conftest import (
    MOCK_AWS_PROVIDERS as MOCK_PROVIDERS,
)
from tests.production.infra.conftest import (
    MOCK_PROVIDER,
)

type Tofu = tuple[str, dict[str, str]]
type Artifacts = dict[str, dict[str, object]]
type Node = dict[str, object]

MODULE_DIRECTORY = Path(__file__).resolve().parents[3] / "infra/opentofu/modules/control-plane"
MODULE_SOURCE = "../../modules/control-plane"
ISOLATED_SOURCE = "./modules/control-plane"
TABLE = "aws_dynamodb_table.control_plane"
POLICY = "terraform_data.key_schema_policy"
TABLE_NAME = "cnesdata-prod-control-plane"
MOCK_TABLE_ARN = f"arn:aws:dynamodb:us-east-2:123456789012:table/{TABLE_NAME}"
ON_DEMAND_CEILING = {"max_read_request_units": 100, "max_write_request_units": 25}
POINT_IN_TIME_RECOVERY = {"enabled": True, "recovery_period_in_days": 35}
FORBIDDEN_TYPES = frozenset({
    "aws_appautoscaling_policy",
    "aws_appautoscaling_scheduled_action",
    "aws_appautoscaling_target",
    "aws_dynamodb_contributor_insights",
    "aws_dynamodb_global_table",
    "aws_dynamodb_table_replica",
})
FORBIDDEN_TABLE_ARGUMENTS = frozenset({
    "global_table_witness",
    "read_capacity",
    "replica",
    "stream_enabled",
    "write_capacity",
})
RUN_STATUS = re.compile(r'^\s*run "(?P<name>[^"]+)"\.\.\. (?P<status>\w+)$', re.MULTILINE)

ACTUAL_TABLE = f"""{{
      hash_key   = {TABLE}.hash_key
      range_key  = {TABLE}.range_key
      attributes = {{ for attribute in {TABLE}.attribute : attribute.name => attribute.type }}
      ttl        = {TABLE}.ttl
    }}"""
ACTUAL_INDEXES = f"""{{
      for index in {TABLE}.global_secondary_index : index.name => {{
        key_schema = [
          for key in index.key_schema : {{
            attribute_name = key.attribute_name
            key_type       = key.key_type
          }}
        ]
        projection_type    = index.projection_type
        non_key_attributes = index.non_key_attributes
      }}
    }}"""
ACTUAL_CEILINGS = (
    f"concat([{TABLE}.on_demand_throughput], "
    f"[for index in {TABLE}.global_secondary_index : index.on_demand_throughput])"
)
ACTUAL_OUTPUTS = (
    "{ table_name = output.table_name, table_arn = output.table_arn, "
    "index_arns = output.index_arns }"
)

MODULE_RUN = """
run "{name}" {{
  command = {command}

  module {{
    source = "{source}"
  }}

  variables {{
    table_name = "{table_name}"
  }}
{body}}}
"""


class Mutation(NamedTuple):
    name: str
    original: str
    replacement: str


class TofuReport(NamedTuple):
    statuses: dict[str, str]
    result: subprocess.CompletedProcess[str]


INDEX_LINE = 'gsi{0} = {{ hash_key = "gsi{0}pk", range_key = "gsi{0}sk", projection_type = "ALL" }}'
KEY_SCHEMA_MUTATIONS = (
    Mutation("chave_de_particao_renomeada", '"pk"', '"id"'),
    Mutation("chave_de_ordenacao_renomeada", '"sk"', '"sort"'),
    Mutation("atributo_de_chave_numerico", '"S"', '"N"'),
    Mutation("chave_de_indice_alterada", '"gsi3sk"', '"gsi3ts"'),
    Mutation(
        "projecao_de_indice_reduzida",
        '"gsi2sk", projection_type = "ALL"',
        '"gsi2sk", projection_type = "KEYS_ONLY"',
    ),
    Mutation("indice_removido", INDEX_LINE.format(5), ""),
    Mutation(
        "indice_de_conveniencia_adicionado",
        INDEX_LINE.format(6),
        f"{INDEX_LINE.format(6)}\n    {INDEX_LINE.format(7)}",
    ),
)


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


def _module_run(name: str, body: str, source: str = MODULE_SOURCE, command: str = "plan") -> str:
    return MODULE_RUN.format(
        name=name, command=command, source=source, table_name=TABLE_NAME, body=body
    )


def _expected_table() -> dict[str, object]:
    inventory = CONTROL_PLANE_INVENTORY
    attribute_type = inventory.key_attribute_type
    return {
        "hash_key": inventory.partition_key,
        "range_key": inventory.sort_key,
        "attributes": dict.fromkeys(inventory.key_attributes(), attribute_type),
        "ttl": [{"attribute_name": inventory.ttl_attribute, "enabled": True}],
    }


def _expected_indexes() -> dict[str, object]:
    return {
        index.name: {
            "key_schema": [
                {"attribute_name": index.partition_key, "key_type": "HASH"},
                {"attribute_name": index.sort_key, "key_type": "RANGE"},
            ],
            "projection_type": index.projection,
            "non_key_attributes": None,
        }
        for index in CONTROL_PLANE_INVENTORY.indexes
    }


def _expected_outputs() -> dict[str, object]:
    return {
        "table_name": TABLE_NAME,
        "table_arn": MOCK_TABLE_ARN,
        "index_arns": {
            index.name: f"{MOCK_TABLE_ARN}/index/{index.name}"
            for index in CONTROL_PLANE_INVENTORY.indexes
        },
    }


def _root_run(shared_outputs: Mapping[str, object]) -> str:
    names = sorted(index.name for index in CONTROL_PLANE_INVENTORY.indexes)
    body = _assertion(
        f'module.control_plane.table_name == "{TABLE_NAME}"', "root_table_name"
    ) + _equal("keys(module.control_plane.index_arns)", names, "root_index_names")
    return f"""
run "raiz_agenda_control_plane" {{
  command = plan

  variables {{
    shared_outputs = {_hcl(shared_outputs)}
  }}
{body}}}
"""


def _bounded_capacity_body() -> str:
    ceilings = [[ON_DEMAND_CEILING]] * (1 + len(CONTROL_PLANE_INVENTORY.indexes))
    return (
        _assertion(f'{TABLE}.billing_mode == "PAY_PER_REQUEST"', "billing_mode")
        + _equal(f"{TABLE}.point_in_time_recovery", [POINT_IN_TIME_RECOVERY], "pitr")
        + _equal(ACTUAL_CEILINGS, ceilings, "on_demand_throughput")
        + _assertion(f"{TABLE}.deletion_protection_enabled == true", "deletion_protection")
        + _assertion(f"length({TABLE}.server_side_encryption) == 0", "aws_owned_encryption")
    )


def _plan_suite(shared_outputs: Mapping[str, object]) -> str:
    unreplicated = (
        _assertion(f"length({TABLE}.replica) == 0", "replica")
        + _assertion(f"length({TABLE}.global_table_witness) == 0", "global_table_witness")
        + _assertion(f"{TABLE}.stream_enabled != true", "stream_enabled")
    )
    arn_override = f"""
  override_resource {{
    target = {TABLE}
    values = {{
      arn = "{MOCK_TABLE_ARN}"
    }}
  }}
"""
    return MOCK_PROVIDERS + "".join((
        _root_run(shared_outputs),
        _module_run(
            "inventario_de_chaves_e_indices",
            _equal(ACTUAL_TABLE, _expected_table(), "table_inventory")
            + _equal(ACTUAL_INDEXES, _expected_indexes(), "index_inventory"),
        ),
        _module_run("capacidade_sob_demanda_limitada", _bounded_capacity_body()),
        _module_run("sem_replica_insights_ou_autoscaling", unreplicated),
        _module_run(
            "arns_exatos_dos_indices",
            arn_override + _equal(ACTUAL_OUTPUTS, _expected_outputs(), "outputs"),
        ),
    ))


def _policy_suite() -> str:
    expected_failure = f"\n  expect_failures = [{POLICY}]\n"
    mutated = (
        _module_run(mutation.name, expected_failure, f"./modules/{mutation.name}")
        for mutation in KEY_SCHEMA_MUTATIONS
    )
    canonical = _module_run("esquema_canonico", "", ISOLATED_SOURCE)
    return MOCK_PROVIDERS + canonical + "".join(mutated)


def _protection_suite() -> str:
    replace = f"\n  plan_options {{\n    replace = [{TABLE}]\n  }}\n"
    return (
        MOCK_PROVIDERS
        + _module_run("cria_tabela", "", ISOLATED_SOURCE, "apply")
        + _module_run("substitui_tabela", replace, ISOLATED_SOURCE)
    )


def _tofu(tofu: Tofu, root: Path, arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [tofu[0], f"-chdir={root}", *arguments],
        check=False,
        capture_output=True,
        env=tofu[1],
        text=True,
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


def _write_mutation(modules: Path, mutation: Mutation) -> None:
    target = modules / mutation.name
    shutil.copytree(modules / "control-plane", target)
    main = target / "main.tf"
    source = main.read_text(encoding="utf-8")
    occurrences = source.count(mutation.original)
    if occurrences != 1:
        pytest.fail(f"mutation_not_unique name={mutation.name} occurrences={occurrences}")
    main.write_text(source.replace(mutation.original, mutation.replacement), encoding="utf-8")


def _write_isolated_root(root: Path, prod_root: Path) -> None:
    modules = root / "modules"
    hidden = shutil.ignore_patterns(".*")
    shutil.copytree(MODULE_DIRECTORY, modules / "control-plane", ignore=hidden)
    for mutation in KEY_SCHEMA_MUTATIONS:
        _write_mutation(modules, mutation)
    for name in ("versions.tf", ".terraform.lock.hcl"):
        shutil.copy2(prod_root / name, root / name)
    tests = root / "tests"
    tests.mkdir()
    (tests / "policy.tftest.hcl").write_text(_policy_suite(), encoding="utf-8")
    (tests / "protection.tftest.hcl").write_text(_protection_suite(), encoding="utf-8")


def _root_module(tofu_artifacts: Artifacts) -> Node:
    return cast("Node", tofu_artifacts["configuration"]["root_module"])


def _module_calls(module: Node) -> dict[str, Node]:
    return cast("dict[str, Node]", module.get("module_calls", {}))


def _resources(module: Node) -> list[Node]:
    return cast("list[Node]", module.get("resources", []))


def _graph_types(module: Node) -> Iterator[str]:
    yield from (cast("str", resource["type"]) for resource in _resources(module))
    for call in _module_calls(module).values():
        yield from _graph_types(cast("Node", call["module"]))


def _root_aws_providers(tofu_artifacts: Artifacts) -> set[str]:
    providers = cast("dict[str, Node]", tofu_artifacts["configuration"]["provider_config"])
    return {
        key for key, provider in providers.items()
        if provider["name"] == "aws" and "module_address" not in provider
    }


def _mocked_providers(suite: str) -> set[str]:
    return {f"aws.{alias}" if alias else "aws" for alias in MOCK_PROVIDER.findall(suite)}


@pytest.fixture(scope="module")
def plan_report(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    valid_shared_outputs: dict[str, object],
) -> Iterator[TofuReport]:
    test_directory = prod_root / ".terraform" / "control-plane-tests"
    test_directory.mkdir(parents=True)
    selector = "-test-directory=.terraform/control-plane-tests"
    try:
        (test_directory / "control-plane.tftest.hcl").write_text(
            _plan_suite(valid_shared_outputs), encoding="utf-8"
        )
        init = _tofu(tofu_context, prod_root, ("init", "-backend=false", "-input=false", selector))
        _require_success(init, "init")
        yield _report(_tofu(tofu_context, prod_root, ("test", "-no-color", selector)))
    finally:
        shutil.rmtree(test_directory)


@pytest.fixture(scope="module")
def isolated_report(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> TofuReport:
    root = tmp_path_factory.mktemp("control-plane-policy")
    _write_isolated_root(root, prod_root)
    providers = Path(tofu_context[1]["TF_DATA_DIR"]) / "providers"
    tofu = (tofu_context[0], {**tofu_context[1], "TF_DATA_DIR": str(root / ".tofu-data")})
    init = ("init", "-backend=false", "-input=false", f"-plugin-dir={providers}")
    _require_success(_tofu(tofu, root, init), "init-isolated")
    return _report(_tofu(tofu, root, ("test", "-no-color")))


def test_raiz_agenda_tabela_do_control_plane(
    plan_report: TofuReport, tofu_artifacts: Artifacts, prod_root: Path
) -> None:
    calls = _module_calls(_root_module(tofu_artifacts))
    module = (prod_root / MODULE_SOURCE).resolve()
    targets = {n: (prod_root / cast("str", c["source"])).resolve() for n, c in calls.items()}

    _assert_runs(plan_report, {"raiz_agenda_control_plane": "pass"})
    assert [n for n, target in targets.items() if target == module] == ["control_plane"]
    assert calls["control_plane"]["source"] == MODULE_SOURCE


def test_tabela_tem_inventario_exato_de_chaves_e_indices(plan_report: TofuReport) -> None:
    _assert_runs(
        plan_report,
        {"inventario_de_chaves_e_indices": "pass", "arns_exatos_dos_indices": "pass"},
    )


def test_tabela_sob_demanda_com_pitr_35_e_teto_de_throughput(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, {"capacidade_sob_demanda_limitada": "pass"})


def test_tabela_sem_replica_insights_ou_autoscaling(
    plan_report: TofuReport,
    tofu_artifacts: Artifacts,
) -> None:
    root = _root_module(tofu_artifacts)
    module = cast("Node", _module_calls(root)["control_plane"]["module"])
    resources = {cast("str", resource["address"]): resource for resource in _resources(module)}
    arguments = set(cast("Node", resources[TABLE]["expressions"]))

    _assert_runs(plan_report, {"sem_replica_insights_ou_autoscaling": "pass"})
    assert set(resources) == {TABLE, POLICY}
    assert not FORBIDDEN_TYPES & set(_graph_types(root))
    assert not FORBIDDEN_TABLE_ARGUMENTS & arguments


def test_tabela_tem_prevent_destroy(isolated_report: TofuReport) -> None:
    result = isolated_report.result
    output = " ".join((result.stdout + result.stderr).split())

    _assert_runs(isolated_report, {"cria_tabela": "pass", "substitui_tabela": "fail"})
    assert f"Resource instance {TABLE} has prevent_destroy set" in output


@pytest.mark.parametrize("mutation", KEY_SCHEMA_MUTATIONS, ids=lambda mutation: mutation.name)
def test_mudanca_de_key_schema_falha_politica(
    isolated_report: TofuReport,
    mutation: Mutation,
) -> None:
    _assert_runs(isolated_report, {"esquema_canonico": "pass", mutation.name: "pass"})


def test_suites_gerados_mockam_todo_provider_aws_da_raiz(
    tofu_artifacts: Artifacts,
    valid_shared_outputs: dict[str, object],
) -> None:
    roots = _root_aws_providers(tofu_artifacts)
    suites = (_plan_suite(valid_shared_outputs), _policy_suite(), _protection_suite())

    assert {"aws", "aws.global_edge"} <= roots
    assert [_mocked_providers(suite) for suite in suites] == [roots] * len(suites)
