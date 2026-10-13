import json
import re
import shutil
import subprocess
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple, cast

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.stub import Stubber

from cnes_infra.aws import AwsRuntimeSettings
from cnes_infra.executor.step_functions import (
    ProductionNetworkPolicy,
    validate_production_state_machine,
)
from packages.cnes_infra.tests.aws.production_values import PRODUCTION_VALUES

type Tofu = tuple[str, dict[str, str]]
type Artifacts = dict[str, dict[str, object]]
type Node = dict[str, object]
type Resource = tuple[str, str, Mapping[str, object]]
type Graphs = tuple[list[Resource], list[Resource]]

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_DIRECTORY = REPO_ROOT / "infra/opentofu/modules/network"
ASL_FIXTURES = REPO_ROOT / "packages/cnes_infra/tests/fixtures/step_functions"
MODULE_SOURCE = "../../modules/network"
PUBLIC_SUBNETS = {"use2-az1": "10.42.0.0/26"}
NETWORK = {
    "name_prefix": "cnesdata-prod", "vpc_cidr": "10.42.0.0/24", "public_subnets": PUBLIC_SUBNETS,
}
ROOT_INPUTS = {
    "name_prefix": {"references": ["local.name_prefix"]},
    **{name: {"constant_value": value} for name, value in NETWORK.items() if name != "name_prefix"},
}
RESOLVER = "10.42.0.2/32"
SUBNET_ID, SECURITY_GROUP_ID = "subnet-0123456789abcdef0", "sg-0123456789abcdef0"
ROUTE_TABLE_ID, INTERNET_GATEWAY_ID = "rtb-0123456789abcdef0", "igw-0123456789abcdef0"
ENDPOINT_ID = "vpce-0123456789abcdef0"
OVERRIDES = {
    ("resource", "aws_subnet.public"): {"id": SUBNET_ID},
    ("resource", "aws_security_group.tasks"): {"id": SECURITY_GROUP_ID},
    ("resource", "aws_route_table.public"): {"id": ROUTE_TABLE_ID},
    ("resource", "aws_internet_gateway.this"): {"id": INTERNET_GATEWAY_ID},
    ("resource", "aws_vpc_endpoint.gateway"): {"id": ENDPOINT_ID, "prefix_list_id": "pl-0abc1234"},
    ("data", "data.aws_region.current"): {"region": "us-east-2"},
}
WITH_ENDPOINTS, WITHOUT_ENDPOINTS = "modulo_network", "modulo_network_sem_endpoints"
ROOT_RUN = "raiz_agenda_network"
MODULE_RUNS = {
    WITH_ENDPOINTS: NETWORK,
    WITHOUT_ENDPOINTS: {**NETWORK, "gateway_endpoints": {"s3": False, "dynamodb": False}},
}
THREE_AZS = {f"use2-az{n}": f"10.42.0.{64 * (n - 1)}/26" for n in (1, 2, 3)}
INVALID_RUNS = {
    "rede_sem_subnet_e_rejeitada": {"public_subnets": {}},
    "tres_azs_sao_rejeitadas": {"public_subnets": THREE_AZS},
    "nome_de_az_no_lugar_do_id_e_rejeitado": {"public_subnets": {"us-east-2a": "10.42.0.0/26"}},
    "subnet_ipv6_e_rejeitada": {"public_subnets": {"use2-az1": "2600:1f16::/64"}},
    "subnet_nao_canonica_e_rejeitada": {"public_subnets": {"use2-az1": "10.42.0.1/26"}},
    "vpc_nao_canonica_e_rejeitada": {"vpc_cidr": "10.42.0.1/24"},
    "vpc_maior_que_16_e_rejeitada": {"vpc_cidr": "10.0.0.0/8"},
}
GATEWAY_ENDPOINT_IDS = {"s3": ENDPOINT_ID, "dynamodb": ENDPOINT_ID}
DNS_EGRESS = (("tcp", 53, 53, RESOLVER), ("udp", 53, 53, RESOLVER))
PUBLIC_TLS_EGRESS = ("tcp", 443, 443, "0.0.0.0/0")
GATEWAY_TLS_EGRESS = ("tcp", 443, 443, "prefix_list")
SECURITY_GROUPS = ("aws_security_group", "aws_default_security_group")
FORBIDDEN_PREFIXES = (
    ("aws_nat_gateway", "nat_gateway"),
    ("aws_egress_only_internet_gateway", "egress_only_internet_gateway"),
    ("aws_lb_listener", "listener"), ("aws_alb_listener", "listener"),
    ("aws_lb_target_group", "target_group"), ("aws_alb_target_group", "target_group"),
    ("aws_lb", "load_balancer"), ("aws_alb", "load_balancer"), ("aws_elb", "load_balancer"),
    ("aws_ecs_service", "ecs_service"), ("aws_service_discovery_", "service_discovery"),
    ("aws_vpc_security_group_ingress_rule", "ingress"),
)
NAT_LB_LISTENER = (
    "nat_gateway", "egress_only_internet_gateway", "load_balancer", "target_group", "listener",
    "ecs_service", "service_discovery",
)
LB_ARN = "arn:aws:elasticloadbalancing:us-east-2:123456789012"
VPC = 'vpc_id = "vpc-0123456789abcdef0"'
GROUP = "security_group_id = module.network.security_group_id"
ALL_PORTS = ('protocol = "-1"', "from_port = 0", "to_port = 0")
ANYWHERE = ('ip_protocol = "-1"', 'cidr_ipv4 = "0.0.0.0/0"')
SYNTHETIC = {
    "aws_nat_gateway": ("nat_gateway", ["subnet_id = module.network.public_subnet_ids[0]"]),
    "aws_egress_only_internet_gateway": ("egress_only_internet_gateway", [VPC]),
    "aws_lb": ("load_balancer", ["subnets = module.network.public_subnet_ids"]),
    "aws_lb_target_group": ("target_group", []),
    "aws_lb_listener": ("listener", [
        f'load_balancer_arn = "{LB_ARN}:loadbalancer/app/proibido/1"',
        'default_action {\n    type = "forward"\n'
        f'    target_group_arn = "{LB_ARN}:targetgroup/proibido/1"\n  }}',
    ]),
    "aws_ecs_service": ("ecs_service", ['name = "proibido"']),
    "aws_service_discovery_http_namespace": ("service_discovery", ['name = "proibido"']),
    "aws_vpc_endpoint": ("interface_endpoint", [
        VPC, 'service_name = "com.amazonaws.us-east-2.ecr.api"', 'vpc_endpoint_type = "Interface"',
    ]),
    "aws_vpc_security_group_ingress_rule": ("ingress", [GROUP, *ANYWHERE]),
    "aws_security_group_rule": ("ingress", [GROUP, 'type = "ingress"', *ALL_PORTS, "self = true"]),
    "aws_security_group": ("ingress", ["ingress {\n    " + "\n    ".join(ALL_PORTS) + "\n  }"]),
    "aws_vpc_security_group_egress_rule": (None, [GROUP, *ANYWHERE]),
}
SYNTHETIC_ROOT = (
    'module "network" {\n  source = "./modules/network"\n  name_prefix = local.name_prefix\n'
    f'  vpc_cidr = "{NETWORK["vpc_cidr"]}"\n'
    f"  public_subnets = {json.dumps(PUBLIC_SUBNETS)}\n}}\n"
) + "".join(
    f'resource "{kind}" "proibido" {{\n  ' + "\n  ".join(body) + "\n}\n"
    for kind, (_, body) in SYNTHETIC.items()
)
MOCK_PROVIDERS = 'mock_provider "aws" {}\n\nmock_provider "aws" {\n  alias = "global_edge"\n}\n'
SYNTHETIC_SUITE = MOCK_PROVIDERS + '\nrun "raiz_sintetica" {\n  command = plan\n}\n'
MOCK_PROVIDER = re.compile(r'^mock_provider "aws" \{(?:\n  alias = "(\w+)"\n)?\}$', re.MULTILINE)
OVERRIDE = "\n  override_{kind} {{\n    target = {target}\n    values = {values}\n  }}\n"


class TofuReport(NamedTuple):
    statuses: dict[str, str]
    plans: dict[str, Node]
    diagnostics: list[str]
    result: subprocess.CompletedProcess[str]


def _run(name: str, variables: Mapping[str, object], body: str, source: str | None) -> str:
    module = f'\n  module {{\n    source = "{source}"\n  }}\n' if source else "\n"
    assigned = "".join(f"\n    {key} = {json.dumps(value)}" for key, value in variables.items())
    header = f'\nrun "{name}" {{\n  command = plan\n{module}'
    return f"{header}  variables {{{assigned}\n  }}\n{body}}}\n"


def _overrides(prefix: str = "") -> str:
    return "".join(
        OVERRIDE.format(kind=kind, target=f"{prefix}{target}", values=json.dumps(values))
        for (kind, target), values in OVERRIDES.items()
    )


def _suites(shared_outputs: Mapping[str, object]) -> dict[str, str]:
    modules = "".join(
        _run(name, variables, _overrides(), MODULE_SOURCE)
        for name, variables in MODULE_RUNS.items()
    )
    root = _run(ROOT_RUN, {"shared_outputs": shared_outputs}, _overrides("module.network."), None)
    rejections = "".join(
        _run(name, NETWORK | bad, f"\n  expect_failures = [var.{next(iter(bad))}]\n", MODULE_SOURCE)
        for name, bad in INVALID_RUNS.items()
    )
    # A missing expected failure errors its whole file and skips every later run in it.
    return {
        "network": MOCK_PROVIDERS + modules + root,
        "network-validation": MOCK_PROVIDERS + rejections,
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
    report = TofuReport({}, {}, [], result)
    for line in result.stdout.splitlines():
        message = cast("dict[str, Any]", json.loads(line))
        kind = message.get("type")
        if kind == "test_run":
            report.statuses[message["test_run"]["run"]] = message["test_run"]["status"]
        elif kind == "test_plan":
            report.plans[message["@testrun"]] = message["test_plan"]
        elif kind == "diagnostic":
            diagnostic = message["diagnostic"]
            report.diagnostics.append(f"{diagnostic['summary']}: {diagnostic.get('detail')}")
    return report


def _assert_runs(report: TofuReport, expected: Mapping[str, str]) -> None:
    actual = {name: report.statuses.get(name) for name in expected}
    assert actual == dict(expected), (
        f"tofu_test runs={actual} diagnostics={report.diagnostics!r} "
        f"stderr={report.result.stderr!r}"
    )


def _config_resources(module: Node, prefix: str = "") -> Iterator[Resource]:
    for resource in cast("list[Node]", module.get("resources", [])):
        if resource["mode"] == "managed":
            expressions = cast("dict[str, Any]", resource.get("expressions", {}))
            values = {
                key: value.get("constant_value", value) if isinstance(value, dict) else value
                for key, value in expressions.items()
            }
            yield f"{prefix}{resource['address']}", cast("str", resource["type"]), values
    for name, call in cast("dict[str, Node]", module.get("module_calls", {})).items():
        yield from _config_resources(cast("Node", call["module"]), f"{prefix}module.{name}.")


def _plan_resources(plan: Node) -> list[Resource]:
    return [
        (change["address"], change["type"], change["change"]["after"] or {})
        for change in cast("list[dict[str, Any]]", plan.get("resource_changes", []))
        if change["mode"] == "managed"
    ]


def _of_type(resources: Iterable[Resource], kind: str) -> list[Mapping[str, object]]:
    return [values for _, resource_kind, values in resources if resource_kind == kind]


def _outputs(plan: Node) -> dict[str, object]:
    outputs = cast("dict[str, Node]", cast("Node", plan["planned_values"])["outputs"])
    return {name: output["value"] for name, output in outputs.items()}


def _category(kind: str, values: Mapping[str, object]) -> str | None:
    prefixed = (category for prefix, category in FORBIDDEN_PREFIXES if kind.startswith(prefix))
    endpoint = values.get("vpc_endpoint_type", "Gateway")
    ingress = (kind == "aws_security_group_rule" and values.get("type") != "egress") or (
        kind in SECURITY_GROUPS and bool(values.get("ingress"))
    )
    if kind == "aws_vpc_endpoint" and endpoint != "Gateway":
        return "interface_endpoint"
    return "ingress" if ingress else next(prefixed, None)


def _findings(resources: Iterable[Resource]) -> set[tuple[str, str]]:
    return {
        (category, address)
        for address, kind, values in resources
        if (category := _category(kind, values)) is not None
    }


def _assert_absent(graphs: Graphs, categories: Sequence[str]) -> None:
    found = {finding for graph in graphs for finding in _findings(graph)}
    selected = sorted(finding for finding in found if finding[0] in categories)
    assert not selected, f"network_forbidden findings={selected}"


def _egress(resources: Iterable[Resource]) -> Counter[tuple[object, ...]]:
    rules: Counter[tuple[object, ...]] = Counter()
    for address, kind, values in resources:
        inline = kind in SECURITY_GROUPS and bool(values.get("egress"))
        if inline or (kind == "aws_security_group_rule" and values.get("type") == "egress"):
            rules[("egress_fora_de_aws_vpc_security_group_egress_rule", address)] += 1
        elif kind == "aws_vpc_security_group_egress_rule":
            target = "prefix_list" if values.get("prefix_list_id") else (
                values.get("cidr_ipv4") or values.get("cidr_ipv6")
                or values.get("referenced_security_group_id")
            )
            ports = (values.get("from_port"), values.get("to_port"))
            rules[(values.get("ip_protocol"), *ports, target)] += 1
    return rules


def _assert_egress(resources: Sequence[Resource], gateways: int) -> None:
    expected = Counter([*DNS_EGRESS, PUBLIC_TLS_EGRESS, *[GATEWAY_TLS_EGRESS] * gateways])
    endpoints = {values.get("prefix_list_id") for values in _of_type(resources, "aws_vpc_endpoint")}
    rules = _of_type(resources, "aws_vpc_security_group_egress_rule")
    used = {values["prefix_list_id"] for values in rules if values.get("prefix_list_id")}
    assert _egress(resources) == expected, f"network_egress_mismatch actual={_egress(resources)}"
    assert used <= endpoints, f"network_egress_foreign_prefix_list={used - endpoints}"


def _validate_asl(subnets: Sequence[str], policy: ProductionNetworkPolicy) -> None:
    definition = json.loads((ASL_FIXTURES / "production_public_ip.json").read_text("utf-8"))
    processor = definition["States"]["RunUnits"]["ItemProcessor"]["States"]["RunProcessor"]
    processor["Parameters"]["NetworkConfiguration"]["AwsvpcConfiguration"] = {
        "AssignPublicIp": "ENABLED", "Subnets": list(subnets),
        "SecurityGroups": [policy.security_group_id],
    }
    arn = PRODUCTION_VALUES["AWS_STATE_MACHINE_ARN"]
    client = boto3.client(
        "stepfunctions", region_name="us-east-2", config=Config(signature_version=UNSIGNED),
    )
    described = {
        "stateMachineArn": arn, "name": "cnesdata-prod", "definition": json.dumps(definition),
        "roleArn": "arn:aws:iam::000000000000:role/cnes-states", "type": "STANDARD",
        "creationDate": datetime(2026, 10, 12, tzinfo=UTC),
    }
    with Stubber(client) as stubber:
        stubber.add_response("describe_state_machine", described, {"stateMachineArn": arn})
        validate_production_state_machine(client, arn, policy)
        stubber.assert_no_pending_responses()


def _write_synthetic_root(root: Path, prod_root: Path) -> None:
    hidden = shutil.ignore_patterns(".*")
    shutil.copytree(MODULE_DIRECTORY, root / "modules" / "network", ignore=hidden)
    for name in ("versions.tf", "providers.tf", ".terraform.lock.hcl"):
        shutil.copy2(prod_root / name, root / name)
    (root / "main.tf").write_text(SYNTHETIC_ROOT, encoding="utf-8")
    (root / "tests").mkdir()
    (root / "tests" / "synthetic.tftest.hcl").write_text(SYNTHETIC_SUITE, encoding="utf-8")


@pytest.fixture(scope="module")
def plan_report(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    valid_shared_outputs: dict[str, object],
) -> Iterator[TofuReport]:
    test_directory = prod_root / ".terraform" / "network-tests"
    test_directory.mkdir(parents=True)
    selector = "-test-directory=.terraform/network-tests"
    try:
        for name, suite in _suites(valid_shared_outputs).items():
            (test_directory / f"{name}.tftest.hcl").write_text(suite, encoding="utf-8")
        init = _tofu(tofu_context, prod_root, ("init", "-backend=false", "-input=false", selector))
        _require_success(init, "init")
        test = ("test", "-no-color", "-json", "-verbose", selector)
        yield _report(_tofu(tofu_context, prod_root, test))
    finally:
        shutil.rmtree(test_directory)


@pytest.fixture(scope="module")
def graphs(plan_report: TofuReport, tofu_artifacts: Artifacts) -> Graphs:
    _assert_runs(plan_report, {ROOT_RUN: "pass"})
    root = cast("Node", tofu_artifacts["configuration"]["root_module"])
    return list(_config_resources(root)), _plan_resources(plan_report.plans[ROOT_RUN])


@pytest.fixture(scope="module")
def synthetic_graphs(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> Graphs:
    root = tmp_path_factory.mktemp("network-synthetic")
    _write_synthetic_root(root, prod_root)
    providers = Path(tofu_context[1]["TF_DATA_DIR"]) / "providers"
    tofu = (tofu_context[0], {**tofu_context[1], "TF_DATA_DIR": str(root / ".tofu-data")})
    init = ("init", "-backend=false", "-input=false", f"-plugin-dir={providers}")
    _require_success(_tofu(tofu, root, init), "init-synthetic")
    report = _report(_tofu(tofu, root, ("test", "-no-color", "-json", "-verbose")))
    _assert_runs(report, {"raiz_sintetica": "pass"})
    shown = _tofu(tofu, root, ("show", "-json", "-config"))
    _require_success(shown, "show-synthetic")
    configuration = cast("Node", json.loads(shown.stdout)["root_module"])
    return list(_config_resources(configuration)), _plan_resources(report.plans["raiz_sintetica"])


def test_raiz_agenda_network_uma_vez(
    plan_report: TofuReport, tofu_artifacts: Artifacts, prod_root: Path
) -> None:
    root = cast("Node", tofu_artifacts["configuration"]["root_module"])
    calls = cast("dict[str, Node]", root.get("module_calls", {}))
    module = (prod_root / MODULE_SOURCE).resolve()
    targets = {n: (prod_root / cast("str", c["source"])).resolve() for n, c in calls.items()}

    _assert_runs(plan_report, {ROOT_RUN: "pass"})
    assert [n for n, target in targets.items() if target == module] == ["network"]
    assert calls["network"]["expressions"] == ROOT_INPUTS


def test_rede_sem_nat_lb_ou_listener(graphs: Graphs, plan_report: TofuReport) -> None:
    resources = _plan_resources(plan_report.plans[WITH_ENDPOINTS])
    routes = [
        (route["destination_cidr_block"], route["gateway_id"], route.get("nat_gateway_id"))
        for route in _of_type(resources, "aws_route")
    ]
    associations = [
        (association["subnet_id"], association["route_table_id"])
        for association in _of_type(resources, "aws_route_table_association")
    ]

    _assert_absent(graphs, NAT_LB_LISTENER)
    assert routes == [("0.0.0.0/0", INTERNET_GATEWAY_ID, None)]
    assert associations == [(SUBNET_ID, ROUTE_TABLE_ID)] * len(PUBLIC_SUBNETS)


def test_security_group_sem_ingress(graphs: Graphs, plan_report: TofuReport) -> None:
    resources = _plan_resources(plan_report.plans[WITH_ENDPOINTS])
    groups = {
        address: (values.get("ingress"), values.get("egress"))
        for address, kind, values in resources
        if kind in SECURITY_GROUPS
    }

    _assert_absent(graphs, ("ingress",))
    assert groups == dict.fromkeys(
        ("aws_default_security_group.this", "aws_security_group.tasks"), ([], [])
    )


def test_egress_so_dns_e_tls(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, dict.fromkeys((*MODULE_RUNS, ROOT_RUN), "pass"))
    root = _plan_resources(plan_report.plans[ROOT_RUN])

    _assert_egress(_plan_resources(plan_report.plans[WITH_ENDPOINTS]), gateways=2)
    _assert_egress(_plan_resources(plan_report.plans[WITHOUT_ENDPOINTS]), gateways=0)
    _assert_egress([r for r in root if r[0].startswith("module.network.")], gateways=2)


def test_sem_interface_endpoint(graphs: Graphs, plan_report: TofuReport) -> None:
    enabled, disabled = (plan_report.plans[name] for name in MODULE_RUNS)
    endpoints = {
        values["service_name"]: (values["vpc_endpoint_type"], values["route_table_ids"])
        for values in _of_type(_plan_resources(enabled), "aws_vpc_endpoint")
    }

    _assert_absent(graphs, ("interface_endpoint",))
    assert endpoints == {
        f"com.amazonaws.us-east-2.{service}": ("Gateway", [ROUTE_TABLE_ID])
        for service in GATEWAY_ENDPOINT_IDS
    }
    assert _outputs(enabled)["gateway_endpoint_ids"] == GATEWAY_ENDPOINT_IDS
    assert _of_type(_plan_resources(disabled), "aws_vpc_endpoint") == []
    assert _outputs(disabled)["gateway_endpoint_ids"] == dict.fromkeys(GATEWAY_ENDPOINT_IDS)


def test_tupla_de_rede_passa_no_validador_de_producao(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, {WITH_ENDPOINTS: "pass", ROOT_RUN: "pass"})
    plan = plan_report.plans[WITH_ENDPOINTS]
    outputs = _outputs(plan)
    subnets = cast("list[str]", outputs["public_subnet_ids"])
    security_group = cast("str", outputs["security_group_id"])
    settings = AwsRuntimeSettings.from_mapping(
        PRODUCTION_VALUES
        | {"AWS_PUBLIC_SUBNET_IDS": ",".join(subnets), "AWS_SECURITY_GROUP_ID": security_group}
    )
    policy = ProductionNetworkPolicy(
        settings.processor_container_name, settings.processor_lease_seconds,
        settings.public_subnet_ids, security_group,
    )
    subnet_values = _of_type(_plan_resources(plan), "aws_subnet")

    _validate_asl(subnets, policy)
    assert settings.public_subnet_ids == (SUBNET_ID,)
    assert settings.security_group_id == SECURITY_GROUP_ID
    assert [s["map_public_ip_on_launch"] for s in subnet_values] == [False] * len(PUBLIC_SUBNETS)


def test_variaveis_de_rede_rejeitam_entrada_invalida(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, dict.fromkeys(INVALID_RUNS, "pass"))


def test_raiz_sintetica_com_proibidos_aparece_nos_dois_grafos(synthetic_graphs: Graphs) -> None:
    expected = {
        (category, f"{kind}.proibido") for kind, (category, _) in SYNTHETIC.items() if category
    }

    assert [_findings(graph) for graph in synthetic_graphs] == [expected, expected]


@pytest.mark.parametrize(
    "guard",
    [
        lambda graphs: _assert_absent(graphs, NAT_LB_LISTENER),
        lambda graphs: _assert_absent(graphs, ("ingress",)),
        lambda graphs: _assert_absent(graphs, ("interface_endpoint",)),
        lambda graphs: _assert_egress(graphs[1], gateways=2),
    ],
    ids=["nat_lb_listener_ou_service", "ingress", "interface_endpoint", "egress"],
)
def test_guardrail_de_rede_reprova_raiz_sintetica(
    synthetic_graphs: Graphs, guard: Callable[[Graphs], None]
) -> None:
    with pytest.raises(AssertionError, match=r"^network_(forbidden|egress_mismatch) "):
        guard(synthetic_graphs)


def test_suites_gerados_mockam_todo_provider_aws_da_raiz(
    tofu_artifacts: Artifacts, valid_shared_outputs: dict[str, object]
) -> None:
    providers = cast("dict[str, Node]", tofu_artifacts["configuration"]["provider_config"])
    roots = {
        key for key, provider in providers.items()
        if provider["name"] == "aws" and "module_address" not in provider
    }
    suites = [*_suites(valid_shared_outputs).values(), SYNTHETIC_SUITE]
    mocked = [
        {f"aws.{alias}" if alias else "aws" for alias in MOCK_PROVIDER.findall(suite)}
        for suite in suites
    ]

    assert {"aws", "aws.global_edge"} <= roots
    assert mocked == [roots] * len(suites)
