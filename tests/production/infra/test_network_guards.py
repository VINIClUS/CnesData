import re
from typing import cast

import pytest

from tests.production.infra import test_network as network
from tests.production.infra.test_network import (
    MODULE_DIRECTORY,
    NAT_LB_LISTENER,
    ROOT_RUN,
    SECURITY_GROUPS,
    SYNTHETIC_SUITE,
    Artifacts,
    Node,
    TofuReport,
    _assert_egress,
    _assert_runs,
    _category,
    _config_resources,
    _plan_resources,
    _suites,
)

MOCK_PROVIDER = re.compile(r'^mock_provider "aws" \{(?:\n  alias = "(\w+)"\n)?\}$', re.MULTILINE)
SORTED_UNIQUE_SUBNETS = "sort(distinct([for subnet in aws_subnet.public : subnet.id]))"
SUBNET_OUTPUT = re.compile(
    r'^output "public_subnet_ids" \{\n(?:  .*\n)*?  value\s*=\s*(?P<value>.+)$', re.MULTILINE
)

# Binding the fixture registers it in this module; an import of it would read as unused.
plan_report = network.plan_report


def test_egress_do_sg_das_tasks_na_raiz_inteira(plan_report: TofuReport) -> None:
    _assert_runs(plan_report, {ROOT_RUN: "pass"})

    _assert_egress(_plan_resources(plan_report.plans[ROOT_RUN]), gateways=2)


def test_grupos_declaram_listas_vazias_para_remover_regra_externa(
    tofu_artifacts: Artifacts,
) -> None:
    root = cast("Node", tofu_artifacts["configuration"]["root_module"])
    groups = {
        address: (values.get("ingress"), values.get("egress"))
        for address, kind, values in _config_resources(root)
        if kind in SECURITY_GROUPS
    }

    assert groups == {
        "module.network.aws_default_security_group.this": ([], []),
        "module.network.aws_security_group.tasks": ([], None),
    }


@pytest.mark.parametrize(
    ("kind", "category"),
    [
        ("aws_ecs_express_gateway_service", "ecs_service"),
        ("aws_vpclattice_service", "vpc_lattice"),
        ("aws_vpclattice_listener", "vpc_lattice"),
        ("aws_vpclattice_target_group", "vpc_lattice"),
        ("aws_globalaccelerator_accelerator", "global_accelerator"),
        ("aws_globalaccelerator_listener", "global_accelerator"),
        ("aws_lightsail_lb", "load_balancer"),
    ],
)
def test_denylist_reprova_listener_e_service_fora_do_elb(kind: str, category: str) -> None:
    assert _category(kind, {}) == category
    assert category in NAT_LB_LISTENER


def test_output_de_subnets_sai_ordenado_e_sem_repeticao(tofu_artifacts: Artifacts) -> None:
    # Overrides take no instance key and mock IDs repeat per resource type, so no mocked plan holds
    # two distinct subnet IDs out of order: the output expression itself is pinned.
    root = cast("Node", tofu_artifacts["configuration"]["root_module"])
    calls = cast("dict[str, Node]", root["module_calls"])
    outputs = cast("dict[str, Node]", cast("Node", calls["network"]["module"])["outputs"])
    source = (MODULE_DIRECTORY / "outputs.tf").read_text(encoding="utf-8")

    assert outputs["public_subnet_ids"]["expression"] == {"references": ["aws_subnet.public"]}
    assert [match["value"] for match in SUBNET_OUTPUT.finditer(source)] == [SORTED_UNIQUE_SUBNETS]


def test_arquivos_do_modulo_de_rede_cabem_em_100_colunas() -> None:
    wide = [
        f"{path.name}:{number}"
        for path in sorted(MODULE_DIRECTORY.glob("*.tf"))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if len(line) > 100
    ]

    assert not wide, f"network_line_too_long lines={wide}"


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
