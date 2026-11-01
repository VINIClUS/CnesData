import json
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from tests.production.infra._athena_contracts import FIXTURES, MANIFEST_PATH, POLICIES, REPO_ROOT
from tests.production.infra._athena_root_plan import (  # noqa: F401
    Node,
    fixture_scheduled_plan,
)
from tests.production.infra.conftest import require_tool

type Report = list[Node]
type Conftest = Callable[..., tuple[int, Report]]

PRODUCT_COST = (
    "cost_over_limit scope=product scenario={} total_usd=8.01 limit_usd=8.00"
)
AGGREGATE_COST = (
    "cost_over_limit scope=aggregate scenario={} total_usd=15.01 limit_usd=15.00"
)
REJECTIONS = {
    "nat_gateway.json": [
        "forbidden_resource reason=nat_gateway type=aws_nat_gateway address=aws_nat_gateway.main"
    ],
    "load_balancer.json": [
        "forbidden_resource reason=load_balancer type=aws_lb address=aws_lb.main"
    ],
    "rds.json": [
        "forbidden_resource reason=database type=aws_db_instance address=aws_db_instance.main"
    ],
    "container_insights.json": [
        "forbidden_resource reason=container_insights type=aws_ecs_cluster "
        "address=aws_ecs_cluster.processor"
    ],
    "regiao_fora_de_us_east_2.json": [
        "forbidden_region region=us-west-2 type=aws_s3_bucket address=aws_s3_bucket.west"
    ],
    "cognito_plus.json": [
        "forbidden_resource reason=paid_plan type=aws_cognito_user_pool "
        "address=aws_cognito_user_pool.main"
    ],
    "tabela_global.json": [
        "forbidden_resource reason=global_table_replica type=aws_dynamodb_table "
        "address=aws_dynamodb_table.main"
    ],
    "custo_produto_acima_de_8.json": [PRODUCT_COST.format(s) for s in ("min", "base", "max")],
    "custo_agregado_acima_de_15.json": [AGGREGATE_COST.format(s) for s in ("min", "base", "max")],
}

FORBIDDEN = {
    "redshift.json": ("data_warehouse", "aws_redshift_cluster", "aws_redshift_cluster.main"),
    "opensearch.json": ("search", "aws_opensearch_domain", "aws_opensearch_domain.main"),
    "elasticache.json": ("cache", "aws_elasticache_cluster", "aws_elasticache_cluster.main"),
    "replicacao_s3.json": (
        "s3_replication",
        "aws_s3_bucket_replication_configuration",
        "aws_s3_bucket_replication_configuration.main",
    ),
    "alb.json": ("load_balancer", "aws_alb", "aws_alb.main"),
    "elb_classico.json": ("load_balancer", "aws_elb", "aws_elb.main"),
    "aurora.json": ("database", "aws_rds_cluster", "aws_rds_cluster.main"),
    "shield_advanced.json": (
        "paid_plan", "aws_shield_subscription", "aws_shield_subscription.main",
    ),
    "athena_capacity_reservation.json": (
        "paid_plan",
        "aws_athena_capacity_reservation",
        "aws_athena_capacity_reservation.main",
    ),
    "lambda_provisioned_concurrency.json": (
        "paid_plan",
        "aws_lambda_provisioned_concurrency_config",
        "aws_lambda_provisioned_concurrency_config.main",
    ),
}
REJECTIONS |= {
    name: [f"forbidden_resource reason={reason} type={kind} address={address}"]
    for name, (reason, kind, address) in FORBIDDEN.items()
}
REJECTIONS["precondicao_sem_bloqueio.json"] = ["precondition_without_blocking line=gated"]
REJECTIONS["exposicao_sem_bloqueio_de_ativacao.json"] = [
    "exposure_requires_activation_block line=gated"
]
REJECTIONS["regiao_em_modulo.json"] = [
    "forbidden_region region=us-west-2 type=aws_s3_bucket address=module.child.aws_s3_bucket.west"
]
REJECTIONS["regiao_sem_constante.json"] = [
    "forbidden_region region=unset type=aws_s3_bucket address=aws_s3_bucket.dynamic"
]


@pytest.fixture(name="conftest", scope="module")
def fixture_conftest() -> Conftest:
    executable = require_tool("conftest")

    def run(*targets: str | Path, extra: tuple[str, ...] = ()) -> tuple[int, Report]:
        assert (POLICIES / "cnesdata.rego").is_file(), "policy_missing"
        command = [
            executable, "test", *map(str, targets), "--policy", str(POLICIES), "--output", "json",
            "--no-color", *extra,
        ]
        done = subprocess.run(  # noqa: S603
            command, check=False, capture_output=True, cwd=REPO_ROOT, text=True
        )
        assert done.returncode in {0, 1}, f"conftest_error stderr={done.stderr!r}"
        return done.returncode, json.loads(done.stdout)

    return run


def _messages(report: Report) -> list[str]:
    return sorted(item["msg"] for entry in report for item in entry.get("failures") or [])


def test_politica_usa_package_main_para_o_comando_da_issue() -> None:
    assert (POLICIES / "cnesdata.rego").is_file(), "policy_missing"
    text = (POLICIES / "cnesdata.rego").read_text(encoding="utf-8")
    assert text.startswith("package main\n") or "\npackage main\n" in text
    assert "import rego.v1" not in text


@pytest.mark.parametrize("name", sorted(path.name for path in (FIXTURES / "accepted").glob("*")))
def test_fixture_aceita_passa_na_politica(conftest: Conftest, name: str) -> None:
    code, report = conftest(FIXTURES / "accepted" / name)
    assert (code, _messages(report)) == (0, [])
    assert sum(entry["successes"] for entry in report) >= 1


def test_comando_da_issue_ignorando_rejeitadas_sai_zero(conftest: Conftest) -> None:
    code, report = conftest(FIXTURES, extra=("--ignore", "rejected"))
    assert code == 0
    assert _messages(report) == []
    assert len(report) == len(list((FIXTURES / "accepted").glob("*.json")))


@pytest.mark.parametrize("name", sorted(REJECTIONS))
def test_fixture_rejeitada_falha_com_mensagem_exata(conftest: Conftest, name: str) -> None:
    code, report = conftest(FIXTURES / "rejected" / name)
    assert code == 1
    assert _messages(report) == sorted(REJECTIONS[name])


def test_todas_as_fixtures_rejeitadas_estao_cobertas() -> None:
    names = {path.name for path in (FIXTURES / "rejected").glob("*.json")}
    assert names == set(REJECTIONS)


def test_plan_mockado_vivo_da_raiz_nao_tem_falhas(
    conftest: Conftest, scheduled_plan: Node, tmp_path: Path
) -> None:
    target = tmp_path / "plano-vivo.json"
    target.write_text(json.dumps(scheduled_plan), encoding="utf-8")
    code, report = conftest(target)
    assert (code, _messages(report)) == (0, [])
    assert sum(entry["successes"] for entry in report) >= 1


def test_manifest_real_passa_na_politica(conftest: Conftest) -> None:
    code, report = conftest(MANIFEST_PATH)
    assert (code, _messages(report)) == (0, [])
    assert sum(entry["successes"] for entry in report) >= 1
