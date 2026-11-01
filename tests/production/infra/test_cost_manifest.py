import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from tests.production.infra._athena_contracts import (
    CROSSING_QUERIES,
    MANIFEST_PATH,
    MONTH_LIMIT_BYTES,
    OVERSHOOT_QUERIES,
    PER_QUERY_CUTOFF_BYTES,
    REPO_ROOT,
    SCHEMA_PATH,
    WINDOW_BYTES,
    WINDOWS_PER_MONTH,
    alarm_metric_count,
    alarms,
    of_type,
)
from tests.production.infra._athena_root_plan import (  # noqa: F401
    Node,
    fixture_scheduled_plan,
)
from tests.production.infra._cost_manifest_model import (
    SCENARIOS,
    totals,
    unsupported_keywords,
    validate,
    violations,
)
from tests.production.infra._processing_native import A_RUN, NativePlans
from tests.production.infra._step_functions_iam_contracts import output_value

AS_OF = "2026-11-01"
AMENDMENT = (
    REPO_ROOT / "docs/superpowers/specs/2026-11-01-athena-observability-cost-amendment.md"
)
PRODUCT_IDS = frozenset({
    "fargate_vcpu_hours", "fargate_memory_gb_hours", "public_ipv4_hours",
    "cloudwatch_alarm_metrics", "cloudwatch_custom_metrics", "cloudwatch_logs_ingestion",
    "cloudwatch_logs_storage", "lambda_requests", "lambda_compute", "sns_requests",
    "ecr_storage", "step_functions_transitions", "dynamodb_read_units", "dynamodb_write_units",
    "dynamodb_pitr_storage", "dynamodb_storage", "s3_web_storage", "s3_data_storage",
    "s3_audit_storage", "s3_athena_results_storage", "s3_requests", "athena_scanned",
    "cognito_essentials_mau", "cognito_allowance", "cloudfront_free_plan", "waf_dedicated",
    "data_transfer_out", "secrets_manager",
})
CAP_IDS = frozenset({
    "fargate_vcpu_hours", "fargate_memory_gb_hours", "public_ipv4_hours",
    "cloudwatch_alarm_metrics", "cloudwatch_custom_metrics", "athena_scanned",
    "cognito_essentials_mau", "ecr_storage", "s3_web_storage", "secrets_manager",
    "cloudfront_free_plan", "waf_dedicated", "kms_envelope",
})
SHARED_IDS = frozenset({"kms_envelope", "state_backend", "budget_actions", "other_projects_spend"})
UNVERIFIED_IDS = frozenset({"cognito_allowance", "other_projects_spend"})
STORED_TOTAL_KEY = re.compile(r"(^|_)(sub)?totals?(_|$)|(^|_)sums?(_|$)")
SOURCE_PREFIXES = (
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/",
    "https://docs.aws.amazon.com/",
    "https://aws.amazon.com/",
)


def _manifest() -> Node:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _lines(manifest: Node) -> dict[str, Node]:
    return {line["id"]: line for line in manifest["lines"]}


def _synthetic(product: str, shared: str = "0") -> Node:
    zero = {"min": 0, "base": 0, "max": 0}

    def line(identifier: str, scope: str, price: str) -> Node:
        quantity = {"min": 1, "base": 1, "max": 1}
        return {
            "id": identifier, "scope": scope, "unit_price_usd": float(price),
            "quantity": quantity, "free_allowance": zero,
        }

    return {"lines": [line("p", "product", product), line("s", "shared", shared)]}


def test_manifest_tem_o_conjunto_exato_de_linhas() -> None:
    lines = _lines(_manifest())
    assert set(lines) == PRODUCT_IDS | SHARED_IDS
    assert {name for name, item in lines.items() if item["scope"] == "shared"} == SHARED_IDS
    assert {name for name, item in lines.items() if item["status"] == "unverified"} == (
        UNVERIFIED_IDS
    )
    assert len(_manifest()["lines"]) == len(lines), "duplicate_line_id"


def test_cada_linha_declara_a_base_da_quantidade_maxima() -> None:
    lines = _lines(_manifest())
    caps = {name for name, item in lines.items() if item["max_basis"] == "cap"}
    assert caps == CAP_IDS
    assert {item["max_basis"] for item in lines.values()} == {"cap", "hypothesis"}
    amendment = AMENDMENT.read_text(encoding="utf-8")
    for name in set(lines) - CAP_IDS:
        assert f"`{name}`" in amendment, f"hypothesis_not_documented line={name}"


def test_manifest_nao_armazena_totais_e_cita_fonte_datada() -> None:
    manifest = _manifest()
    assert manifest["as_of"] == AS_OF
    assert manifest["region"] == "us-east-2"
    assert set(manifest) == {"schema_version", "as_of", "region", "currency", "limits", "lines"}
    for line in manifest["lines"]:
        stored = {key for key in line if STORED_TOTAL_KEY.search(key)}
        assert not stored, f"stored_total line={line['id']} keys={sorted(stored)}"
        assert line["source"]["url"].startswith(SOURCE_PREFIXES), line["id"]
        assert line["source"]["term"], line["id"]


def test_linhas_nao_verificaveis_nao_inventam_numero() -> None:
    for name, line in _lines(_manifest()).items():
        if line["status"] != "unverified":
            continue
        assert line["unit_price_usd"] is None, name
        assert set(line["quantity"].values()) == {None}, name
        assert line["assumption"], name


def test_cenario_maximo_nao_aplica_franquia_de_free_tier() -> None:
    for name, line in _lines(_manifest()).items():
        assert line["free_allowance"]["max"] in (0, None), f"line={name}"


def test_schema_do_manifesto_e_fechado_e_usa_so_palavras_suportadas() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert unsupported_keywords(schema) == []
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["additionalProperties"] is False
    assert schema["properties"]["limits"]["properties"]["product_usd"]["const"] == 8
    assert schema["properties"]["limits"]["properties"]["aggregate_usd"]["const"] == 15
    broken = _manifest()
    broken["lines"][0]["extra"] = 1
    broken["limits"]["product_usd"] = 9
    errors = validate(broken, schema)
    assert any("additional" in item for item in errors)
    assert any("const=8" in item for item in errors)


@pytest.mark.parametrize(
    ("product", "shared", "expected"),
    [
        ("8.00", "0", []),
        ("8.01", "0", [f"cost_over_limit scope=product scenario={s} total_usd=8.01 "
                       "limit_usd=8.00" for s in SCENARIOS]),
        ("7.99", "7.01", []),
        ("7.99", "7.02", [f"cost_over_limit scope=aggregate scenario={s} total_usd=15.01 "
                          "limit_usd=15.00" for s in SCENARIOS]),
    ],
    ids=["produto_8_00", "produto_8_01", "agregado_15_00", "agregado_15_01"],
)
def test_fronteira_dos_limites_8_e_15(product: str, shared: str, expected: list[str]) -> None:
    assert violations(_synthetic(product, shared)) == expected
    assert totals(_synthetic(product, shared))["product"]["max"] == Decimal(product)


def test_quantidades_de_fargate_seguem_o_plano(
    scheduled_plan: Node, processing_native: NativePlans
) -> None:
    budget = output_value(scheduled_plan, "scheduler_task_hour_budget")
    limit, scheduled = budget["monthly_limit_hours"], budget["scheduled_hours"]
    reserve, billed = budget["unit_reserve_hours"], budget["billed_minutes_per_run"]
    tasks = of_type(processing_native.plans[A_RUN], "aws_ecs_task_definition")
    sizes = {(item["change"]["after"]["cpu"], item["change"]["after"]["memory"]) for item in tasks}
    assert len(sizes) == 1
    ((cpu, memory),) = sizes
    vcpu, gib = int(cpu) / 1024, int(memory) / 1024
    hours = {"min": scheduled / billed, "base": scheduled + reserve / 2, "max": limit}
    lines = _lines(_manifest())
    for scenario in SCENARIOS:
        assert lines["public_ipv4_hours"]["quantity"][scenario] == hours[scenario]
        assert lines["fargate_vcpu_hours"]["quantity"][scenario] == hours[scenario] * vcpu
        assert lines["fargate_memory_gb_hours"]["quantity"][scenario] == hours[scenario] * gib


def test_contagem_de_metricas_de_alarme_e_filtros_vem_do_plano(scheduled_plan: Node) -> None:
    lines = _lines(_manifest())
    counted = sum(alarm_metric_count(item) for item in alarms(scheduled_plan).values())
    filters = len(of_type(scheduled_plan, "aws_cloudwatch_log_metric_filter"))
    assert set(lines["cloudwatch_alarm_metrics"]["quantity"].values()) == {counted}
    assert set(lines["cloudwatch_custom_metrics"]["quantity"].values()) == {filters}


def test_athena_no_manifesto_chega_ao_teto_do_cutoff() -> None:
    in_flight = OVERSHOOT_QUERIES + CROSSING_QUERIES
    ceiling = WINDOWS_PER_MONTH * WINDOW_BYTES + in_flight * PER_QUERY_CUTOFF_BYTES
    assert ceiling <= MONTH_LIMIT_BYTES
    line = _lines(_manifest())["athena_scanned"]
    assert line["unit_price_usd"] == 5
    assert line["quantity"]["max"] == ceiling / 1e12
    assert line["quantity"]["min"] <= line["quantity"]["base"] <= line["quantity"]["max"]


def test_limites_fixos_do_produto_cognito_e_waf() -> None:
    lines = _lines(_manifest())
    cognito = lines["cognito_essentials_mau"]
    assert cognito["unit_price_usd"] == 0.015
    assert cognito["quantity"] == {"min": 1, "base": 10, "max": 100}
    waf = lines["waf_dedicated"]
    assert waf["unit_price_usd"] == 0
    assert waf["exposure_usd"] == 7
    assert waf["precondition"] == "cloudfront_free_plan_active"
    assert waf["blocking"] == "activation"
    assert lines["secrets_manager"]["binding"] == "BILLING_MODE=disabled"
    assert lines["step_functions_transitions"]["binding"] == "monthly_attempt_limit=200"
    assert lines["ecr_storage"]["quantity"]["max"] == 2
    assert lines["s3_web_storage"]["quantity"]["max"] == 5
    assert "365" in lines["s3_audit_storage"]["description"]


def test_toda_precondicao_declara_bloqueio_e_exposicao_acima_do_limite_bloqueia_ativacao() -> None:
    manifest = _manifest()
    product_max = totals(manifest)["product"]["max"]
    for name, line in _lines(manifest).items():
        if "precondition" not in line:
            continue
        blocking = line.get("blocking")
        assert blocking in {"activation", "advisory"}, f"blocking_missing line={name}"
        exposure = Decimal(str(line.get("exposure_usd", 0)))
        if product_max + exposure > Decimal(manifest["limits"]["product_usd"]):
            assert line["blocking"] == "activation", f"activation_block_required line={name}"


def test_manifest_fica_fora_da_raiz_aplicada() -> None:
    prod = Path(__file__).resolve().parents[3] / "infra" / "opentofu" / "env" / "prod"
    assert prod not in MANIFEST_PATH.parents
