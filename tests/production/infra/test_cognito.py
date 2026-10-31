import json
import shutil
import subprocess
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple, cast

import pytest

from cnes_infra.aws import AwsRuntimeSettings
from packages.cnes_infra.tests.aws.production_values import PRODUCTION_VALUES
from tests.production.infra._cognito_dashboard import _assert_dashboard_env, _dashboard_scopes
from tests.production.infra.conftest import (
    MOCK_AWS_PROVIDERS as MOCK_PROVIDERS,
)
from tests.production.infra.conftest import (
    MOCK_PROVIDER,
)

type Tofu = tuple[str, dict[str, str]]
type Artifacts = dict[str, dict[str, object]]
type Node = dict[str, object]
type Resource = tuple[str, str, Mapping[str, object]]

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULE_DIRECTORY = REPO_ROOT / "infra/opentofu/modules/cognito"
MODULE_SOURCE, ISOLATED_SOURCE = "../../modules/cognito", "./modules/cognito"
REVIEWED_SUFFIX = "prod54qgag"
COGNITO = {"name_prefix": "cnesdata-prod", "domain_suffix": REVIEWED_SUFFIX}
ROOT_INPUTS = {
    "name_prefix": {"references": ["local.name_prefix"]},
    "domain_suffix": {"constant_value": REVIEWED_SUFFIX},
}
POOL_ID, CLIENT_ID = "us-east-2_aBcD3fGh1", "1a2b3c4d5e6f7g8h9i0j1k2l3m"
OVERRIDES = {
    ("resource", "aws_cognito_user_pool.this"): {
        "id": POOL_ID, "endpoint": f"cognito-idp.us-east-2.amazonaws.com/{POOL_ID}",
    },
    ("resource", "aws_cognito_user_pool_client.dashboard"): {"id": CLIENT_ID},
    ("data", "data.aws_region.current"): {"region": "us-east-2"},
}
APP_ORIGIN, API_ORIGIN = "https://cnesdata.com.br", "https://api.cnesdata.com.br"
SCOPES = sorted(("openid", "email", "profile", f"{API_ORIGIN}/api.access"))
PUBLIC_CLIENT = {
    "user_pool_id": POOL_ID, "generate_secret": False, "allowed_oauth_flows": ["code"],
    "allowed_oauth_flows_user_pool_client": True, "supported_identity_providers": ["COGNITO"],
    "callback_urls": [f"{APP_ORIGIN}/auth/callback"], "logout_urls": [APP_ORIGIN],
    "explicit_auth_flows": ["ALLOW_REFRESH_TOKEN_AUTH"], "enable_token_revocation": True,
    "prevent_user_existence_errors": "ENABLED",
}
DOMAIN = f"https://cnesdata-{REVIEWED_SUFFIX}.auth.us-east-2.amazoncognito.com"
EXPECTED_OUTPUTS = {
    "issuer": f"https://cognito-idp.us-east-2.amazonaws.com/{POOL_ID}", "audience": API_ORIGIN,
    "user_pool_id": POOL_ID, "client_id": CLIENT_ID, "domain": DOMAIN,
    "authorize_url": f"{DOMAIN}/oauth2/authorize", "token_url": f"{DOMAIN}/oauth2/token",
    "logout_url": f"{DOMAIN}/logout",
}
STRONG_PASSWORD = {
    "minimum_length": 14, "require_lowercase": True, "require_numbers": True,
    "require_symbols": True, "require_uppercase": True,
}
MODULE_RESOURCES = {
    "aws_cognito_user_pool.this", "aws_cognito_resource_server.api",
    "aws_cognito_user_pool_client.dashboard", "aws_cognito_user_pool_domain.this",
    "aws_cognito_managed_login_branding.dashboard",
}
TIER_BLOCKS = (
    "user_pool_add_ons", "email_mfa_configuration", "web_authn_configuration", "sign_in_policy",
)
FORBIDDEN_POOL_ARGUMENTS = {
    "sms_configuration", "sms_authentication_message", "sms_verification_message", "lambda_config",
    *TIER_BLOCKS,
}
EMAIL_RECOVERY = [{"recovery_mechanism": [{"name": "verified_email", "priority": 1}]}]
PREVENT_DESTROY = "aws_cognito_user_pool.this has prevent_destroy set"
MODULE_RUN, ROOT_RUN = "modulo_cognito", "raiz_agenda_cognito"
VALID_SUFFIXES = {
    "sufixo_de_6_caracteres_e_aceito": "a1b2c3",
    "sufixo_de_16_caracteres_e_aceito": "a1b2c3d4e5f6g7h8",
}
INVALID_SUFFIXES = {
    "sufixo_vazio_e_rejeitado": "",
    "sufixo_de_5_caracteres_e_rejeitado": "a1b2c",
    "sufixo_de_17_caracteres_e_rejeitado": "a1b2c3d4e5f6g7h8i",
    "sufixo_com_maiuscula_e_rejeitado": "Prod54qgag",
    "sufixo_com_hifen_e_rejeitado": "prod-54qgag",
    "sufixo_com_ponto_e_rejeitado": "auth.prod54",
    "sufixo_com_aws_e_rejeitado": "prodaws54q",
    "sufixo_com_amazon_e_rejeitado": "amazon54qg",
    "sufixo_com_cognito_e_rejeitado": "cognito54q",
}
OVERRIDE = "\n  override_{kind} {{\n    target = {target}\n    values = {values}\n  }}\n"
REPLACE_POOL = "\n  plan_options {\n    replace = [aws_cognito_user_pool.this]\n  }\n"


class Run(NamedTuple):
    name: str
    variables: Mapping[str, object]
    body: str = ""
    source: str | None = MODULE_SOURCE
    command: str = "plan"


class TofuReport(NamedTuple):
    statuses: dict[str, str]
    plans: dict[str, Node]
    diagnostics: list[tuple[str | None, str]]
    result: subprocess.CompletedProcess[str]


def _run(run: Run) -> str:
    module = f'\n  module {{\n    source = "{run.source}"\n  }}\n' if run.source else "\n"
    assigned = "".join(f"\n    {key} = {json.dumps(value)}" for key, value in run.variables.items())
    header = f'\nrun "{run.name}" {{\n  command = {run.command}\n{module}'
    return f"{header}  variables {{{assigned}\n  }}\n{run.body}}}\n"


def _overrides(prefix: str = "") -> str:
    return "".join(
        OVERRIDE.format(kind=kind, target=f"{prefix}{target}", values=json.dumps(values))
        for (kind, target), values in OVERRIDES.items()
    )


def _suites(shared_outputs: Mapping[str, object]) -> dict[str, str]:
    accepted = {MODULE_RUN: REVIEWED_SUFFIX, **VALID_SUFFIXES}
    modules = "".join(
        _run(Run(name, COGNITO | {"domain_suffix": suffix}, _overrides()))
        for name, suffix in accepted.items()
    )
    root = _run(Run(ROOT_RUN, {"shared_outputs": shared_outputs}, source=None))
    expected = "\n  expect_failures = [var.domain_suffix]\n"
    rejections = "".join(
        _run(Run(name, COGNITO | {"domain_suffix": suffix}, expected))
        for name, suffix in INVALID_SUFFIXES.items()
    )
    # A missing expected failure errors its whole file and skips every later run in it.
    return {
        "cognito": MOCK_PROVIDERS + modules + root,
        "cognito-validation": MOCK_PROVIDERS + rejections,
    }


PROTECTION_SUITE = MOCK_PROVIDERS + "".join((
    _run(Run("cria_pool", COGNITO, source=ISOLATED_SOURCE, command="apply")),
    _run(Run("substitui_pool", COGNITO, REPLACE_POOL, ISOLATED_SOURCE)),
))


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
            text = f"{diagnostic['summary']}: {diagnostic.get('detail')}"
            report.diagnostics.append((message.get("@testrun"), " ".join(text.split())))
    return report


def _assert_runs(report: TofuReport, expected: Mapping[str, str]) -> None:
    actual = {name: report.statuses.get(name) for name in expected}
    assert actual == dict(expected), (
        f"tofu_test runs={actual} diagnostics={report.diagnostics!r} "
        f"stderr={report.result.stderr!r}"
    )


def _module_resources(module: Node, prefix: str = "") -> Iterator[Resource]:
    for resource in cast("list[Node]", module.get("resources", [])):
        if resource["mode"] == "managed":
            expressions = cast("dict[str, Any]", resource.get("expressions", {}))
            values = {
                key: value.get("constant_value", value) if isinstance(value, dict) else value
                for key, value in expressions.items()
            }
            yield f"{prefix}{resource['address']}", cast("str", resource["type"]), values
    for name, call in cast("dict[str, Node]", module.get("module_calls", {})).items():
        yield from _module_resources(cast("Node", call["module"]), f"{prefix}module.{name}.")


def _config(tofu_artifacts: Artifacts) -> list[Resource]:
    return list(_module_resources(cast("Node", tofu_artifacts["configuration"]["root_module"])))


def _plan_resources(plan: Node) -> list[Resource]:
    return [
        (change["address"], change["type"], change["change"]["after"] or {})
        for change in cast("list[dict[str, Any]]", plan.get("resource_changes", []))
        if change["mode"] == "managed"
    ]


def _of_type(resources: Iterable[Resource], kind: str) -> list[Mapping[str, object]]:
    return [values for _, resource_kind, values in resources if resource_kind == kind]


def _single(resources: Iterable[Resource], kind: str) -> Mapping[str, Any]:
    found = _of_type(resources, kind)
    assert len(found) == 1, f"cognito_resource_count kind={kind} found={len(found)}"
    return found[0]


def _planned(report: TofuReport, run: str) -> list[Resource]:
    _assert_runs(report, {run: "pass"})
    return _plan_resources(report.plans[run])


def _outputs(report: TofuReport) -> dict[str, str]:
    _assert_runs(report, {MODULE_RUN: "pass"})
    planned = cast("Node", report.plans[MODULE_RUN]["planned_values"])
    outputs = cast("dict[str, Node]", planned["outputs"])
    return {name: cast("str", output["value"]) for name, output in outputs.items()}


def _sorted(value: object) -> object:
    return sorted(cast("list[str]", value)) if isinstance(value, list) else value


@pytest.fixture(scope="module")
def plan_report(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    valid_shared_outputs: dict[str, object],
) -> Iterator[TofuReport]:
    test_directory = prod_root / ".terraform" / "cognito-tests"
    test_directory.mkdir(parents=True)
    selector = "-test-directory=.terraform/cognito-tests"
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
def protection_report(
    tofu_context: Tofu,
    tofu_artifacts: Artifacts,
    prod_root: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> TofuReport:
    root = tmp_path_factory.mktemp("cognito-protection")
    hidden = shutil.ignore_patterns(".*")
    shutil.copytree(MODULE_DIRECTORY, root / "modules" / "cognito", ignore=hidden)
    for name in ("versions.tf", ".terraform.lock.hcl"):
        shutil.copy2(prod_root / name, root / name)
    (root / "tests").mkdir()
    (root / "tests" / "protection.tftest.hcl").write_text(PROTECTION_SUITE, encoding="utf-8")
    providers = Path(tofu_context[1]["TF_DATA_DIR"]) / "providers"
    tofu = (tofu_context[0], {**tofu_context[1], "TF_DATA_DIR": str(root / ".tofu-data")})
    init = ("init", "-backend=false", "-input=false", f"-plugin-dir={providers}")
    _require_success(_tofu(tofu, root, init), "init-isolated")
    return _report(_tofu(tofu, root, ("test", "-no-color", "-json")))


def test_raiz_agenda_cognito_com_sufixo_revisado(
    plan_report: TofuReport, tofu_artifacts: Artifacts, prod_root: Path
) -> None:
    root = cast("Node", tofu_artifacts["configuration"]["root_module"])
    calls = cast("dict[str, Node]", root.get("module_calls", {}))
    module = (prod_root / MODULE_SOURCE).resolve()
    targets = {n: (prod_root / cast("str", c["source"])).resolve() for n, c in calls.items()}
    domains = _of_type(_planned(plan_report, ROOT_RUN), "aws_cognito_user_pool_domain")

    assert [n for n, target in targets.items() if target == module] == ["cognito"]
    assert calls["cognito"]["expressions"] == ROOT_INPUTS
    assert [domain["domain"] for domain in domains] == [f"cnesdata-{REVIEWED_SUFFIX}"]


def test_client_publico_pkce_sem_secret(plan_report: TofuReport) -> None:
    clients = _of_type(_planned(plan_report, MODULE_RUN), "aws_cognito_user_pool_client")

    assert [{key: _sorted(c.get(key)) for key in PUBLIC_CLIENT} for c in clients] == [
        PUBLIC_CLIENT
    ]


def test_resource_server_e_scope_da_api(
    plan_report: TofuReport, tofu_artifacts: Artifacts
) -> None:
    planned = _planned(plan_report, MODULE_RUN)
    server = _single(planned, "aws_cognito_resource_server")
    client = _single(planned, "aws_cognito_user_pool_client")
    configured = _single(_config(tofu_artifacts), "aws_cognito_user_pool_client")
    resource, dashboard_scopes = _dashboard_scopes()

    assert (server["identifier"], server["user_pool_id"]) == (API_ORIGIN, POOL_ID)
    assert [scope["scope_name"] for scope in server["scope"]] == ["api.access"]
    assert sorted(client["allowed_oauth_scopes"]) == SCOPES == dashboard_scopes
    assert resource == API_ORIGIN == _outputs(plan_report)["audience"]
    assert "aws_cognito_resource_server.api" in configured["allowed_oauth_scopes"]["references"]


def test_tier_do_pool_fixado_explicitamente(
    plan_report: TofuReport, tofu_artifacts: Artifacts
) -> None:
    configured = _of_type(_config(tofu_artifacts), "aws_cognito_user_pool")
    planned = [
        _of_type(_planned(plan_report, run), "aws_cognito_user_pool")
        for run in (MODULE_RUN, ROOT_RUN)
    ]

    assert [pool.get("user_pool_tier") for pool in configured] == ["ESSENTIALS"]
    assert [[pool["user_pool_tier"] for pool in pools] for pools in planned] == [["ESSENTIALS"]] * 2


@pytest.mark.parametrize("run", [MODULE_RUN, ROOT_RUN])
def test_dominio_usa_login_gerenciado_v2(plan_report: TofuReport, run: str) -> None:
    domain = _single(_planned(plan_report, run), "aws_cognito_user_pool_domain")

    assert domain["managed_login_version"] == 2


def test_login_gerenciado_v2_tem_branding_do_client_publico(
    plan_report: TofuReport, tofu_artifacts: Artifacts
) -> None:
    configured = _single(_config(tofu_artifacts), "aws_cognito_managed_login_branding")
    targets = {
        "client_id": "aws_cognito_user_pool_client.dashboard",
        "user_pool_id": "aws_cognito_user_pool.this",
    }

    for attribute, target in targets.items():
        assert set(configured[attribute]["references"]) == {target, f"{target}.id"}
    assert configured["use_cognito_provided_values"] is True
    assert not {"assets", "settings"} & set(configured)
    for run in (MODULE_RUN, ROOT_RUN):
        planned = _planned(plan_report, run)
        branding = _single(planned, "aws_cognito_managed_login_branding")
        client = _single(planned, "aws_cognito_user_pool_client")
        pool = _single(planned, "aws_cognito_user_pool")
        assert branding["client_id"] == client["id"]
        assert branding["user_pool_id"] == pool["id"] == client["user_pool_id"]
        assert branding["use_cognito_provided_values"] is True
        assert branding.get("assets") in (None, [])
        assert branding.get("settings") is None


def test_sem_custom_domain_sms_social_ou_m2m(
    plan_report: TofuReport, tofu_artifacts: Artifacts
) -> None:
    configured = _config(tofu_artifacts)
    planned = _planned(plan_report, MODULE_RUN)
    cognito = {address for address, kind, _ in configured if kind.startswith("aws_cognito_")}
    owned = {kind for address, kind, _ in configured if address.startswith("module.cognito.")}
    domain = _single(planned, "aws_cognito_user_pool_domain")
    pool = _single(planned, "aws_cognito_user_pool")
    flows = [c["allowed_oauth_flows"] for c in _of_type(planned, "aws_cognito_user_pool_client")]

    assert cognito == {f"module.cognito.{address}" for address in MODULE_RESOURCES}
    assert owned == {address.split(".")[0] for address in MODULE_RESOURCES}
    assert (domain["domain"], domain["certificate_arn"]) == (f"cnesdata-{REVIEWED_SUFFIX}", None)
    assert (pool["mfa_configuration"], pool["sms_configuration"], pool["lambda_config"]) == (
        "OFF", [], [],
    )
    assert not FORBIDDEN_POOL_ARGUMENTS & set(_single(configured, "aws_cognito_user_pool"))
    assert {block: pool[block] for block in TIER_BLOCKS} == {block: [] for block in TIER_BLOCKS}
    assert flows == [["code"]]


def test_so_admin_cria_contas_por_email_com_senha_forte(plan_report: TofuReport) -> None:
    pool = _single(_planned(plan_report, MODULE_RUN), "aws_cognito_user_pool")
    admin = [entry["allow_admin_create_user_only"] for entry in pool["admin_create_user_config"]]
    passwords = [{key: entry[key] for key in STRONG_PASSWORD} for entry in pool["password_policy"]]
    # Password history needs ESSENTIALS; the provider sends 0, which leaves it off.
    history = [entry["password_history_size"] for entry in pool["password_policy"]]

    assert admin == [True]
    assert passwords == [STRONG_PASSWORD]
    assert history in ([None], [0])
    assert (pool["username_attributes"], pool["username_configuration"]) == (
        ["email"], [{"case_sensitive": False}],
    )


def test_pool_tem_deletion_protection_e_prevent_destroy(
    plan_report: TofuReport, protection_report: TofuReport
) -> None:
    pool = _single(_planned(plan_report, MODULE_RUN), "aws_cognito_user_pool")
    # The cleanup of cria_pool reports the same prevent_destroy error: only substitui_pool counts.
    replaced = [text for run, text in protection_report.diagnostics if run == "substitui_pool"]

    _assert_runs(protection_report, {"cria_pool": "pass", "substitui_pool": "error"})
    assert pool["deletion_protection"] == "ACTIVE"
    assert [PREVENT_DESTROY in text for text in replaced] == [True], replaced


def test_recuperacao_de_conta_so_por_email_verificado(plan_report: TofuReport) -> None:
    pool = _single(_planned(plan_report, MODULE_RUN), "aws_cognito_user_pool")

    assert pool["account_recovery_setting"] == EMAIL_RECOVERY


def test_sufixo_de_prefixo_invalido_e_rejeitado(plan_report: TofuReport) -> None:
    kind = "aws_cognito_user_pool_domain"
    domains = {
        name: [domain["domain"] for domain in _of_type(_planned(plan_report, name), kind)]
        for name in VALID_SUFFIXES
    }

    _assert_runs(plan_report, dict.fromkeys(INVALID_SUFFIXES, "pass"))
    assert domains == {name: [f"cnesdata-{suffix}"] for name, suffix in VALID_SUFFIXES.items()}


def test_outputs_batem_com_settings_de_producao(plan_report: TofuReport) -> None:
    outputs = _outputs(plan_report)
    client = _single(_planned(plan_report, MODULE_RUN), "aws_cognito_user_pool_client")
    oidc = {"OIDC_ISSUER": "issuer", "OIDC_AUDIENCE": "audience", "OIDC_DOMAIN": "domain"}
    settings = AwsRuntimeSettings.from_mapping(
        PRODUCTION_VALUES | {key: outputs[output] for key, output in oidc.items()}
    )

    assert outputs == EXPECTED_OUTPUTS
    assert settings.production_policy is True
    assert (settings.oidc_issuer, settings.oidc_audience, settings.oidc_domain) == tuple(
        outputs[output] for output in oidc.values()
    )
    assert (settings.oidc_authorize_url, settings.oidc_token_url, settings.oidc_logout_url) == (
        outputs["authorize_url"], outputs["token_url"], outputs["logout_url"],
    )
    _assert_dashboard_env(outputs, client)


def test_suites_gerados_mockam_todo_provider_aws_da_raiz(
    tofu_artifacts: Artifacts, valid_shared_outputs: dict[str, object]
) -> None:
    providers = cast("dict[str, Node]", tofu_artifacts["configuration"]["provider_config"])
    roots = {
        key for key, provider in providers.items()
        if provider["name"] == "aws" and "module_address" not in provider
    }
    suites = [*_suites(valid_shared_outputs).values(), PROTECTION_SUITE]
    mocked = [
        {f"aws.{alias}" if alias else "aws" for alias in MOCK_PROVIDER.findall(suite)}
        for suite in suites
    ]

    assert {"aws", "aws.global_edge"} <= roots
    assert mocked == [roots] * len(suites)
