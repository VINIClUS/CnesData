import json
import re
import shutil
from hashlib import sha256
from pathlib import Path

import pytest

from tests.production.infra._static_web_controls import CONTROLS
from tests.production.infra._static_web_free import (
    CACHING_OPTIMIZED,
    SECURITY_HEADERS,
    run_headers_function,
)

ROOT = Path(__file__).resolve().parents[3] / "infra/opentofu/modules/static-web"
FILES = ("main.tf", "variables.tf", "outputs.tf", "function.js", "headers.js")
RESOURCES = (
    ("s3_bucket", "this"), ("s3_bucket_public_access_block", "this"),
    ("s3_bucket_ownership_controls", "this"), ("s3_bucket_versioning", "this"),
    ("s3_bucket_server_side_encryption_configuration", "this"), ("s3_bucket_policy", "this"),
    ("s3_bucket_lifecycle_configuration", "this"), ("cloudfront_origin_access_control", "this"),
    ("cloudfront_function", "spa"), ("cloudfront_function", "headers"),
    ("wafv2_web_acl", "this"), ("cloudfront_distribution", "this"),
)
KINDS = [kind for kind, _ in RESOURCES]
INPUT_NAMES = (
    "bucket_name certificate_arn cognito serving_origin csp_mode "
    "certificate_domain_validated waf_ruleset"
)
INPUTS = INPUT_NAMES.split()
OUTPUTS = {
    "bucket_name": "aws_s3_bucket.this.bucket",
    "bucket_arn": "aws_s3_bucket.this.arn",
    "distribution_id": "aws_cloudfront_distribution.this.id",
    "distribution_arn": "aws_cloudfront_distribution.this.arn",
    "distribution_domain_name": "aws_cloudfront_distribution.this.domain_name",
    "web_acl_arn": "aws_wafv2_web_acl.this.arn",
    "api_base_url": '"https://api.cnesdata.com.br/api/v1"',
    "free_subscription_binding_input": "{distribution_arn=aws_cloudfront_distribution.this.arn,"
    "web_acl_arn=aws_wafv2_web_acl.this.arn}",
}
ALLOW = """{ Sid = "AllowCloudFront" Effect = "Allow"
    Principal = { Service = "cloudfront.amazonaws.com" }
    Action = "s3:GetObject" Resource = "${aws_s3_bucket.this.arn}/*"
    Condition = { StringEquals = { "AWS:SourceArn" = aws_cloudfront_distribution.this.arn } } }"""
DENY = """{ Sid = "DenyInsecureTransport" Effect = "Deny" Principal = "*" Action = "s3:*"
    Resource = [aws_s3_bucket.this.arn, "${aws_s3_bucket.this.arn}/*"]
    Condition = { Bool = { "aws:SecureTransport" = "false" } } }"""
LIFECYCLE = """bucket = aws_s3_bucket.this.id depends_on = [aws_s3_bucket_versioning.this]
  rule { id = "static-web-gc" status = "Enabled"
    filter { tag { key = "static-web-gc" value = "eligible" } }
    noncurrent_version_expiration {
      noncurrent_days = local.rollback_window_days newer_noncurrent_versions = 1
    }
  }
  rule { id = "static-web-abort-multipart" status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 1 }
  }
"""
CSP = (
    'join("", ['
    "\"default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \","
    "\"img-src 'self'; connect-src 'self' \","
    '"${local.hosted_login_origin} ${local.api_origin} ${local.serving_origin}; ",'
    "\"object-src 'none'; base-uri 'self'; frame-ancestors 'none'\","
    "])"
)
CSP_HEADER = (
    '(var.csp_mode == "enforce" ? "content-security-policy" : '
    '"content-security-policy-report-only")'
)
RESPONSE_HEADERS = (
    "merge({"
    + " ".join(f'"{name}" = "{value}"' for name, value in SECURITY_HEADERS.items())
    + " (local.csp_header) = local.csp },"
    ' var.certificate_domain_validated ? { "strict-transport-security" = "max-age=31536000" }'
    " : {},)"
)
HEADERS_CODE = (
    'templatefile("${path.module}/headers.js", { headers = jsonencode(local.response_headers) })'
)
FREE_INELIGIBLE = re.compile(
    r"forwarded_values|min_ttl|default_ttl|max_ttl|realtime_log_config_arn|"
    r"response_headers_policy_id|origin_request_policy_id|field_level_encryption_id|"
    r"logging_config|continuous_deployment_policy_id|key_value_store|trusted_"
)
MULTILINE = {
    ("csp", 2): r'join\("", \[\n.*?^  \]\)',
    ("csp_header", 2): r"\(\n.*?^  \)",
    ("response_headers", 2): r"merge\(\n.*?^  \)",
    ("code", 2): r"templatefile\(.*?^  \}\)",
    ("condition", 4): r"\(\n.*?^    \)",
}
VALIDATIONS = {
    "bucket_name": r'can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$",var.bucket_name))',
    "certificate_arn": r'(can(regex(join("",["^arn:aws:acm:us-east-1:[0-9]{12}:certificate/",'
    r'"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
    r'[0-9a-fA-F]{12}$",]),var.certificate_arn)))',
    "cognito": r'(can(regex("^https://cognito-idp\\.us-east-2\\.amazonaws\\.com/'
    r'us-east-2_[A-Za-z0-9]+/?$",var.cognito.issuer_url))&&'
    r'can(regex("^https://[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?'
    r'\\.auth\\.us-east-2\\.amazoncognito\\.com/?$",var.cognito.hosted_login_origin))'
    r"&&length(trimspace(var.cognito.client_id))>0)",
    "serving_origin": r'(can(regex("^https://[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?'
    r'(\\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+/?$",var.serving_origin)))',
    "csp_mode": 'contains(["report_only","enforce"],var.csp_mode)',
    "waf_ruleset": 'var.waf_ruleset=="baseline_v1"',
}


def _compact(text: str) -> str:
    return re.sub(r"\s+", "", text)


def load_sources(root: Path) -> dict[str, str]:
    sources = {}
    for name in FILES:
        path = root / name
        assert path.is_file(), f"arquivo_ausente caminho={path}"
        sources[name] = path.read_text(encoding="utf-8")
    return sources


def _sections(source: str, header: str, indent: int = 0) -> list[str]:
    assert not re.search(r"(?m)(?:^|\s)(?:#|//|/\*)|<<", source), "comentario_ou_heredoc"
    prefix = " " * indent
    pattern = rf"^{prefix}{re.escape(header)} \{{\n(.*?)^{prefix}\}}[ \t]*$"
    return re.findall(pattern, source, re.MULTILINE | re.DOTALL)


def _section(source: str, header: str, indent: int = 0) -> str:
    bodies = _sections(source, header, indent)
    assert len(bodies) == 1, f"bloco={header} quantidade={len(bodies)}"
    return bodies[0]


def _field(body: str, name: str, expected: str, indent: int = 2) -> None:
    span = MULTILINE.get((name, indent), r"[^\n]+")
    if not expected.startswith(("(", "join(", "merge(", "templatefile(")):
        span = r"[^\n]+"
    prefix = rf"^{' ' * indent}{re.escape(name)}[ \t]*=[ \t]*"
    assert len(re.findall(prefix, body, re.MULTILINE)) == 1, f"campo={name} atribuicoes"
    values = re.findall(prefix + f"({span})$", body, re.MULTILINE | re.DOTALL)
    assert len(values) == 1, f"campo={name} quantidade={len(values)}"
    _same(values[0], expected, name)


def _same(actual: str, expected: str, field: str = "") -> None:
    assert _compact(actual) == _compact(expected), f"campo={field} esperado={expected[:80]}"
    for literal in re.findall(r'"[^"]*"', expected):
        assert actual.count(literal) == expected.count(literal), f"campo={field} literal={literal}"


def _resource(sources: dict[str, str], kind: str, label: str = "") -> str:
    label = label or ("spa" if kind == "cloudfront_function" else "this")
    return _section(sources["main.tf"], f'resource "aws_{kind}" "{label}"')


def _closed(source: str, headers: tuple[str, ...], indent: int = 0) -> None:
    rest = source
    prefix = " " * indent
    for header in headers:
        _section(source, header, indent)
        pattern = rf"^{prefix}{re.escape(header)} \{{\n.*?^{prefix}\}}[ \t]*$"
        rest = re.sub(pattern, "", rest, flags=re.MULTILINE | re.DOTALL)
    assert not rest.strip(), f"top_level_desconhecido={rest.strip()[:100]}"


def assert_ownership(sources: dict[str, str]) -> None:
    headers = tuple(f'resource "aws_{kind}" "{label}"' for kind, label in RESOURCES)
    _closed(sources["main.tf"], ("terraform", "locals", *headers))
    terraform = _section(sources["main.tf"], "terraform")
    _same(
        terraform,
        'required_providers { aws = { source="hashicorp/aws" '
        'version="~> 6.33" configuration_aliases=[aws.global_edge] } }',
    )
    for kind, label in RESOURCES:
        body = _resource(sources, kind, label)
        if kind == "wafv2_web_acl":
            _field(body, "provider", "aws.global_edge")
        else:
            assert not re.search(r"^  provider\s*=", body, re.MULTILINE), f"provider={kind}"
    assert not re.search(r'\b(provider|module|data)\s+"', sources["main.tf"]), "ownership"


def assert_private_origin(sources: dict[str, str]) -> None:
    bucket = _resource(sources, "s3_bucket")
    _field(bucket, "bucket", "var.bucket_name")
    assert "force_destroy" not in bucket, "bucket_force_destroy"
    _same(_section(bucket, "lifecycle", 2), "prevent_destroy=true")
    for kind in KINDS[1:6]:
        _field(_resource(sources, kind), "bucket", "aws_s3_bucket.this.id")
    bpa = _resource(sources, "s3_bucket_public_access_block")
    flags = "block_public_acls block_public_policy ignore_public_acls restrict_public_buckets"
    for flag in flags.split():
        _field(bpa, flag, "true")
    _same(
        _section(_resource(sources, "s3_bucket_ownership_controls"), "rule", 2),
        'object_ownership="BucketOwnerEnforced"',
    )
    _same(
        _section(_resource(sources, "s3_bucket_versioning"), "versioning_configuration", 2),
        'status="Enabled"',
    )
    _same(
        _section(_resource(sources, "s3_bucket_server_side_encryption_configuration"), "rule", 2),
        'apply_server_side_encryption_by_default{sse_algorithm="AES256"}',
    )
    policy = _resource(sources, "s3_bucket_policy")
    _same(
        policy,
        f'bucket=aws_s3_bucket.this.id policy=jsonencode({{Version="2012-10-17" '
        f"Statement=[{ALLOW},{DENY}]}})",
    )
    oac = _resource(sources, "cloudfront_origin_access_control")
    for key, value in (
        ("origin_access_control_origin_type", '"s3"'),
        ("signing_behavior", '"always"'),
        ("signing_protocol", '"sigv4"'),
    ):
        _field(oac, key, value)


def assert_distribution(sources: dict[str, str]) -> None:
    body = _resource(sources, "cloudfront_distribution")
    assert not re.search(
        r"ordered_cache_behavior|\bdynamic\b|website|custom_origin|origin_access_identity", body
    ), "behavior_ou_origem_proibida"
    _field(body, "aliases", "[local.public_domain]")
    _field(body, "web_acl_id", "aws_wafv2_web_acl.this.arn")
    origin = _section(body, "origin", 2)
    _field(origin, "origin_id", '"static-web-s3"', 4)
    _field(origin, "domain_name", "aws_s3_bucket.this.bucket_regional_domain_name", 4)
    _field(origin, "origin_access_control_id", "aws_cloudfront_origin_access_control.this.id", 4)
    behavior = _section(body, "default_cache_behavior", 2)
    for key, value in (
        ("target_origin_id", '"static-web-s3"'),
        ("cache_policy_id", "local.caching_optimized_policy_id"),
        ("viewer_protocol_policy", '"redirect-to-https"'),
        ("allowed_methods", '["GET","HEAD"]'),
        ("cached_methods", '["GET","HEAD"]'),
        ("compress", "true"),
    ):
        _field(behavior, key, value, 4)
    assert not FREE_INELIGIBLE.search(body), "free_plan_ineligible"
    associations = _sections(behavior, "function_association", 4)
    events = (("viewer-request", "spa"), ("viewer-response", "headers"))
    assert len(associations) == len(events), f"function_association={len(associations)}"
    for association, (event, label) in zip(associations, events, strict=True):
        _same(association, f'event_type="{event}" function_arn=aws_cloudfront_function.{label}.arn')
    _same(
        _section(body, "viewer_certificate", 2),
        "acm_certificate_arn=var.certificate_arn "
        'ssl_support_method="sni-only" minimum_protocol_version="TLSv1.2_2021"',
    )
    spa = _resource(sources, "cloudfront_function")
    _field(spa, "code", 'file("${path.module}/function.js")')
    headers = _resource(sources, "cloudfront_function", "headers")
    _field(headers, "name", '"cnesdata-prod-static-web-headers"')
    _field(headers, "code", HEADERS_CODE)
    for function in (spa, headers):
        _field(function, "runtime", '"cloudfront-js-2.0"')
        _field(function, "publish", "true")
        assert not FREE_INELIGIBLE.search(function), "free_plan_ineligible"
    template = re.findall(r"\$\{[^}]*\}|%\{|`", sources["headers.js"])
    assert template == ["${headers}"], f"template_headers={template}"


def _waf_rule(group: str, priority: int) -> str:
    return (
        f'    name = "AWS-{group}"\n    priority = {priority}\n'
        "    override_action {\n      none {}\n    }\n"
        "    statement {\n      managed_rule_group_statement {\n"
        f'        name = "{group}"\n        vendor_name = "AWS"\n      }}\n    }}\n'
        "    visibility_config {\n      sampled_requests_enabled = false\n"
        f'      cloudwatch_metrics_enabled = false\n      metric_name = "{group}"\n    }}\n'
    )


def assert_waf(sources: dict[str, str]) -> None:
    body = _resource(sources, "wafv2_web_acl")
    _field(body, "name", '"cnesdata-prod-static-web"')
    _field(body, "scope", '"CLOUDFRONT"')
    _same(_section(body, "default_action", 2), "allow{}")
    _same(
        _section(body, "visibility_config", 2),
        "sampled_requests_enabled=false "
        'cloudwatch_metrics_enabled=false metric_name="cnesdata-prod-static-web"',
    )
    rules = _sections(body, "rule", 2)
    assert len(rules) == 2, f"waf_rules={len(rules)}"
    groups = ("AWSManagedRulesCommonRuleSet", "AWSManagedRulesAmazonIpReputationList")
    for priority, rule in enumerate(rules):
        _same(rule, _waf_rule(groups[priority], priority))
    assert not re.search(r"\bdynamic\b|logging|captcha|challenge|scope_down|version\s*=", body), (
        "waf"
    )


def assert_headers_cache_lifecycle(sources: dict[str, str]) -> None:
    local = _section(sources["main.tf"], "locals")
    for key, value in (
        ("public_domain", '"cnesdata.com.br"'),
        ("api_origin", '"https://api.cnesdata.com.br"'),
        ("rollback_window_days", "7"),
        ("hosted_login_origin", 'trimsuffix(var.cognito.hosted_login_origin, "/")'),
        ("serving_origin", 'trimsuffix(var.serving_origin, "/")'),
        ("csp", CSP),
        ("caching_optimized_policy_id", f'"{CACHING_OPTIMIZED}"'),
        ("csp_header", CSP_HEADER),
        ("response_headers", RESPONSE_HEADERS),
    ):
        _field(local, key, value)
    assert "issuer_origin" not in local, "issuer_origin_morto"
    errors = _sections(_resource(sources, "cloudfront_distribution"), "custom_error_response", 2)
    assert len(errors) == 2, "erros_quantidade"
    for error, code in zip(errors, (403, 404), strict=True):
        _same(error, f"error_code={code} error_caching_min_ttl=0")
    _same(_resource(sources, "s3_bucket_lifecycle_configuration"), LIFECYCLE)


def assert_io(sources: dict[str, str]) -> None:
    _closed(sources["variables.tf"], tuple(f'variable "{name}"' for name in INPUTS))
    for name in INPUTS:
        body = _section(sources["variables.tf"], f'variable "{name}"')
        assert not re.search(r"^  default\s*=", body, re.MULTILINE), f"default={name}"
        _field(body, "nullable", "false")
        if name == "cognito":
            types = re.findall(r"(?ms)^  type[ \t]*=[ \t]*(object\(\{\n.*?^  \}\)|[^\n]*)$", body)
            assert len(types) == 1, f"tipo=cognito quantidade={len(types)}"
            _same(types[0], "object({issuer_url=stringhosted_login_origin=stringclient_id=string})")
        else:
            _field(body, "type", "bool" if name == "certificate_domain_validated" else "string")
        if name in VALIDATIONS:
            _field(_section(body, "validation", 2), "condition", VALIDATIONS[name], 4)
    _closed(sources["outputs.tf"], tuple(f'output "{name}"' for name in OUTPUTS))
    for name, expected in OUTPUTS.items():
        body = _section(sources["outputs.tf"], f'output "{name}"')
        if name == "free_subscription_binding_input":
            value = re.search(r"(?ms)^  value\s*=\s*(\{.*?^  \})", body)
            assert value, "output_binding_ausente"
            _same(value[1], expected)
        else:
            _field(body, "value", expected)


def test_modulo_respeita_ownership_e_aliases() -> None:
    assert_ownership(load_sources(ROOT))


def test_origem_privada_separa_allow_oac_e_deny_tls() -> None:
    assert_private_origin(load_sources(ROOT))


def test_distribuicao_liga_s3_oac_waf_acm_e_funcao() -> None:
    assert_distribution(load_sources(ROOT))


def test_waf_declara_somente_baseline_v1() -> None:
    assert_waf(load_sources(ROOT))


def test_headers_cache_e_lifecycle_respeitam_ruling() -> None:
    assert_headers_cache_lifecycle(load_sources(ROOT))


def test_funcao_de_headers_sobrescreve_a_origem_com_o_mapa_do_modulo() -> None:
    template = load_sources(ROOT)["headers.js"]
    mapped = SECURITY_HEADERS | {"content-security-policy-report-only": "default-src 'self'"}
    code = template.replace("${headers}", json.dumps(mapped))

    assert code != template
    assert run_headers_function(code) == mapped


def test_inputs_e_outputs_mantem_contrato_minimo() -> None:
    assert_io(load_sources(ROOT))


GROUPS = (assert_ownership, assert_private_origin, assert_distribution) + (
    assert_waf,
    assert_headers_cache_lifecycle,
    assert_io,
)
type Control = tuple[str, str, str, str, str, int]


def _replace_once(text: str, old: str, new: str) -> str:
    assert text.count(old) == 1, f"mutacao_alvo={old} quantidade={text.count(old)}"
    return text.replace(old, new, 1)


def _structural_mutation(body: str, old: str, new: str) -> str:
    if old == "rule":
        return (
            body + "\n  rule {\n" + _waf_rule("AWSManagedRulesKnownBadInputsRuleSet", 2) + "\n  }\n"
        )
    if old == "expiration":
        anchor = '    id     = "static-web-gc"'
        return _replace_once(body, anchor, "    expiration { days = 7 }\n" + anchor)
    if old == "default":
        return body + '  default={issuer_url="",hosted_login_origin="",client_id="synthetic"}\n'
    if old == "validation":
        section = _section(body, "validation", 2)
        return _replace_once(body, "  validation {\n" + section + "  }", "")
    return _replace_once(body, old, new)


def _mutation(body: str, control: Control) -> str:
    name, _, _, old, new, _ = control
    patterns = {
        "condition": r"Condition\s*=\s*\{\s*StringEquals\s*=\s*\{[^}]+\}\s*\}",
        "principal": r'Principal\s*=\s*\{\s*Service\s*=\s*"cloudfront.amazonaws.com"\s*\}',
        "error_code": r"(?m)^    error_code\s*=\s*403$",
        "C09": r'Resource\s*=\s*"\$\{aws_s3_bucket.this.arn\}/\*"',
    }
    key = "C09" if name.startswith("C09") else old
    if old in ("provider", "block_public_policy"):
        patterns[key] = rf"(?m)^  {old}\s*=\s*.+$"
        new = f"  {old} = {new}"
    if key in patterns:
        targets = re.findall(patterns[key], body)
        assert len(targets) == 1, f"mutacao_alvo={key} quantidade={len(targets)}"
        if key in ("principal", "C09"):
            new = f"{'Principal' if key == 'principal' else 'Resource'} = {new}"
        if key == "error_code":
            new = targets[0] + "\n    response_code = 200"
        return _replace_once(body, targets[0], new)
    return _structural_mutation(body, old, new)


def _corrupt(sources: dict[str, str], control: Control) -> dict[str, str]:
    _, file, kind, old, new, _ = control
    if old == "append":
        return sources | {file: sources[file] + "\n" + new}
    kind, _, label = kind.partition(":")
    label = label or ("spa" if kind == "cloudfront_function" else "this")
    header = f'resource "aws_{kind}" "{label}"'
    if file != "main.tf":
        header = f'{"variable" if file == "variables.tf" else "output"} "{kind}"'
    if kind == "locals":
        header = "locals"
    body = _section(sources[file], header)
    changed = _mutation(body, control)
    return sources | {file: _replace_once(sources[file], body, changed)}


@pytest.mark.parametrize("control", CONTROLS, ids=[row[0] for row in CONTROLS])
def test_controle_detecta_corrupcao_de_source(control: Control, tmp_path: Path) -> None:
    original = load_sources(ROOT)
    before = {name: sha256((ROOT / name).read_bytes()).hexdigest() for name in FILES}
    group = GROUPS[control[-1]]
    group(original)
    copy = tmp_path / "static-web"
    try:
        shutil.copytree(ROOT, copy)
        mutant = _corrupt(load_sources(copy), control)
        target = copy / control[1]
        target.write_text(mutant[control[1]], encoding="utf-8")
        assert sha256(target.read_bytes()).hexdigest() != before[control[1]], "mutacao_sem_delta"
        with pytest.raises(AssertionError):
            group(load_sources(copy))
    finally:
        assert before == {name: sha256((ROOT / name).read_bytes()).hexdigest() for name in FILES}
        shutil.rmtree(copy, ignore_errors=True)
