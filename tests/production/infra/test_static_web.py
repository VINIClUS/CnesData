import re
import shutil
from hashlib import sha256
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3] / "infra/opentofu/modules/static-web"
FILES = ("main.tf", "variables.tf", "outputs.tf", "function.js")
RESOURCE_NAMES = (
    "s3_bucket s3_bucket_public_access_block s3_bucket_ownership_controls s3_bucket_versioning "
    "s3_bucket_server_side_encryption_configuration s3_bucket_policy "
    "s3_bucket_lifecycle_configuration cloudfront_origin_access_control cloudfront_function "
    "cloudfront_cache_policy cloudfront_response_headers_policy wafv2_web_acl "
    "cloudfront_distribution"
)
KINDS = RESOURCE_NAMES.split()
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
  rule { id = "static-web-gc" status = "Disabled"
    filter { tag { key = "static-web-gc" value = "eligible" } }
    noncurrent_version_expiration {
      noncurrent_days = local.rollback_window_days newer_noncurrent_versions = 1
    }
  }
"""
CSP = (
    'join("", ['
    "\"default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \","
    "\"img-src 'self' data:; connect-src 'self' ${local.issuer_origin} \","
    '"${local.hosted_login_origin} ${local.api_origin} ${local.serving_origin}; ",'
    "\"frame-ancestors 'none'\","
    "])"
)
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
    span = r'join\("", \[\n.*?^  \]\)' if name == "csp" else r"\(\n.*?^    \)"
    multiline = (name, indent) in (("csp", 2), ("condition", 4))
    if not multiline or not expected.startswith(("(", "join(")):
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


def _resource(sources: dict[str, str], kind: str) -> str:
    label = "spa" if kind == "cloudfront_function" else "this"
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
    headers = tuple(
        f'resource "aws_{k}" "{"spa" if k == "cloudfront_function" else "this"}"' for k in KINDS
    )
    _closed(sources["main.tf"], ("terraform", "locals", *headers))
    terraform = _section(sources["main.tf"], "terraform")
    _same(
        terraform,
        'required_providers { aws = { source="hashicorp/aws" '
        'version="~> 6.33" configuration_aliases=[aws.global_edge] } }',
    )
    for kind in KINDS:
        body = _resource(sources, kind)
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
        ("cache_policy_id", "aws_cloudfront_cache_policy.this.id"),
        ("response_headers_policy_id", "aws_cloudfront_response_headers_policy.this.id"),
        ("viewer_protocol_policy", '"redirect-to-https"'),
        ("allowed_methods", '["GET","HEAD"]'),
        ("cached_methods", '["GET","HEAD"]'),
        ("compress", "true"),
    ):
        _field(behavior, key, value, 4)
    _same(
        _section(behavior, "function_association", 4),
        'event_type="viewer-request" function_arn=aws_cloudfront_function.spa.arn',
    )
    _same(
        _section(body, "viewer_certificate", 2),
        "acm_certificate_arn=var.certificate_arn "
        'ssl_support_method="sni-only" minimum_protocol_version="TLSv1.2_2021"',
    )
    function = _resource(sources, "cloudfront_function")
    _field(function, "runtime", '"cloudfront-js-2.0"')
    _field(function, "publish", "true")
    _field(function, "code", 'file("${path.module}/function.js")')


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


def _headers(sources: dict[str, str]) -> None:
    body = _resource(sources, "cloudfront_response_headers_policy")
    security = _section(body, "security_headers_config", 2)
    headers = (
        "content_type_options|frame_options|referrer_policy|"
        'dynamic "strict_transport_security"|dynamic "content_security_policy"'
    )
    _closed(security, tuple(headers.split("|")), 4)
    for block, value in (
        ("content_type_options", "override=true"),
        ("frame_options", 'frame_option="DENY" override=true'),
        ("referrer_policy", 'referrer_policy="strict-origin-when-cross-origin" override=true'),
    ):
        _same(_section(security, block, 4), value)
    hsts = _section(security, 'dynamic "strict_transport_security"', 4)
    _field(hsts, "for_each", "var.certificate_domain_validated ? [1] : []", 6)
    _same(
        _section(hsts, "content", 6),
        "access_control_max_age_sec=31536000 include_subdomains=false preload=false override=true",
    )
    csp = _section(security, 'dynamic "content_security_policy"', 4)
    _field(csp, "for_each", 'var.csp_mode == "enforce" ? [1] : []', 6)
    _same(_section(csp, "content", 6), "content_security_policy=local.csp override=true")
    custom = _section(body, "custom_headers_config", 2)
    _closed(custom, ("items", 'dynamic "items"'), 4)
    _same(
        _section(custom, "items", 4),
        'header="Permissions-Policy" '
        'value="camera=(), microphone=(), geolocation=()" override=true',
    )
    report = _section(custom, 'dynamic "items"', 4)
    _field(report, "for_each", 'var.csp_mode == "report_only" ? [1] : []', 6)
    _same(
        _section(report, "content", 6),
        'header="Content-Security-Policy-Report-Only" value=local.csp override=true',
    )


def assert_headers_cache_lifecycle(sources: dict[str, str]) -> None:
    local = _section(sources["main.tf"], "locals")
    for key, value in (
        ("public_domain", '"cnesdata.com.br"'),
        ("api_origin", '"https://api.cnesdata.com.br"'),
        ("rollback_window_days", "7"),
        ("issuer_origin", 'regex("^https://[^/]+", var.cognito.issuer_url)'),
        ("hosted_login_origin", 'trimsuffix(var.cognito.hosted_login_origin, "/")'),
        ("serving_origin", 'trimsuffix(var.serving_origin, "/")'),
        ("csp", CSP),
    ):
        _field(local, key, value)
    _headers(sources)
    cache = _resource(sources, "cloudfront_cache_policy")
    for key, value in (("min_ttl", "0"), ("default_ttl", "0"), ("max_ttl", "31536000")):
        _field(cache, key, value)
    _same(
        _section(cache, "parameters_in_cache_key_and_forwarded_to_origin", 2),
        'cookies_config{cookie_behavior="none"} headers_config{header_behavior="none"} '
        'query_strings_config{query_string_behavior="none"} '
        "enable_accept_encoding_gzip=true enable_accept_encoding_brotli=true",
    )
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


def test_inputs_e_outputs_mantem_contrato_minimo() -> None:
    assert_io(load_sources(ROOT))


MAIN = "main.tf"
BPA = "s3_bucket_public_access_block"
OAC = "cloudfront_origin_access_control"
POLICY = "s3_bucket_policy"
DIST = "cloudfront_distribution"
HEADERS = "cloudfront_response_headers_policy"
GC = "s3_bucket_lifecycle_configuration"
DIST_ARN = "aws_cloudfront_distribution.this.arn"
WILD_ARN = '"arn:aws:cloudfront::123456789012:distribution/*"'
FOREIGN_ARN = '"arn:aws:cloudfront::123456789012:distribution/FOREIGN"'
ACM_SAMPLE = '"arn:aws:acm:us-east-1:123456789012:certificate/00000000-0000-0000-0000-000000000001"'
ADD_RESOURCE = 'resource "aws_s3_bucket_acl" "extra" {\n}\n'
CSP_REPORT = 'var.csp_mode == "report_only"'
HSTS_GATE = "var.certificate_domain_validated ? [1] : []"
CONTROLS = (
    ("C01_waf_no_provider_padrao", MAIN, "wafv2_web_acl", "provider", "aws", 0),
    ("C02_recurso_fora_do_slice", MAIN, "", "append", ADD_RESOURCE, 0),
    ("C03_versao_desligada", MAIN, "s3_bucket_versioning", '"Enabled"', '"Suspended"', 1),
    ("C04_bpa_permissivo", MAIN, BPA, "block_public_policy", "false", 1),
    ("C05_assinatura_oac_desligada", MAIN, OAC, '"always"', '"never"', 1),
    ("C06_sourcearn_wildcard", MAIN, POLICY, DIST_ARN, WILD_ARN, 1),
    ("C07_sourcearn_alheio", MAIN, POLICY, DIST_ARN, FOREIGN_ARN, 1),
    ("C08_sem_condicao_oac", MAIN, POLICY, "condition", "", 1),
    ("C09_recurso_global_allow", MAIN, POLICY, '"${aws_s3_bucket.this.arn}/*"', '"*"', 1),
    ("C10_allow_publico", MAIN, POLICY, "principal", '"*"', 1),
    ("C11_tls_deny_invertido", MAIN, POLICY, '"false"', '"true"', 1),
    ("C12_certificado_desligado_do_input", MAIN, DIST, "var.certificate_arn", ACM_SAMPLE, 2),
    ("C13_alias_alheio", MAIN, DIST, "local.public_domain", '"example.invalid"', 2),
    ("C14_terceiro_grupo_waf", MAIN, "wafv2_web_acl", "rule", "", 3),
    ("C15_waf_regional", MAIN, "wafv2_web_acl", '"CLOUDFRONT"', '"REGIONAL"', 3),
    ("C16_csp_sem_hosted_login", MAIN, "locals", "${local.hosted_login_origin} ", "", 4),
    ("C17_modos_csp_colapsados", MAIN, HEADERS, CSP_REPORT, 'var.csp_mode == "enforce"', 4),
    ("C18_hsts_incondicional", MAIN, HEADERS, HSTS_GATE, "[1]", 4),
    ("C19_ttl_minimo_positivo", MAIN, "cloudfront_cache_policy", "min_ttl", "1", 4),
    ("C20_erro_mascarado", MAIN, DIST, "error_code", "", 4),
    ("C21_lifecycle_ativado", MAIN, GC, '"Disabled"', '"Enabled"', 4),
    ("C22_expiracao_current", MAIN, GC, "expiration", "", 4),
    ("C23_api_output_alterado", "outputs.tf", "api_base_url", "/api/v1", "/api/v2", 5),
    ("C24_cognito_com_default", "variables.tf", "cognito", "default", "", 5),
    ("C25_serving_sem_validation", "variables.tf", "serving_origin", "validation", "", 5),
    ("C26_arquivo_da_funcao_errado", MAIN, "cloudfront_function", "/function.js", "/missing.js", 2),
)
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
        return _replace_once(body, "    status", "    expiration { days = 7 }\n    status")
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
    if old in ("provider", "block_public_policy", "min_ttl"):
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
    label = "spa" if kind == "cloudfront_function" else "this"
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
