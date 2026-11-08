"""Controles de mutação do módulo static-web."""

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
ISSUER_CONNECT = "connect-src 'self' https://cognito-idp.us-east-2.amazonaws.com "
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
    ("C27_multipart_desligado", MAIN, GC, '"Enabled"', '"Disabled"', 4),
    ("C28_multipart_longo", MAIN, GC, "days_after_initiation = 1", "days_after_initiation = 30", 4),
    ("C29_csp_sem_object_src", MAIN, "locals", "object-src 'none'; ", "", 4),
    ("C30_csp_sem_base_uri", MAIN, "locals", "base-uri 'self'; ", "", 4),
    ("C31_csp_com_data_uri", MAIN, "locals", "img-src 'self'", "img-src 'self' data:", 4),
    ("C32_csp_com_eval", MAIN, "locals", "script-src 'self'", "script-src 'self' 'unsafe-eval'", 4),
    ("C33_csp_com_issuer", MAIN, "locals", "connect-src 'self' ", ISSUER_CONNECT, 4),
    ("C34_csp_sem_unsafe_inline", MAIN, "locals", " 'unsafe-inline'", "", 4),
    ("C35_abort_com_prefixo", MAIN, GC, "filter {}", 'filter {\n      prefix = "x"\n    }', 4),
    (
        "C36_abort_com_expiracao",
        MAIN,
        GC,
        "abort_incomplete_multipart_upload {",
        "expiration {\n      days = 1\n    }\n    abort_incomplete_multipart_upload {",
        4,
    ),
    (
        "C37_issuer_local_morto",
        MAIN,
        "locals",
        "rollback_window_days = 7",
        'rollback_window_days = 7\n  issuer_origin = "https://x"',
        4,
    ),
)
