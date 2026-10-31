# Static web composition — authoritative continuation amendment

Status: controller-adopted; integration pending. This amends issue #382,
not a new roadmap, tracker, deployment permission or independent specification.

## Authority and precedence

Newer explicit user restrictions govern ownership and operational permissions.
This continuation then governs only the declarative composition described below.
All other obligations in the governing infrastructure Task5 and deployment design persist.
The historical infrastructure plan is byte-immutable; it is not replaced or rewritten.

Governing preimages at d24d4f6b312405a8a3a2073e141a6c4a5a0e6335:
- docs/superpowers/plans/2026-08-31-cnesdata-production-infrastructure-implementation-plan.md
  SHA256 3d0cd1b2002d3a2b1157b79a0557547414043436ae54ea11c426d800e7c9af7e.
- docs/superpowers/specs/2026-08-29-cnesdata-production-deployment-design.md
  SHA256 52276df64cd83bc62b43fc4ce6bb15c689a9bf5919d7d9ec71dec096ad8de950.

## Bounded source and test paths

Modify infra/opentofu/env/prod/main.tf and infra/opentofu/env/prod/outputs.tf.
Modify only the output-inventory test in tests/production/infra/test_composition.py.
Create tests/production/infra/test_static_web_composition.py.
Keep all four static-web module files, existing static Python/JS tests,
providers/versions/locks, shared-output schema and CI/scanner policy unchanged.
Modify only the relevant deployment-design ownership/dependency paragraph and reference
this amendment; keep each changed/new document within500 lines. Do not edit the715-line plan.
Any additional tracked path requires an explicit amendment before authoring.

### Ratified follow-up: shared mock compatibility

The complete root regression exposed legacy empty provider mocks that cannot satisfy
the static-web input validators. The independently reviewed follow-up permits these paths
under tests/production/infra: conftest.py, test_cognito.py, test_control_plane.py,
test_data_buckets.py, test_data_buckets_protection.py, test_network.py and
test_network_guards.py. The controller ratified this scope before writer activation.

Centralize the existing mock string and AWS header recognizer in conftest.py.
Supply only computed ACM/function ARNs, Cognito endpoint, S3 regional host and regional/global
region data. Preserve all configured fields, existing overrides, runs and assertion oracles.
The recognizer must support nested defaults and retain exact provider/alias coverage;
eight literal positive/negative cases exercise that boundary. Do not add root overrides.
The separate static-web A/B fixtures remain independent. Existing validators stay unchanged.
The integration gate is the complete 173-case infra suite, including the eight guard cases.
Mechanical import ordering and assertion splitting required by Ruff are allowed without
changing predicates, messages, control selectors or the guard-before-index evaluation order.

## Exact declarations

Root aws_acm_certificate.static_web uses aws.global_edge/us-east-1:
domain_name=cnesdata.com.br, validation_method=DNS, create_before_destroy=true.
No extra SAN/domain, wildcard, imported key, private CA, DNS resource or validation waiter.
A certificate request/ARN is not ISSUED; no apply or implicit issuance sequence is authorized.

module.static_web uses ../../modules/static-web; explicitly map aws and aws.global_edge.
Inputs: bucket_name=cnesdata-prod-static-web from local.name_prefix plus -static-web;
certificate_arn=aws_acm_certificate.static_web.arn; cognito issuer_url/hosted_login_origin/
client_id consume module.cognito.issuer/domain/client_id respectively;
serving_origin consumes module.data_buckets.serving_origin.
Initial csp_mode=report_only, certificate_domain_validated=false, waf_ruleset=baseline_v1.
Default region stays us-east-2. No production readiness or capacity claim follows.

Root cloudfront_domain_name consumes module.static_web.distribution_domain_name.
Root cloudfront_distribution_id consumes module.static_web.distribution_id.
acm_dns_validation_records is a map keyed by each actual DVO domain_name, with exact
name/type/value copied from resource_record_name/resource_record_type/resource_record_value.
Preserve tokens and trailing dots; never fabricate validation records from the ARN.
dashboard_runtime_env contains only VITE_API_BASE_URL from module.static_web.api_base_url:
https://api.cnesdata.com.br/api/v1. Only api_loopback_contract remains null.

## Tests and proof limits

Evaluate the real root/modules in isolated native mocked runs, every run command=plan;
mock both AWS provider configurations and only their computed boundary values.
No override_module or replacement modules. Pre-fix missing resources/null outputs must
fail explicit behavior assertions after successful setup, not invalid override-target errors.
A/B propagation is behavioral only for serving endpoint, ACM ARN, CloudFront outputs
and DVO record fields. Exact evaluated Cognito origin remains a CSP assertion.

Issuer-pool identity and client_id are not behaviorally observable in this CSP consumer:
valid issuers share the same allowed host, and the pool path is discarded.
Require the normalized actual static_web issuer_url input expression to reference
module.cognito.issuer, and the client_id expression to reference module.cognito.client_id.
A valid literal issuer on the same host must fail the structural companion assertion;
its CSP may remain identical. Do not count input-validation/setup failure as this control.
Keep the immutable module unchanged; do not invent behavioral issuer A/B propagation.

Independent TESTS SPEC/UTILITY precedes CODE; exact-source SPEC/Standards follows.
Source freeze, runtime preflight and sole-writer handoff are required before execution.
Mocks are not live provider/authentication/issuance/DNS/FREE evidence.

## Ownership and retained holds

personal-infra-live is limited to Budget/freeze and its backend. It receives no DNS,
additional state or other resource ownership. Product owns Web S3/CloudFront/ACM/WAF.
DNS is an external handoff; existing operator/account/zone/configuration and change evidence
remain unresolved before any real action. No new owner, repository or tracker is invented.

Declarative use of merged381 outputs is permitted;381/398 actual code+PKCE/resource/aud,
refresh and final empty cleanup inventory remain open. #60 stays open until official PEN IANA;
production certificate issuance/rollout stays blocked as user policy, not an ACM mock input.
Deploy develop stays disabled. No apply, DNS mutation, FREE activation or paid fallback.
Costs<=USD8/product, aggregate freezeUSD15, paid ActionsUSD0 and BILLING_MODE=disabled.

Object headers/no-secret inventory, current/prior key+version references, seven-day
retirement, backup/rollback/readback, strict total below5,000,000,000 bytes including
all versions/incomplete uploads/next release, and reviewed multipart abort/upload deadlines
remain later delivery work. GC stays Disabled. #388 cost/freeze and #389 nonmutating
acceptance/runbooks remain their scopes. This composition slice does not close #382.
