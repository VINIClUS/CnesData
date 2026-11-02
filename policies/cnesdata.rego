package main

region_allowed := "us-east-2"

edge_region := "us-east-1"

forbidden_types := {
	"aws_nat_gateway": "nat_gateway",
	"aws_lb": "load_balancer",
	"aws_alb": "load_balancer",
	"aws_elb": "load_balancer",
	"aws_db_instance": "database",
	"aws_rds_cluster": "database",
	"aws_rds_cluster_instance": "database",
	"aws_redshift_cluster": "data_warehouse",
	"aws_redshiftserverless_namespace": "data_warehouse",
	"aws_opensearch_domain": "search",
	"aws_elasticsearch_domain": "search",
	"aws_opensearchserverless_collection": "search",
	"aws_elasticache_cluster": "cache",
	"aws_elasticache_replication_group": "cache",
	"aws_elasticache_serverless_cache": "cache",
	"aws_dynamodb_global_table": "global_table_replica",
	"aws_s3_bucket_replication_configuration": "s3_replication",
	"aws_shield_subscription": "paid_plan",
	"aws_athena_capacity_reservation": "paid_plan",
	"aws_lambda_provisioned_concurrency_config": "paid_plan",
}

changes contains rc if {
	some rc in object.get(input, "resource_changes", [])
	rc.mode == "managed"
}

resource_msg(reason, rc) := sprintf(
	"forbidden_resource reason=%s type=%s address=%s",
	[reason, rc.type, rc.address],
)

deny contains resource_msg(forbidden_types[rc.type], rc) if {
	some rc in changes
}

deny contains resource_msg("container_insights", rc) if {
	some rc in changes
	rc.type == "aws_ecs_cluster"
	some setting in rc.change.after.setting
	setting.name == "containerInsights"
	setting.value != "disabled"
}

deny contains resource_msg("paid_plan", rc) if {
	some rc in changes
	rc.type == "aws_cognito_user_pool"
	rc.change.after.user_pool_tier == "PLUS"
}

deny contains resource_msg("global_table_replica", rc) if {
	some rc in changes
	rc.type == "aws_dynamodb_table"
	count(object.get(rc.change.after, "replica", [])) > 0
}

config_resources contains entry if {
	walk(input.configuration.root_module, [path, node])
	is_object(node)
	some resource in object.get(node, "resources", [])
	startswith(resource.type, "aws_")
	modules := [sprintf("module.%s", [path[i + 1]]) | some i, key in path; key == "module_calls"]
	entry := {
		"type": resource.type,
		"key": resource.provider_config_key,
		"address": concat(".", array.concat(modules, [resource.address])),
	}
}

region_of(key) := region if {
	region := input.configuration.provider_config[key].expressions.region.constant_value
} else := "unset"

cloudfront_scope(entry) if {
	some rc in changes
	rc.type == entry.type
	startswith(rc.address, entry.address)
	rc.change.after.scope == "CLOUDFRONT"
}

edge_exempt(entry, region) if {
	region == edge_region
	entry.type == "aws_acm_certificate"
}

edge_exempt(entry, region) if {
	region == edge_region
	entry.type == "aws_wafv2_web_acl"
	cloudfront_scope(entry)
}

deny contains msg if {
	some entry in config_resources
	region := region_of(entry.key)
	region != region_allowed
	not edge_exempt(entry, region)
	msg := sprintf(
		"forbidden_region region=%s type=%s address=%s",
		[region, entry.type, entry.address],
	)
}

limits := {"product": 8, "aggregate": 15}

scenarios := ["min", "base", "max"]

price(line) := line.unit_price_usd if line.unit_price_usd != null

price(line) := 0 if line.unit_price_usd == null

line_cost(line, scenario) := max([0, (qty(line, scenario) - free(line, scenario)) * price(line)])

qty(line, scenario) := line.quantity[scenario] if line.quantity[scenario] != null

qty(line, scenario) := 0 if line.quantity[scenario] == null

free(line, scenario) := line.free_allowance[scenario] if line.free_allowance[scenario] != null

free(line, scenario) := 0 if line.free_allowance[scenario] == null

in_scope("product", line) if line.scope == "product"

in_scope("aggregate", _)

cents(scope, scenario) := round(100 * sum([line_cost(line, scenario) |
	some line in input.lines
	in_scope(scope, line)
]))

usd(amount) := sprintf("%d.%02d", [floor(amount / 100), amount - (floor(amount / 100) * 100)])

deny contains msg if {
	some scope, limit in limits
	some scenario in scenarios
	total := cents(scope, scenario)
	total > limit * 100
	msg := sprintf(
		"cost_over_limit scope=%s scenario=%s total_usd=%s limit_usd=%s",
		[scope, scenario, usd(total), usd(limit * 100)],
	)
}

deny contains msg if {
	some line in input.lines
	line.id == "waf_dedicated"
	not waf_gated(line)
	msg := "waf_unbound_exposure line=waf_dedicated reason=activation_gate_incomplete"
}

deny contains msg if {
	input.lines
	not waf_line_present
	msg := "waf_line_missing line=waf_dedicated"
}

waf_line_present if {
	some line in input.lines
	line.id == "waf_dedicated"
}

deny contains msg if {
	some line in input.lines
	line.precondition
	not line.blocking
	msg := sprintf("precondition_without_blocking line=%s", [line.id])
}

deny contains msg if {
	some line in input.lines
	line.precondition
	line.blocking
	line.blocking != "activation"
	cents("product", "max") + round(100 * line.exposure_usd) > limits.product * 100
	msg := sprintf("exposure_requires_activation_block line=%s", [line.id])
}

waf_gated(line) if {
	line.precondition == "cloudfront_free_plan_active"
	line.exposure_usd > 0
	line.blocking == "activation"
	line.evidence == "pending"
}

protected_types := {
	"aws_s3_bucket",
	"aws_dynamodb_table",
	"aws_cognito_user_pool",
	"aws_cognito_user_pool_client",
	"aws_cognito_user_pool_domain",
}

deny contains msg if {
	some rc in changes
	protected_types[rc.type]
	"delete" in rc.change.actions
	msg := sprintf(
		"protected_change type=%s address=%s actions=%s",
		[rc.type, rc.address, concat(",", rc.change.actions)],
	)
}

live_changes contains rc if {
	some rc in changes
	is_object(rc.change.after)
}

is_unknown(rc, key) if object.get(object.get(rc.change, "after_unknown", {}), key, false) == true

bpa_flags := {
	"block_public_acls",
	"block_public_policy",
	"ignore_public_acls",
	"restrict_public_buckets",
}

deny contains resource_msg("bpa_disabled", rc) if {
	some rc in live_changes
	rc.type == "aws_s3_bucket_public_access_block"
	some flag in bpa_flags
	object.get(rc.change.after, flag, null) != true
	not is_unknown(rc, flag)
}

public_acls := {"public-read", "public-read-write", "authenticated-read"}

deny contains resource_msg("public_acl", rc) if {
	some rc in live_changes
	rc.type in {"aws_s3_bucket_acl", "aws_s3_bucket"}
	public_acls[rc.change.after.acl]
}

statements(document) := document.Statement if is_array(document.Statement)

statements(document) := [document.Statement] if is_object(document.Statement)

open_principal(principal) if principal == "*"

open_principal(principal) if {
	is_object(principal)
	principal.AWS == "*"
}

open_principal(principal) if {
	is_object(principal)
	is_array(principal.AWS)
	"*" in principal.AWS
}

has_condition(statement) if {
	is_object(statement.Condition)
	count(statement.Condition) > 0
}

deny contains resource_msg("public_policy", rc) if {
	some rc in live_changes
	rc.type in {"aws_s3_bucket_policy", "aws_s3_bucket"}
	is_string(rc.change.after.policy)
	json.is_valid(rc.change.after.policy)
	some statement in statements(json.unmarshal(rc.change.after.policy))
	statement.Effect == "Allow"
	open_principal(statement.Principal)
	not has_condition(statement)
}

deny contains resource_msg("ingress", rc) if {
	some rc in live_changes
	rc.type in {"aws_security_group", "aws_default_security_group"}
	is_array(rc.change.after.ingress)
	count(rc.change.after.ingress) > 0
}

deny contains resource_msg("ingress", rc) if {
	some rc in live_changes
	rc.type == "aws_vpc_security_group_ingress_rule"
}

deny contains resource_msg("ingress", rc) if {
	some rc in live_changes
	rc.type == "aws_security_group_rule"
	rc.change.after.type == "ingress"
}

paid_cloudfront_types := {
	"aws_cloudfront_monitoring_subscription",
	"aws_cloudfront_realtime_log_config",
}

deny contains resource_msg("paid_cloudfront", rc) if {
	some rc in live_changes
	paid_cloudfront_types[rc.type]
}

deny contains resource_msg("cloudfront_dedicated_ip", rc) if {
	some rc in live_changes
	rc.type == "aws_cloudfront_distribution"
	some certificate in rc.change.after.viewer_certificate
	certificate.ssl_support_method == "vip"
}

deny contains resource_msg("cloudfront_origin_shield", rc) if {
	some rc in live_changes
	rc.type == "aws_cloudfront_distribution"
	some origin in rc.change.after.origin
	some shield in origin.origin_shield
	shield.enabled == true
}

deny contains resource_msg("mutable_image_tag", rc) if {
	some rc in live_changes
	rc.type == "aws_ecr_repository"
	object.get(rc.change.after, "image_tag_mutability", null) != "IMMUTABLE"
	not is_unknown(rc, "image_tag_mutability")
}

digest_pinned(image) if regex.match(`@sha256:[0-9a-f]{64}$`, image)

deny contains resource_msg("image_without_digest", rc) if {
	some rc in live_changes
	rc.type == "aws_ecs_task_definition"
	is_string(rc.change.after.container_definitions)
	json.is_valid(rc.change.after.container_definitions)
	some container in json.unmarshal(rc.change.after.container_definitions)
	not digest_pinned(object.get(container, "image", ""))
}

secret_key_pattern := `(?i)(password|passwd|secret|token|private_key|api_key|secret_key)$`

secret_exempt_keys := {"token_validity_units", "lock_token"}

secret_exempt(_, path) if secret_exempt_keys[path[_]]

secret_exempt(rc, path) if {
	rc.type == "aws_cognito_user_pool_client"
	path == ["client_secret"]
}

deny contains msg if {
	some rc in live_changes
	walk(rc.change.after, [path, value])
	is_string(value)
	value != ""
	key := path[count(path) - 1]
	is_string(key)
	regex.match(secret_key_pattern, key)
	not secret_exempt(rc, path)
	msg := sprintf(
		"secret_literal type=%s address=%s key=%s",
		[rc.type, rc.address, concat(".", [sprintf("%v", [part]) | some part in path])],
	)
}

secret_value_patterns := {
	"private_key": `-----BEGIN [A-Z ]*PRIVATE KEY-----`,
	"aws_access_key": `\b(AKIA|ASIA)[0-9A-Z]{16}\b`,
}

deny contains msg if {
	some rc in live_changes
	walk(rc.change.after, [_, value])
	is_string(value)
	some reason, pattern in secret_value_patterns
	regex.match(pattern, value)
	msg := sprintf("secret_value reason=%s type=%s address=%s", [reason, rc.type, rc.address])
}

ignore_changes_allowed := {"aws_athena_workgroup": ["${state}"]}

deny contains msg if {
	some kind, named in input.resource
	some name, blocks in named
	some block in blocks
	some lifecycle in object.get(block, "lifecycle", [])
	not ignore_changes_allowed[kind] == lifecycle.ignore_changes
	msg := sprintf("ignore_changes_forbidden type=%s name=%s", [kind, name])
}
