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
	msg := "waf_unbound_exposure line=waf_dedicated reason=missing_precondition_or_exposure"
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
}
