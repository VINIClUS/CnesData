terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

locals {
  region         = "us-east-2"
  five_minutes   = 300
  one_week       = 604800
  write_cap_wru  = 25
  sustained_pct  = 0.8
  sustained_runs = 12
  unit_task_cap  = 10

  operator_topic_arn = format(
    "arn:aws:sns:%s:%s:%s", local.region, var.account_id, aws_sns_topic.operator.name,
  )
  alarm_arn_pattern = format(
    "arn:aws:cloudwatch:%s:%s:alarm:%s-*", local.region, var.account_id, var.name_prefix,
  )
  unit_group = "family:${var.name_prefix}-processor-unit"
  namespace  = "CnesData/Processor"

  unit_failure_detail = {
    "$or" = [
      { containers = { exitCode = [{ "anything-but" = 0 }] } },
      { stopCode = ["TaskFailedToStart"] },
    ]
  }
  unit_rules = {
    failed  = local.unit_failure_detail
    stopped = {}
  }

  filters = {
    processor-degraded = {
      metric = "ProcessorDegraded"
      pattern = join(" || ", [
        "{ ($.event = \"processor_execution_probe_failed\")",
        "($.event = \"processor_entrypoint_failed\")",
        "($.event = \"processor_recovery_completed\" && $.failed > 0)",
        "($.event = \"processor_recovery_completed\" && $.reconcile_failed IS TRUE) }",
      ])
    }
    execution-attempts-exhausted = {
      metric  = "ExecutionAttemptsExhausted"
      pattern = "{ $.event = \"dispatch_deferred *reason=monthly_attempts_exhausted\" }"
    }
  }

  dimension_maps = {
    for name, alarm in local.simple_alarms :
    name => alarm.dimension[0] == "" ? null : { (alarm.dimension[0]) = alarm.dimension[1] }
  }

  simple_alarms = {
    ddb-sustained-write = {
      description = "Control-plane writes sustained at 80% of the on-demand write cap for one hour."
      namespace   = "AWS/DynamoDB"
      metric      = "ConsumedWriteCapacityUnits"
      dimension   = ["TableName", var.table_name]
      period      = local.five_minutes
      evaluations = local.sustained_runs
      threshold   = local.sustained_pct * local.write_cap_wru * local.five_minutes
    }
    unit-task-failures = {
      description = "A unit task stopped with a failure."
      namespace   = "AWS/Events"
      metric      = "TriggeredRules"
      dimension   = ["RuleName", aws_cloudwatch_event_rule.unit["failed"].name]
      period      = local.five_minutes
      evaluations = 1
      threshold   = 1
    }
    unit-task-hours-proxy = {
      description = "Weekly unit task count above the 4 h unit reserve pace (monitoring only)."
      namespace   = "AWS/Events"
      metric      = "TriggeredRules"
      dimension   = ["RuleName", aws_cloudwatch_event_rule.unit["stopped"].name]
      period      = local.one_week
      evaluations = 1
      threshold   = local.unit_task_cap
    }
    cutoff-lambda-errors = {
      description = "The Athena cutoff Lambda failed; the workgroup may still be enabled."
      namespace   = "AWS/Lambda"
      metric      = "Errors"
      dimension   = ["FunctionName", var.cutoff_function_name]
      period      = local.five_minutes
      evaluations = 1
      threshold   = 1
    }
    cutoff-sns-delivery-failures = {
      description = "The Athena cutoff topic failed to deliver a notification."
      namespace   = "AWS/SNS"
      metric      = "NumberOfNotificationsFailed"
      dimension   = ["TopicName", var.cutoff_topic_name]
      period      = local.five_minutes
      evaluations = 1
      threshold   = 1
    }
    processor-degraded = {
      description = "Processor recovery or entrypoint reported a failure."
      namespace   = local.namespace
      metric      = "ProcessorDegraded"
      dimension   = ["", ""]
      period      = local.five_minutes
      evaluations = 1
      threshold   = 1
    }
    execution-attempts-exhausted = {
      description = "The monthly Step Functions attempt counter is exhausted."
      namespace   = local.namespace
      metric      = "ExecutionAttemptsExhausted"
      dimension   = ["", ""]
      period      = local.five_minutes
      evaluations = 1
      threshold   = 1
    }
  }

  math_alarms = {
    states-execution-failures = {
      description = "A state machine execution failed, timed out or was aborted."
      namespace   = "AWS/States"
      dimension   = ["StateMachineArn", var.state_machine_arn]
      metrics = {
        failed    = "ExecutionsFailed"
        timed_out = "ExecutionsTimedOut"
        aborted   = "ExecutionsAborted"
      }
    }
    ddb-throttles = {
      description = "Control-plane requests throttled by the on-demand maximum throughput."
      namespace   = "AWS/DynamoDB"
      dimension   = ["TableName", var.table_name]
      metrics = {
        read  = "ReadMaxOnDemandThroughputThrottleEvents"
        write = "WriteMaxOnDemandThroughputThrottleEvents"
      }
    }
  }
}

#trivy:ignore:AVD-AWS-0095:exp:2027-04-11
resource "aws_sns_topic" "operator" {
  name = "${var.name_prefix}-operator-alarms"
}

resource "aws_sns_topic_policy" "operator" {
  arn = local.operator_topic_arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowAlarmPublish"
      Effect    = "Allow"
      Principal = { Service = "cloudwatch.amazonaws.com" }
      Action    = "sns:Publish"
      Resource  = local.operator_topic_arn
      Condition = {
        StringEquals = { "aws:SourceAccount" = var.account_id }
        ArnLike      = { "aws:SourceArn" = local.alarm_arn_pattern }
      }
    }]
  })
}

resource "aws_cloudwatch_event_rule" "unit" {
  for_each = local.unit_rules

  name = "${var.name_prefix}-unit-task-${each.key}"
  event_pattern = jsonencode({
    source        = ["aws.ecs"]
    "detail-type" = ["ECS Task State Change"]
    detail = merge(each.value, {
      lastStatus = ["STOPPED"]
      clusterArn = [var.cluster_arn]
      group      = [local.unit_group]
    })
  })
}

resource "aws_cloudwatch_log_metric_filter" "processor" {
  for_each = local.filters

  name           = "${var.name_prefix}-${each.key}"
  log_group_name = var.log_group_name
  pattern        = each.value.pattern

  metric_transformation {
    name      = each.value.metric
    namespace = local.namespace
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "simple" {
  for_each = local.simple_alarms

  alarm_name          = "${var.name_prefix}-${each.key}"
  alarm_description   = each.value.description
  namespace           = each.value.namespace
  metric_name         = each.value.metric
  dimensions          = local.dimension_maps[each.key]
  statistic           = "Sum"
  period              = each.value.period
  evaluation_periods  = each.value.evaluations
  datapoints_to_alarm = each.value.evaluations
  threshold           = each.value.threshold
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [local.operator_topic_arn]
  depends_on          = [aws_cloudwatch_log_metric_filter.processor]
}

resource "aws_cloudwatch_metric_alarm" "math" {
  for_each = local.math_alarms

  alarm_name          = "${var.name_prefix}-${each.key}"
  alarm_description   = each.value.description
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [local.operator_topic_arn]

  metric_query {
    id          = "total"
    expression  = join(" + ", [for id in keys(each.value.metrics) : "FILL(${id}, 0)"])
    label       = each.key
    return_data = true
  }

  dynamic "metric_query" {
    for_each = each.value.metrics

    content {
      id = metric_query.key

      metric {
        namespace   = each.value.namespace
        metric_name = metric_query.value
        period      = local.five_minutes
        stat        = "Sum"
        dimensions  = { (each.value.dimension[0]) = each.value.dimension[1] }
      }
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "system_errors" {
  alarm_name          = "${var.name_prefix}-ddb-system-errors"
  alarm_description   = "Control-plane DynamoDB returned HTTP 500 on GetItem, Query or UpdateItem."
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [local.operator_topic_arn]

  metric_query {
    id          = "total"
    expression  = "FILL(get_item, 0) + FILL(query, 0) + FILL(update_item, 0)"
    label       = "SystemErrors"
    return_data = true
  }

  dynamic "metric_query" {
    for_each = { get_item = "GetItem", query = "Query", update_item = "UpdateItem" }

    content {
      id = metric_query.key

      metric {
        namespace   = "AWS/DynamoDB"
        metric_name = "SystemErrors"
        period      = local.five_minutes
        stat        = "Sum"
        dimensions  = { TableName = var.table_name, Operation = metric_query.value }
      }
    }
  }
}
