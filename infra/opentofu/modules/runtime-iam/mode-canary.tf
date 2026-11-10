locals {
  mode_aliases = ["mode_seed", "mode_dispatch", "mode_audit_launch", "mode_observe"]
  mode_operations = {
    promotion = {
      unit-seed        = ["us", "mode_seed", "unit"]
      unit-start       = ["ux", "mode_dispatch", "unit"]
      unit-observe     = ["uo", "mode_observe", "unit"]
      recovery-seed    = ["rs", "mode_seed", "recovery"]
      recovery-start   = ["rx", "mode_dispatch", "recovery"]
      recovery-observe = ["ro", "mode_observe", "recovery"]
      audit-start      = ["ax", "mode_audit_launch", "audit"]
      audit-observe    = ["ao", "mode_observe", "audit"]
    }
    readiness = {
      drill-seed           = ["ds", "mode_seed", "readiness"]
      drill-unit-start     = ["dx", "mode_dispatch", "readiness"]
      drill-recovery-start = ["dr", "mode_dispatch", "readiness"]
      drill-hang-first     = ["h1", "mode_dispatch", "readiness"]
      drill-hang-second    = ["h2", "mode_dispatch", "readiness"]
      drill-observe        = ["do", "mode_observe", "readiness"]
    }
  }
  mode_session_base = try(merge({
    for key in [
      "product_source_sha", "product_source_closure_sha256", "shared_source_sha",
      "shared_source_closure_sha256", "release_id", "activation_sha256", "context_kind",
    ] : key => local.mode_manifest[key]
    }, local.mode_manifest.owner, {
    schema_version          = 2
    context_manifest_sha256 = var.mode_canary.context_manifest.sha256
  }), {})
  mode_sessions = try({
    for alias in local.mode_aliases : alias => sort([
      for operation, entry in local.mode_operations[var.mode_canary.context_kind] : format(
        "mc-%s-%s", entry[0], substr(sha256("${replace(replace(replace(
          jsonencode(merge(local.mode_session_base, { operation = operation, phase = entry[2] })),
        "\\u003c", "<"), "\\u003e", ">"), "\\u0026", "&")}\n"), 0, 40)
      ) if entry[1] == alias
    ])
  }, {})
  mode_enabled   = var.mode_canary != null
  mode_input     = local.mode_enabled ? var.mode_canary : null
  mode_resources = try(local.mode_input.resources, null)
  mode_names = {
    for alias in local.mode_aliases : alias => "${var.name_prefix}-${replace(alias, "_", "-")}"
  }
  mode_expiry  = { DateLessThan = { "aws:CurrentTime" = try(local.mode_input.enabled_until, "") } }
  mode_cluster = { ArnEquals = { "ecs:cluster" = try(local.mode_resources.cluster_arn, "") } }
  mode_ascii_hex = zipmap(split("", "0123456789abcdefghijklmnopqrstuvwxyz-"), [
    for code in concat(range(48, 58), range(97, 123), [45]) : format("%02x", code)
  ])
  mode_tenant_hex = try(join("", [
    for char in split("", local.mode_resources.scope.tenant_id) : local.mode_ascii_hex[char]
  ]), "")
  mode_partition_keys = try(concat(["TENANT#${local.mode_tenant_hex}"], [
    for run in local.mode_resources.scope.run_ids : format(
      "TENANT#%s#RUN#%s", local.mode_tenant_hex,
      join("", [for char in split("", run) : local.mode_ascii_hex[char]])
    )
  ]), [])
  mode_ddb_rules = {
    Seed = { actions = ["GetItem", "PutItem"], keys = local.mode_partition_keys }
    Dispatch = {
      actions = ["GetItem", "Query", "PutItem", "UpdateItem", "ConditionCheckItem"]
      keys    = local.mode_partition_keys
    }
    Fence     = { actions = ["GetItem", "ConditionCheckItem"], keys = ["ENV#prod#FENCE"] }
    Semaphore = { actions = ["GetItem", "UpdateItem"], keys = ["ENV#prod#SEMAPHORE"] }
    Attempts  = { actions = ["UpdateItem"], keys = ["ENV#prod#ATTEMPTS"] }
    Observe   = { actions = ["GetItem", "Query"], keys = local.mode_partition_keys }
    Cursor    = { actions = ["GetItem"], keys = ["OUTBOX#CURSOR"] }
  }
  mode_ddb_statements = { for name, rule in local.mode_ddb_rules : name => {
    Sid      = name
    Effect   = "Allow"
    Action   = [for action in rule.actions : "dynamodb:${action}"]
    Resource = try(local.mode_resources.table_arn, "")
    Condition = merge(local.mode_expiry, {
      "ForAllValues:StringEquals" = { "dynamodb:LeadingKeys" = rule.keys }
      Null                        = { "dynamodb:LeadingKeys" = "false" }
    })
  } }
  mode_launch_statements = { for mode in ["recovery", "audit"] : mode => [
    {
      Sid       = "Launch"
      Effect    = "Allow"
      Action    = ["ecs:RunTask"]
      Resource  = try(local.mode_resources.revisions[mode], "")
      Condition = merge(local.mode_expiry, local.mode_cluster)
    },
    {
      Sid    = "Roles"
      Effect = "Allow"
      Action = ["iam:PassRole"]
      Resource = try([
        local.mode_resources.task_role_arns[mode], local.mode_resources.execution_role_arn,
      ], [])
      Condition = merge(local.mode_expiry, {
        StringEquals = { "iam:PassedToService" = "ecs-tasks.amazonaws.com" }
      })
    },
  ] }
  mode_task_aliases = ["mode_dispatch", "mode_audit_launch", "mode_observe"]
  mode_task_statements = { for alias in local.mode_task_aliases : alias => {
    Sid    = "Tasks"
    Effect = "Allow"
    Action = alias == "mode_dispatch" ? [
      "ecs:DescribeTasks", "ecs:StopTask",
    ] : ["ecs:DescribeTasks"]
    Resource  = try("${replace(local.mode_resources.cluster_arn, ":cluster/", ":task/")}/*", "")
    Condition = merge(local.mode_expiry, local.mode_cluster)
  } }
  mode_machine_statements = { for alias in ["mode_dispatch", "mode_observe"] : alias => [
    {
      Sid    = "Machine"
      Effect = "Allow"
      Action = alias == "mode_dispatch" ? [
        "states:DescribeStateMachine", "states:StartExecution",
      ] : ["states:DescribeStateMachine"]
      Resource  = try(local.mode_resources.machine_arn, "")
      Condition = local.mode_expiry
    },
    {
      Sid    = "Executions"
      Effect = "Allow"
      Action = alias == "mode_dispatch" ? [
        "states:DescribeExecution", "states:StopExecution",
      ] : ["states:DescribeExecution"]
      Resource = try(format(
        "%s:*", replace(local.mode_resources.machine_arn, ":stateMachine:", ":execution:")
      ), "")
      Condition = local.mode_expiry
    },
  ] }
  mode_object_rules = {
    Raw = {
      actions = ["s3:PutObject", "s3:GetObject"]
      bucket  = try(local.mode_resources.data_bucket_arn, "")
      keys    = try(local.mode_resources.scope.raw_object_keys, [])
    }
    Outputs = {
      actions = ["s3:GetObject"]
      bucket  = try(local.mode_resources.data_bucket_arn, "")
      keys    = try(local.mode_resources.scope.output_object_keys, [])
    }
    Audit = {
      actions = ["s3:GetObject", "s3:GetObjectRetention"]
      bucket  = try(local.mode_resources.audit_bucket_arn, "")
      keys    = try(local.mode_resources.scope.audit_object_keys, [])
    }
  }
  mode_object_statements = { for name, rule in local.mode_object_rules : name => {
    Sid       = name
    Effect    = "Allow"
    Action    = rule.actions
    Resource  = [for key in rule.keys : "${rule.bucket}/${key}"]
    Condition = local.mode_expiry
  } }
  mode_observer_reads = [
    { actions = ["ecs:DescribeTaskDefinition"], resources = ["*"] },
    {
      actions   = ["scheduler:GetSchedule"]
      resources = try(values(local.mode_resources.schedule_arns), [])
    },
    {
      actions   = ["cloudwatch:DescribeAlarms"]
      resources = try(values(local.mode_resources.alarm_arns), [])
    },
    {
      actions = ["logs:GetLogEvents"]
      resources = try([
        for mode in ["unit", "recovery", "audit"] :
        "${local.mode_resources.log_group_arn}:log-stream:${mode}/processor/*"
      ], [])
    },
  ]
  mode_readiness_reads = try(local.mode_input.context_kind == "readiness", false) ? [
    {
      Effect    = "Allow"
      Action    = ["ecs:ListTasks"]
      Resource  = "*"
      Condition = merge(local.mode_expiry, local.mode_cluster)
    },
    {
      Effect    = "Allow"
      Action    = ["cloudwatch:GetMetricData"]
      Resource  = "*"
      Condition = local.mode_expiry
    },
  ] : []
  mode_statements = {
    mode_seed = [local.mode_ddb_statements.Seed, local.mode_object_statements.Raw]
    mode_dispatch = concat(
      [for name in ["Dispatch", "Fence", "Semaphore", "Attempts"] :
        local.mode_ddb_statements[name]
      ],
      local.mode_launch_statements.recovery, [local.mode_task_statements.mode_dispatch],
      local.mode_machine_statements.mode_dispatch,
    )
    mode_audit_launch = concat(
      local.mode_launch_statements.audit, [local.mode_task_statements.mode_audit_launch],
    )
    mode_observe = concat(
      [local.mode_ddb_statements.Observe, local.mode_ddb_statements.Cursor],
      [local.mode_object_statements.Outputs, local.mode_object_statements.Audit],
      [local.mode_task_statements.mode_observe], local.mode_machine_statements.mode_observe,
      [for rule in local.mode_observer_reads : {
        Effect    = "Allow"
        Action    = rule.actions
        Resource  = rule.resources
        Condition = local.mode_expiry
      }], local.mode_readiness_reads,
    )
  }
  mode_closed = {
    for alias in local.mode_aliases : alias =>
    alias == "mode_audit_launch" && try(local.mode_input.context_kind == "readiness", false)
  }
  mode_closed_policy = jsonencode({
    Version = "2012-10-17", Statement = [{ Effect = "Deny", Action = "*", Resource = "*" }]
  })
  mode_policies = { for alias in local.mode_aliases : alias => (
    local.mode_closed[alias] ? local.mode_closed_policy :
    jsonencode({ Version = "2012-10-17", Statement = local.mode_statements[alias] })
  ) }
  mode_trust = { for alias in local.mode_aliases : alias => jsonencode({
    Version = "2012-10-17"
    Statement = [merge({
      Effect    = local.mode_closed[alias] ? "Deny" : "Allow"
      Principal = { AWS = try(local.mode_input.application_principal_arn, "") }
      Action    = "sts:AssumeRole"
      }, local.mode_closed[alias] ? {} : {
      Condition = merge(local.mode_expiry, {
        StringEquals = { "sts:RoleSessionName" = try(local.mode_sessions[alias], []) }
      })
    })]
  }) }
}

resource "aws_iam_role" "mode_seed" {
  count                = local.mode_enabled ? 1 : 0
  name                 = local.mode_names.mode_seed
  max_session_duration = 3600
  assume_role_policy   = local.mode_trust.mode_seed
}

resource "aws_iam_role" "mode_dispatch" {
  count                = local.mode_enabled ? 1 : 0
  name                 = local.mode_names.mode_dispatch
  max_session_duration = 3600
  assume_role_policy   = local.mode_trust.mode_dispatch
}

resource "aws_iam_role" "mode_audit_launch" {
  count                = local.mode_enabled ? 1 : 0
  name                 = local.mode_names.mode_audit_launch
  max_session_duration = 3600
  assume_role_policy   = local.mode_trust.mode_audit_launch
}

resource "aws_iam_role" "mode_observe" {
  count                = local.mode_enabled ? 1 : 0
  name                 = local.mode_names.mode_observe
  max_session_duration = 3600
  assume_role_policy   = local.mode_trust.mode_observe
}

resource "aws_iam_role_policy" "mode_seed" {
  count  = local.mode_enabled ? 1 : 0
  name   = "${local.mode_names.mode_seed}-runtime"
  role   = aws_iam_role.mode_seed[0].name
  policy = local.mode_policies.mode_seed
}

resource "aws_iam_role_policy" "mode_dispatch" {
  count  = local.mode_enabled ? 1 : 0
  name   = "${local.mode_names.mode_dispatch}-runtime"
  role   = aws_iam_role.mode_dispatch[0].name
  policy = local.mode_policies.mode_dispatch
}

resource "aws_iam_role_policy" "mode_audit_launch" {
  count  = local.mode_enabled ? 1 : 0
  name   = "${local.mode_names.mode_audit_launch}-runtime"
  role   = aws_iam_role.mode_audit_launch[0].name
  policy = local.mode_policies.mode_audit_launch
}

resource "aws_iam_role_policy" "mode_observe" {
  count  = local.mode_enabled ? 1 : 0
  name   = "${local.mode_names.mode_observe}-runtime"
  role   = aws_iam_role.mode_observe[0].name
  policy = local.mode_policies.mode_observe
}
