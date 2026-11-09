locals {
  demo_seed_actions = ["dynamodb:GetItem", "dynamodb:PutItem"]
}

resource "aws_iam_role" "demo_seed" {
  count = var.demo_seed == null ? 0 : 1

  name                 = "${var.name_prefix}-demo-seed"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.demo_seed.trusted_principal_arn }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "sts:RoleSessionName" = "demo-seed" }
      }
    }]
  })
}

resource "aws_iam_role_policy" "demo_seed" {
  count = var.demo_seed == null ? 0 : 1

  name = "${var.name_prefix}-demo-seed"
  role = aws_iam_role.demo_seed[0].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = local.demo_seed_actions
      Resource = try(var.runtime_grants.table_arn, "")
      Condition = {
        "ForAllValues:StringEquals" = {
          "dynamodb:LeadingKeys" = [var.demo_seed.tenant_partition_key]
        }
        Null = { "dynamodb:LeadingKeys" = "false" }
      }
    }]
  })
}
