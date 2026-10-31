locals {
  ecs_trust_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "aws:SourceAccount" = var.account_id }
      }
    }]
  })
}

resource "aws_iam_role" "task" {
  for_each = toset(["unit", "recovery", "audit"])

  name               = "${var.name_prefix}-processor-${each.key}"
  assume_role_policy = local.ecs_trust_policy
}

resource "aws_iam_role" "execution" {
  name               = "${var.name_prefix}-processor-execution"
  assume_role_policy = local.ecs_trust_policy
}

resource "aws_iam_role_policy" "execution" {
  name = "${var.name_prefix}-processor-pull-logs"
  role = aws_iam_role.execution.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchCheckLayerAvailability",
        ]
        Resource = var.repository_arn
      },
      {
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${trimsuffix(var.log_group_arn, ":*")}:log-stream:*"
      },
    ]
  })
}
