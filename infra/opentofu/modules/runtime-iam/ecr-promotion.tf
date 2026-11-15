locals {
  ecr_promotion_enabled = var.ecr_promotion != null
  ecr_promotion_oidc    = "token.actions.githubusercontent.com"
  ecr_promotion_trust = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = "arn:aws:iam::${var.account_id}:oidc-provider/${local.ecr_promotion_oidc}" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${local.ecr_promotion_oidc}:aud" = "sts.amazonaws.com"
          "${local.ecr_promotion_oidc}:sub" = format("repo:%s:environment:%s",
          try(var.ecr_promotion.repository, ""), try(var.ecr_promotion.environment, ""))
          "${local.ecr_promotion_oidc}:job_workflow_ref" = format("%s/%s@%s",
            try(var.ecr_promotion.repository, ""), try(var.ecr_promotion.workflow_path, ""),
          try(var.ecr_promotion.ref, ""))
        }
      }
    }]
  })
  ecr_promotion_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:BatchGetImage", "ecr:DescribeImages",
          "ecr:DescribeRepositories", "ecr:GetDownloadUrlForLayer",
        ]
        Resource = var.repository_arn
      },
      {
        Effect    = "Allow"
        Action    = "ecr:GetAuthorizationToken"
        Resource  = "*"
        Condition = { StringEquals = { "aws:RequestedRegion" = "us-east-2" } }
      },
    ]
  })
}

resource "aws_iam_role" "ecr_promotion" {
  count = local.ecr_promotion_enabled ? 1 : 0

  name                 = "${var.name_prefix}-ecr-promotion"
  assume_role_policy   = local.ecr_promotion_trust
  max_session_duration = 3600
  tags                 = var.tags
}

resource "aws_iam_role_policy" "ecr_promotion" {
  count = local.ecr_promotion_enabled ? 1 : 0

  name   = "${var.name_prefix}-ecr-promotion-runtime"
  role   = aws_iam_role.ecr_promotion[0].name
  policy = local.ecr_promotion_policy
}
