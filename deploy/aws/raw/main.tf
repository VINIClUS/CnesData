terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "sa-east-1"
}

locals {
  environments = toset(["dev", "prod"])
  account_id   = "836651842853"
}

data "aws_caller_identity" "current" {}

check "expected_account" {
  assert {
    condition     = data.aws_caller_identity.current.account_id == local.account_id
    error_message = "aws_account_mismatch"
  }
}

resource "aws_dynamodb_table" "raw" {
  for_each     = local.environments
  name         = "cnesdata-raw-${each.key}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }

  dynamic "attribute" {
    for_each = range(1, 7)
    content {
      name = "gsi${attribute.value}pk"
      type = "S"
    }
  }
  dynamic "attribute" {
    for_each = range(1, 7)
    content {
      name = "gsi${attribute.value}sk"
      type = "S"
    }
  }
  dynamic "global_secondary_index" {
    for_each = range(1, 7)
    content {
      name            = "gsi${global_secondary_index.value}"
      hash_key        = "gsi${global_secondary_index.value}pk"
      range_key       = "gsi${global_secondary_index.value}sk"
      projection_type = "ALL"
    }
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

resource "aws_s3_bucket" "raw" {
  for_each      = local.environments
  bucket        = "cnesdata-raw-${each.key}-${local.account_id}"
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  for_each                = local.environments
  bucket                  = aws_s3_bucket.raw[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  for_each = local.environments
  bucket   = aws_s3_bucket.raw[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  for_each = local.environments
  bucket   = aws_s3_bucket.raw[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_iam_user" "raw" {
  for_each = local.environments
  name     = "cnesdata-raw-${each.key}"
}

resource "aws_iam_user_policy" "raw" {
  for_each = local.environments
  name     = "cnesdata-raw-${each.key}"
  user     = aws_iam_user.raw[each.key].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeTable", "dynamodb:GetItem", "dynamodb:PutItem",
          "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query",
          "dynamodb:Scan", "dynamodb:TransactWriteItems"
        ]
        Resource = [
          aws_dynamodb_table.raw[each.key].arn,
          "${aws_dynamodb_table.raw[each.key].arn}/index/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.raw[each.key].arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = ["${aws_s3_bucket.raw[each.key].arn}/*"]
      }
    ]
  })
}

resource "aws_iam_access_key" "raw" {
  for_each = local.environments
  user     = aws_iam_user.raw[each.key].name
}

output "raw_access_key_ids" {
  value     = { for env, key in aws_iam_access_key.raw : env => key.id }
  sensitive = true
}

output "raw_secret_access_keys" {
  value     = { for env, key in aws_iam_access_key.raw : env => key.secret }
  sensitive = true
}
