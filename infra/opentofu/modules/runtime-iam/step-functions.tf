resource "aws_iam_role" "step_functions" {
  count = var.state_machine_arn == null ? 0 : 1

  name = "${var.name_prefix}-step-functions"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = {
          "aws:SourceAccount" = var.account_id
          "aws:SourceArn"     = var.state_machine_arn
        }
      }
    }]
  })
}
