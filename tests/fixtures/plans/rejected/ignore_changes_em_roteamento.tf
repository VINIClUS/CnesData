resource "aws_scheduler_schedule" "recovery" {
  name                = "example-recovery"
  schedule_expression = "rate(30 minutes)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:ecs:us-east-2:123456789012:cluster/example"
    role_arn = "arn:aws:iam::123456789012:role/example"
  }

  lifecycle {
    ignore_changes = [target]
  }
}
