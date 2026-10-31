resource "aws_ecs_cluster" "processor" {
  name = "${var.name_prefix}-processor"

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_sfn_state_machine" "processor" {
  count = var.processor_active_unit_task_definition_arn == null ? 0 : 1

  name     = var.name_prefix
  type     = "STANDARD"
  role_arn = var.step_functions_role_arn
  definition = templatefile("${path.module}/state_machine.asl.json", {
    cluster_arn         = aws_ecs_cluster.processor.arn
    task_definition_arn = var.processor_active_unit_task_definition_arn
    public_subnet_ids   = jsonencode(sort(distinct(var.public_subnet_ids)))
    security_group_id   = var.security_group_id
  })
}
