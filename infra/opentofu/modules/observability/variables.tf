variable "name_prefix" {
  description = "Product namespace for alarm, topic, rule and filter names."
  type        = string
  nullable    = false
}

variable "account_id" {
  description = "Production account ID bound into the operator topic policy."
  type        = string
  nullable    = false
}

variable "table_name" {
  description = "Control-plane DynamoDB table name."
  type        = string
  nullable    = false
}

variable "state_machine_arn" {
  description = "Deterministic Step Functions state machine ARN."
  type        = string
  nullable    = false
}

variable "log_group_name" {
  description = "Processor log group read by the metric filters."
  type        = string
  nullable    = false
}

variable "cluster_arn" {
  description = "Own ECS processor cluster ARN matched by the unit task rules."
  type        = string
  nullable    = false
}

variable "cutoff_function_name" {
  description = "Athena cutoff Lambda function name."
  type        = string
  nullable    = false
}

variable "cutoff_topic_name" {
  description = "Athena cutoff SNS topic name."
  type        = string
  nullable    = false
}
