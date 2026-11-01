output "operator_topic_arn" {
  description = "Operator SNS topic ARN: the action of every alarm except the Athena cutoff."
  value       = local.operator_topic_arn
}

output "alarm_names" {
  description = "Alarm names created by this module."
  value = sort(concat(
    [for alarm in aws_cloudwatch_metric_alarm.simple : alarm.alarm_name],
    [for alarm in aws_cloudwatch_metric_alarm.math : alarm.alarm_name],
    [aws_cloudwatch_metric_alarm.system_errors.alarm_name],
  ))
}
