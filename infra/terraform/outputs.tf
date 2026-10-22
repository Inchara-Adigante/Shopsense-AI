output "lake_uri" {
  description = "Set SHOPSENSE_LAKE to this to run the pipeline against AWS."
  value       = local.lake_uri
}

output "glue_jobs" {
  value = { for k, j in aws_glue_job.step : k => j.name }
}

output "glue_workflow" {
  value = aws_glue_workflow.daily.name
}

output "glue_database" {
  value = aws_glue_catalog_database.shopsense.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.shopsense.name
}

output "redshift_workgroup" {
  value = var.enable_redshift ? aws_redshiftserverless_workgroup.shopsense[0].workgroup_name : null
}

output "redshift_copy_role_arn" {
  description = "IAM role for COPY in sql/redshift/create_tables.sql."
  value       = var.enable_redshift ? aws_iam_role.redshift[0].arn : null
}
