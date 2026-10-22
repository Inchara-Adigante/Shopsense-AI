# ---------------------------------------------------------------- Athena: pay-per-query SQL over the curated zone
resource "aws_athena_workgroup" "shopsense" {
  name = local.name
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = var.athena_bytes_scanned_limit
    result_configuration {
      output_location = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"
    }
  }
  force_destroy = true
}

# ---------------------------------------------------------------- Redshift Serverless (optional, off by default)
resource "aws_redshiftserverless_namespace" "shopsense" {
  count                 = var.enable_redshift ? 1 : 0
  namespace_name        = local.name
  db_name               = "shopsense"
  admin_username        = "shopsense_admin"
  manage_admin_password = true # password generated and stored in Secrets Manager, never in Terraform state
  iam_roles             = [aws_iam_role.redshift[0].arn]
  default_iam_role_arn  = aws_iam_role.redshift[0].arn
}

resource "aws_redshiftserverless_workgroup" "shopsense" {
  count               = var.enable_redshift ? 1 : 0
  namespace_name      = aws_redshiftserverless_namespace.shopsense[0].namespace_name
  workgroup_name      = local.name
  base_capacity       = var.redshift_base_rpu
  publicly_accessible = false
}

resource "aws_redshiftserverless_usage_limit" "daily_compute" {
  count         = var.enable_redshift ? 1 : 0
  resource_arn  = aws_redshiftserverless_workgroup.shopsense[0].arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_daily_rpu_hours_limit
  period        = "daily"
  breach_action = "deactivate" # stop queries rather than keep billing
}

# ---------------------------------------------------------------- budget alert
resource "aws_budgets_budget" "monthly" {
  name         = "${local.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:project$${var.project}"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}
