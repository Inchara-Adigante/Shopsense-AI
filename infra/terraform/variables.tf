variable "project" {
  description = "Name prefix for every resource."
  type        = string
  default     = "shopsense"
}

variable "environment" {
  description = "Deployment environment (dev, prod, ...)."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "wheel_path" {
  description = "Local path of the built shopsense wheel (pip wheel --no-deps -w dist .)."
  type        = string
  default     = "../../dist/shopsense-0.4.0-py3-none-any.whl"
}

variable "glue_workers" {
  description = "G.1X workers per Glue job (2 is the minimum and plenty for this data volume)."
  type        = number
  default     = 2
}

variable "schedule_cron" {
  description = "When the daily Glue workflow runs (UTC, AWS cron syntax)."
  type        = string
  default     = "cron(15 6 * * ? *)"
}

variable "enable_redshift" {
  description = "Create Redshift Serverless. Off by default: it bills per RPU-hour while queries run."
  type        = bool
  default     = false
}

variable "redshift_base_rpu" {
  description = "Redshift Serverless base capacity (RPUs)."
  type        = number
  default     = 8
}

variable "redshift_daily_rpu_hours_limit" {
  description = "Hard cap on Redshift Serverless compute per day, in RPU-hours."
  type        = number
  default     = 2
}

variable "athena_bytes_scanned_limit" {
  description = "Per-query cutoff for Athena, in bytes (cost guard)."
  type        = number
  default     = 1073741824 # 1 GiB
}

variable "monthly_budget_usd" {
  description = "Monthly AWS budget for alerts."
  type        = number
  default     = 5
}

variable "budget_alert_email" {
  description = "Email address for budget alerts."
  type        = string
}
