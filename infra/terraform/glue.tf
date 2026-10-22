# ---------------------------------------------------------------- catalog: curated tables queryable from Athena / Spectrum
resource "aws_glue_catalog_database" "shopsense" {
  name = replace(local.name, "-", "_")
}

resource "aws_glue_crawler" "curated" {
  name          = "${local.name}-curated"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.shopsense.name

  dynamic "s3_target" {
    for_each = toset(["dim_date", "dim_product", "dim_customer", "dim_fulfillment_center", "dim_promotion",
    "fact_order_item", "fact_inventory_daily", "fact_session", "fact_product_activity", "fact_review"])
    content {
      path = "${local.lake_uri}/curated/${s3_target.value}/"
    }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

# ---------------------------------------------------------------- jobs: the same clean / curate code that runs locally
locals {
  glue_default_arguments = {
    "--LAKE_URI"                         = local.lake_uri
    "--additional-python-modules"        = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.wheel.key}"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
  }
}

resource "aws_glue_job" "step" {
  for_each = toset(["clean", "curate"])

  name              = "${local.name}-${each.key}"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0" # Spark 3.5 - the transformation code avoids Spark-4-only APIs
  worker_type       = "G.1X"
  execution_class   = "STANDARD" # set explicitly: AWS returns it, so leaving it unset causes a perpetual diff
  number_of_workers = var.glue_workers
  timeout           = 60 # minutes
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = merge(local.glue_default_arguments, { "--STEP" = each.key })

  execution_property {
    max_concurrent_runs = 1 # each run builds on the previous lake state
  }
}

# ---------------------------------------------------------------- daily workflow: clean -> curate -> crawl
resource "aws_glue_workflow" "daily" {
  name = "${local.name}-daily"
}

resource "aws_glue_trigger" "start" {
  name          = "${local.name}-start"
  type          = "SCHEDULED"
  schedule      = var.schedule_cron
  workflow_name = aws_glue_workflow.daily.name
  actions {
    job_name = aws_glue_job.step["clean"].name
  }
}

resource "aws_glue_trigger" "after_clean" {
  name          = "${local.name}-after-clean"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.daily.name
  predicate {
    conditions {
      job_name = aws_glue_job.step["clean"].name
      state    = "SUCCEEDED" # a data-quality failure fails clean, so curate never sees unaudited data
    }
  }
  actions {
    job_name = aws_glue_job.step["curate"].name
  }
}

resource "aws_glue_trigger" "after_curate" {
  name          = "${local.name}-after-curate"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.daily.name
  predicate {
    conditions {
      job_name = aws_glue_job.step["curate"].name
      state    = "SUCCEEDED"
    }
  }
  actions {
    crawler_name = aws_glue_crawler.curated.name
  }
}
