# Deploying ShopSense on AWS

Everything runs locally by default. This folder deploys the batch lake to AWS with the **same code**:

| Local | AWS (this Terraform) |
|---|---|
| `lake/` folder | **S3** lake bucket (versioned, encrypted, public access blocked, lifecycle rules) |
| PySpark `clean` / `curate` | **AWS Glue 5.0** jobs (Spark 3.5) running `infra/glue/shopsense_job.py` + the `shopsense` wheel |
| Airflow daily DAG | **Glue workflow**: scheduled `clean` → `curate` (only if clean succeeded) → crawler |
| DuckDB queries over curated Parquet | **Glue Data Catalog + Athena** workgroup (per-query scan limit) |
| DuckDB warehouse | **Redshift Serverless** (optional, off by default) + `sql/redshift/create_tables.sql` |
| n/a | **AWS Budgets** alert (default $5 / month) |

## Verification status
- `terraform fmt` and `terraform validate` pass (Terraform 1.9.8, AWS provider 5.80).
- `terraform apply` was run against **moto**, a local AWS emulator. **22 of 23 resources were created**: S3 buckets with encryption, versioning, lifecycle and public-access blocks; the uploaded Glue script and wheel; the IAM role and policy; the Glue database, crawler, both jobs, the workflow and 3 triggers; and the Athena workgroup.
  - A second `plan` was clean apart from the gaps below. It also caught a perpetual diff (`execution_class`), now fixed.
  - Not applicable on moto, which doesn't emulate these: the AWS-managed `AWSGlueServiceRole` policy attachment, AWS Budgets notifications, and Redshift Serverless.
- The S3 code path is tested end to end: `tests/test_s3_lake.py` runs the whole pipeline with `SHOPSENSE_LAKE=s3://…` against an S3-compatible server and gets a warehouse **identical** to the local run.
- **Not yet done:** deploying to a real AWS account and running the Glue jobs there. The transformation code avoids Spark-4-only APIs so it can run on Glue 5.0 (Spark 3.5), but that hasn't been executed on Glue.

## Deploy

Prerequisites: an AWS account, AWS CLI credentials (`aws configure` or SSO), and Terraform ≥ 1.6.

```bash
# 1. build the package the Glue jobs import
pip wheel --no-deps -w dist .

# 2. create the infrastructure
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars      # set budget_alert_email
terraform init
terraform plan
terraform apply

# 3. land the data in S3 and run the pipeline against it
export SHOPSENSE_LAKE=$(terraform output -raw lake_uri)
cd ../..
python -m shopsense.generate.retail_data          # local landing area
python -m shopsense.ingest.land_raw               # uploads raw deliveries to s3://.../lake/raw/
aws glue start-workflow-run --name $(terraform -chdir=infra/terraform output -raw glue_workflow)

# 4. query the curated zone (after the crawler has run)
aws athena start-query-execution --work-group shopsense-dev \
  --query-string "SELECT category, SUM(net_amount) FROM fact_order_item JOIN dim_product USING (product_key) GROUP BY 1" \
  --query-execution-context Database=shopsense_dev
```

Redshift (optional): set `enable_redshift = true`, then `terraform apply`. Run `sql/redshift/create_tables.sql` in the Redshift query editor, using the `redshift_copy_role_arn` output for `COPY`. The admin password is generated and kept in Secrets Manager (`manage_admin_password`).

**Tear down:** `terraform destroy`. The lake bucket is versioned, so empty it first: `aws s3 rm s3://<bucket> --recursive` plus old versions, or add `force_destroy`.

## What it costs
These are rough us-east-1 list prices; check the AWS pricing pages.

| Item | Driver | This project |
|---|---|---|
| S3 | about $0.023/GB-month | under $0.01/month for about 80 MB |
| Glue | about $0.44 per DPU-hour, 1-minute minimum per run | 2 G.1X workers × 2 short jobs ≈ **$0.05–0.15 per daily run** |
| Athena | about $5 per TB scanned | fractions of a cent (Parquet + partitions, 1 GiB cutoff per query) |
| Redshift Serverless | about $0.375 per RPU-hour while queries run | off by default; when on, capped at 2 RPU-hours/day by a usage limit |
| Budget alert | free | emails at 80% of actual and 100% of forecast spend |

The guardrails are the reason this is safe to try on a personal account: Redshift is off by default, Redshift has a daily usage limit, Athena has a scan cutoff, staging files and Athena results expire, and the $5 budget alert is set from day one.
