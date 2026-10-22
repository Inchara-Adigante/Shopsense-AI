"""
AWS Glue job entry point: runs one ShopSense lake step on Glue's managed Spark.

Job parameters (set by Terraform in infra/terraform/glue.tf):
    --STEP            clean | curate
    --LAKE_URI        s3://<lake-bucket>/lake
    --additional-python-modules   s3://<scripts-bucket>/wheels/shopsense-<version>-py3-none-any.whl

The transformation code is the same that runs locally; only the SparkSession
comes from Glue, and S3 access uses Glue's built-in S3 support (no S3A setup).
"""
import os
import sys

from awsglue.context import GlueContext          # provided by the Glue runtime
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "STEP", "LAKE_URI"])
os.environ["SHOPSENSE_LAKE"] = args["LAKE_URI"]
os.environ["SHOPSENSE_ON_GLUE"] = "1"              # skip local S3A settings; Glue provides S3 access

sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glue)
job.init(args["JOB_NAME"], args)

from shopsense.config import get_paths            # noqa: E402  (after SHOPSENSE_LAKE is set)
from shopsense.transform import clean, curate     # noqa: E402

lake = get_paths().lake
if args["STEP"] == "clean":
    report = clean.run(spark, lake)
    print({t: {k: v for k, v in s.items() if k != "affected_partitions"} for t, s in report.tables.items()})
elif args["STEP"] == "curate":
    print(curate.run(spark, lake))
else:
    raise ValueError(f"unknown STEP {args['STEP']}")
job.commit()
