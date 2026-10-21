"""Shared SparkSession factory.

Locally this runs Spark in-process (local[*]). On AWS the same transformation
code runs as a Glue job, where Glue provides the session instead.
"""
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "shopsense") -> SparkSession:
    return (
        SparkSession.builder
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")             # all timestamps are UTC
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .getOrCreate()
    )
