"""
The lake on S3 must give exactly the same warehouse as the lake on local disk.

Runs the full pipeline twice in subprocesses (Spark needs its S3A connector loaded at JVM start):
once with SHOPSENSE_LAKE=s3://... against a local S3-compatible server (moto), once on local
disk, then compares every warehouse table row by row.

Opt-in, because the first run downloads Hadoop's S3A connector (~1.3 GB) from Maven Central:
    SHOPSENSE_S3_TESTS=1 pytest tests/test_s3_lake.py
"""
import os
import socket
import subprocess
import sys

import duckdb
import pytest

from shopsense.warehouse.load import TABLES

pytestmark = pytest.mark.skipif(not os.getenv("SHOPSENSE_S3_TESTS"), reason="set SHOPSENSE_S3_TESTS=1 to run")
DAYS = "20"


@pytest.fixture(scope="module")
def s3_endpoint():
    moto_server = pytest.importorskip("moto.server")
    boto3 = pytest.importorskip("boto3")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=port)
    server.start()
    endpoint = f"http://127.0.0.1:{port}"
    boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="test", aws_secret_access_key="test",
                 region_name="us-east-1").create_bucket(Bucket="shopsense-lake")
    yield endpoint
    server.stop()


def run_pipeline(env_overrides: dict) -> subprocess.CompletedProcess:
    env = {**os.environ, "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
           "AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-east-1", **env_overrides}
    return subprocess.run([sys.executable, "-m", "shopsense.pipeline", "--days", DAYS], env=env,
                          capture_output=True, text=True, timeout=1800)


def test_s3_lake_matches_local_lake(s3_endpoint, tmp_path):
    s3 = run_pipeline({"SHOPSENSE_LAKE": "s3://shopsense-lake/lake", "SHOPSENSE_S3_ENDPOINT": s3_endpoint,
                       "SHOPSENSE_LANDING": str(tmp_path / "landing_s3"),
                       "SHOPSENSE_WAREHOUSE": str(tmp_path / "s3.duckdb")})
    assert s3.returncode == 0, s3.stderr[-3000:]
    local = run_pipeline({"SHOPSENSE_LAKE": str(tmp_path / "lake"), "SHOPSENSE_LANDING": str(tmp_path / "landing"),
                          "SHOPSENSE_WAREHOUSE": str(tmp_path / "local.duckdb")})
    assert local.returncode == 0, local.stderr[-3000:]

    con = duckdb.connect(str(tmp_path / "s3.duckdb"), read_only=True)
    con.execute(f"ATTACH '{tmp_path / 'local.duckdb'}' AS loc (READ_ONLY)")
    for t in [f"dw.{t}" for t in TABLES] + ["mart.restock_this_week"]:
        diff = con.execute(f"SELECT (SELECT COUNT(*) FROM (SELECT * FROM {t} EXCEPT ALL SELECT * FROM loc.{t})) + "
                           f"(SELECT COUNT(*) FROM (SELECT * FROM loc.{t} EXCEPT ALL SELECT * FROM {t}))").fetchone()[0]
        assert diff == 0, f"{t}: {diff} rows differ"
    assert con.execute("SELECT COUNT(*) FROM dw.fact_order_item").fetchone()[0] > 0
