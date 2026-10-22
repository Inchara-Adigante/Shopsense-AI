"""
Storage abstraction: the lake can live on local disk or in S3 with the same code.

    SHOPSENSE_LAKE=/path/to/lake            -> pathlib.Path
    SHOPSENSE_LAKE=s3://my-bucket/lake      -> upath.UPath (pathlib API over fsspec/s3fs)

Everything outside this module keeps using the pathlib API (`/`, glob, exists,
read_text, ...). The few operations pathlib can't do the same way on both -
recursive delete, atomic writes, appends, uploading a local file, and handing a
path to Spark or DuckDB - go through the helpers below.

S3 settings (all optional; on AWS the default credential chain is used):
    SHOPSENSE_S3_ENDPOINT   custom endpoint, e.g. http://127.0.0.1:5055 for a local
                            S3-compatible server (moto, MinIO); unset for real AWS S3
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION   standard AWS variables
"""
import os
import shutil
from pathlib import Path

LakePath = Path   # type alias for annotations; at runtime a UPath for s3:// lakes


def is_remote(path) -> bool:
    return "://" in str(path) and not str(path).startswith("file://")


def s3_storage_options() -> dict:
    opts: dict = {}
    if endpoint := os.getenv("SHOPSENSE_S3_ENDPOINT"):
        opts["client_kwargs"] = {"endpoint_url": endpoint}
    return opts


def lake_path(uri: str | os.PathLike):
    """Path for a local location, UPath for s3://..."""
    if is_remote(uri):
        from upath import UPath
        return UPath(str(uri), **s3_storage_options())
    return Path(uri)


def spark_uri(path) -> str:
    """Spark/Hadoop reads S3 through the s3a:// connector."""
    s = str(path)
    return "s3a://" + s[len("s3://"):] if s.startswith("s3://") else s


def rmtree(path) -> None:
    if is_remote(path):
        if path.exists():
            path.fs.rm(path.path, recursive=True)
    else:
        shutil.rmtree(path, ignore_errors=True)


def write_text_atomic(path, text: str) -> None:
    """Local: write a temp file then rename (never half-written). S3: a PUT is already atomic."""
    if is_remote(path):
        path.write_text(text)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    tmp.replace(path)


def append_line(path, line: str) -> None:
    """Append one line to a log file. S3 has no append: read, add, rewrite (fine for small audit logs)."""
    if is_remote(path):
        existing = path.read_text() if path.exists() else ""
        path.write_text(existing + line + "\n")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(line + "\n")


def copy_in(local_file: Path, dest) -> None:
    """Copy a local file into the lake (upload for S3)."""
    if is_remote(dest):
        dest.fs.put_file(str(local_file), dest.path)
    else:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_file, dest)


def register_duckdb(con, path) -> None:
    """Let DuckDB read s3:// paths through the same fsspec filesystem (no httpfs extension needed)."""
    if is_remote(path):
        import fsspec
        if con.filesystem_is_registered("s3"):            # one registration per connection
            return
        con.register_filesystem(fsspec.filesystem("s3", **s3_storage_options()))


def spark_s3_conf() -> dict[str, str]:
    """Spark settings for an s3a:// lake. On AWS Glue the platform provides these - set SHOPSENSE_ON_GLUE=1."""
    if os.getenv("SHOPSENSE_ON_GLUE") or not is_remote(os.getenv("SHOPSENSE_LAKE", "")):
        return {}
    import glob

    import pyspark
    jars = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api-*.jar"))
    hadoop_version = os.path.basename(jars[0]).removeprefix("hadoop-client-api-").removesuffix(".jar") if jars else "3.4.1"
    conf = {
        "spark.jars.packages": f"org.apache.hadoop:hadoop-aws:{hadoop_version}",   # must match Spark's Hadoop
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }
    if endpoint := os.getenv("SHOPSENSE_S3_ENDPOINT"):
        conf.update({
            "spark.hadoop.fs.s3a.endpoint": endpoint,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
        })
    if os.getenv("AWS_ACCESS_KEY_ID"):
        conf.update({"spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
                     "spark.hadoop.fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY", "")})
    return conf
