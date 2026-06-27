"""SparkSession factory wired for Delta Lake + s3a -> MinIO.

The s3a/Delta configs are set here (in addition to the image's spark-defaults.conf)
so the session is correct whether launched via spark-submit in the cluster or as a
plain `python` process in local[*] mode. The s3a *jars* are provided by the Spark
image (baked) — for pure-local runs against MinIO you must run inside the container;
unit tests use local Delta paths and never touch s3a.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.config import settings


def get_spark(app_name: str = "ifood-case") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings.use_ssl).lower())
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
