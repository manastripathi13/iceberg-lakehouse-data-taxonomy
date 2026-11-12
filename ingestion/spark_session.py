"""Build a SparkSession wired to the Iceberg catalog served by Nessie.

`ref` picks the Nessie branch. Writing to a branch other than `main` means the
data is invisible to everyone else until the branch is merged (write-audit-publish).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.10.0"
PACKAGES = ",".join(
    [
        f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    ]
)


def get_spark(ref: str = "main", app: str = "lakehouse") -> SparkSession:
    nessie = os.getenv("NESSIE_URI", "http://localhost:19120/iceberg")
    s3 = os.getenv("S3_ENDPOINT", "http://localhost:8333")
    key = os.getenv("S3_ACCESS_KEY", "lakeadmin")
    secret = os.getenv("S3_SECRET_KEY", "lakeadmin-secret")
    builder = (
        SparkSession.builder.appName(app)
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "rest")
        .config("spark.sql.catalog.lake.uri", f"{nessie}/{ref}")
        .config("spark.sql.catalog.lake.warehouse", "warehouse")
        .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lake.s3.endpoint", s3)
        .config("spark.sql.catalog.lake.s3.path-style-access", "true")
        .config("spark.sql.catalog.lake.s3.access-key-id", key)
        .config("spark.sql.catalog.lake.s3.secret-access-key", secret)
        .config("spark.sql.catalog.lake.client.region", "us-east-1")
        .config("spark.sql.defaultCatalog", "lake")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
