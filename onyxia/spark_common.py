"""Session Spark commune (labs 07 et 09). Parametrable par variables d'environnement."""
import os
from pyspark.sql import SparkSession

ICEBERG_PACKAGE = os.environ.get("ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0")
EXTRA_PACKAGES = os.environ.get("EXTRA_SPARK_PACKAGES", "")  # ex. hadoop-aws hors cluster
BUCKET = os.environ["LAB_BUCKET_NAME"]
ENDPOINT = os.environ.get("S3_ENDPOINT_URL") or f"https://{os.environ['AWS_S3_ENDPOINT']}"
PROVIDER = os.environ.get("S3A_CREDENTIALS_PROVIDER", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

def session(app, polaris_credential=None):
    b = (
        SparkSession.builder.appName(app).master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "us-east-1"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", PROVIDER)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", ",".join(p for p in [ICEBERG_PACKAGE, EXTRA_PACKAGES] if p))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    )
    if polaris_credential:
        b = (
            b.config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.polaris.type", "rest")
            .config("spark.sql.catalog.polaris.uri", os.environ["POLARIS_URI"] + "/api/catalog")
            .config("spark.sql.catalog.polaris.warehouse", os.environ["POLARIS_CATALOG"])
            .config("spark.sql.catalog.polaris.credential", polaris_credential)
            .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
            .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        )
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s
