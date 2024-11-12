import os
from pyspark.sql import SparkSession
from delta import *


def init_spark(app_name):
    postgres_driver = os.getenv("POSTGRES_DRIVER")
    builder = SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.jars", postgres_driver) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()