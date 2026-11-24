import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def build_spark(app_name: str) -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if os.name == "nt":
        hadoop_home = os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        os.environ["PATH"] = hadoop_home + r"\bin;" + os.environ["PATH"]

    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "4g")

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    shuffle = os.environ.get("SPARK_SHUFFLE_PARTITIONS")
    if shuffle:
        builder = builder.config("spark.sql.shuffle.partitions", shuffle)

    return configure_spark_with_delta_pip(builder).getOrCreate()