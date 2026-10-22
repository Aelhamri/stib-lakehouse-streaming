import os
import sys

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    # Les processus Python lancés par Spark doivent utiliser le même Python que nous.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Windows : Spark a besoin de winutils.exe et hadoop.dll.
    if os.name == "nt":
        hadoop_home = os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        os.environ["PATH"] = hadoop_home + r"\bin;" + os.environ["PATH"]

    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )