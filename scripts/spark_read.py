import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("bronze-read")
    .getOrCreate()
)

df = spark.read.text("data/raw/*.jsonl.gz")

bronze = (
    df
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("ingestion_date", F.to_date("ingestion_ts"))
)

bronze.printSchema()
bronze.show(3, truncate=False)
bronze.groupBy("source_file").count().orderBy("source_file").show(truncate=False)
print("Nombre de lignes :", bronze.count())
chemin_bronze = "data/lakehouse/bronze/positions"

bronze.write.mode("overwrite").parquet(chemin_bronze)

relu = spark.read.parquet(chemin_bronze)
print("Lignes relues :", relu.count())
spark.stop()