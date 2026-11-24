import sys

from delta import DeltaTable

from src.spark_jobs.spark_session import build_spark

spark = build_spark("delta-inspect")
path = sys.argv[1]

DeltaTable.forPath(spark, path).history().select(
    "version", "operation", "operationMetrics"
).show(truncate=False)
print("lignes :", spark.read.format("delta").load(path).count())
spark.stop()