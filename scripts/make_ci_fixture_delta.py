"""Convertit la fixture Parquet de la CI en table Delta (petite : ~1,7 Mo)."""
from src.spark_jobs.spark_session import build_spark

SRC = "tests/fixtures/silver/positions_tracked"
DST = "tests/fixtures/silver_delta/positions_tracked"

spark = build_spark("make_ci_fixture_delta")
df = spark.read.parquet(SRC)
(df.coalesce(1)
   .write.format("delta")
   .mode("overwrite")
   .partitionBy("service_date")
   .save(DST))
print("lignes :", spark.read.format("delta").load(DST).count())
spark.stop()