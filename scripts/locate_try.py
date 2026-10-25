from src import config
from src.spark_jobs.silver_geolocation import load_offsets, locate_on_segment
from src.spark_jobs.spark_session import build_spark
from pyspark.sql import functions as F

spark = build_spark("locate-try")

silver = spark.read.parquet(str(config.SILVER_CLEANED_DIR))
offsets = load_offsets(spark, config.STOP_OFFSETS_PATH)
positioned, quarantine = locate_on_segment(silver, offsets)

n_silver, n_pos, n_q = silver.count(), positioned.count(), quarantine.count()
print(f"Silver : {n_silver:,} | positionnées : {n_pos:,} | quarantaine : {n_q:,} | somme : {n_pos + n_q:,}")
positioned.groupBy("geo_status").count().show()

ok = positioned.groupBy("shape_source").count().withColumnRenamed("count", "ok")
rejected = quarantine.groupBy("shape_source").count().withColumnRenamed("count", "rejected")
(
    ok.join(rejected, "shape_source", "outer")
    .fillna(0)
    .withColumn("taux_rejet_pct", F.round(100 * F.col("rejected") / (F.col("ok") + F.col("rejected")), 2))
    .show()
)
spark.stop()