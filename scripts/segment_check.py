from pyspark.sql import functions as F

from src import config
from src.spark_jobs.silver_cleaning import base_id
from src.spark_jobs.spark_session import build_spark

TOL = 100  # tolérance en mètres (bruit de projection, arrêt hors tracé)

spark = build_spark("segment-check")

silver = spark.read.parquet(str(config.SILVER_CLEANED_DIR))
offsets = (
    spark.read.parquet(str(config.STOP_OFFSETS_PATH))
    .withColumn(
        "seg_len_m",
        F.coalesce(F.col("next_gap_m"), F.col("shape_length_m") - F.col("s0_m")),
    )
    .groupBy("lineid", "direction", "stop_base")
    .agg(F.max("seg_len_m").alias("seg_len_m"))
)

j = (
    silver.withColumn("stop_base", base_id(F.col("pointId")))
    .join(F.broadcast(offsets), ["lineid", "direction", "stop_base"], "left")
)

n_silver = silver.count()
stats = j.agg(
    F.count("*").alias("total"),
    F.sum(F.col("seg_len_m").isNull().cast("int")).alias("no_segment"),
    F.sum((F.col("distance_m") > F.col("seg_len_m") + TOL).cast("int")).alias("beyond"),
).collect()[0]

print(f"Silver : {n_silver:,} | après join : {stats['total']:,} (doivent être égaux)")
print(f"Sans tronçon connu : {stats['no_segment']:,} ({100 * stats['no_segment'] / n_silver:.2f} %)")
print(f"Au-delà du tronçon (+{TOL} m) : {stats['beyond']:,} ({100 * stats['beyond'] / n_silver:.2f} %)")

print("--- Lignes avec le plus de dépassements ---")
(
    j.filter(F.col("distance_m") > F.col("seg_len_m") + TOL)
    .groupBy("lineid").count().orderBy(F.desc("count")).show(10)
)
spark.stop()