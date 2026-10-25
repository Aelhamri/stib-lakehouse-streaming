from pyspark.sql import functions as F

from src import config
from src.spark_jobs.silver_cleaning import base_id
from src.spark_jobs.spark_session import build_spark

TOL = 100
KEYS = ["lineid", "direction", "stop_base"]

spark = build_spark("segment-diag")

silver = spark.read.parquet(str(config.SILVER_CLEANED_DIR))
offsets = (
    spark.read.parquet(str(config.STOP_OFFSETS_PATH))
    .withColumn(
        "seg_len_m",
        F.coalesce(F.col("next_gap_m"), F.col("shape_length_m") - F.col("s0_m")),
    )
    .groupBy(*KEYS)
    .agg(F.max("seg_len_m").alias("seg_len_m"))
)
in_catalog = (
    spark.read.json(str(config.STATIC_DIR / "line_stops.jsonl.gz"))
    .select("lineid", "direction", base_id(F.col("pointid")).alias("stop_base"))
    .distinct()
    .withColumn("in_line_stops", F.lit(True))
)

j = (
    silver.withColumn("stop_base", base_id(F.col("pointId")))
    .join(F.broadcast(offsets), KEYS, "left")
    .join(F.broadcast(in_catalog), KEYS, "left")
)

print("--- Sans tronçon : pourquoi ? ---")
no_seg = j.filter(F.col("seg_len_m").isNull())
(
    no_seg.withColumn(
        "cause",
        F.when(F.col("in_line_stops").isNull(), "A: arrêt absent de line_stops pour cette direction")
        .otherwise("B: dans line_stops mais perdu à la géométrie"),
    )
    .groupBy("cause").count().show(truncate=False)
)
print("--- Lignes les plus touchées ---")
no_seg.groupBy("lineid").count().orderBy(F.desc("count")).show(10)

print("--- Dépassements : de combien dépasse-t-on le tronçon ? (m) ---")
beyond = j.filter(F.col("distance_m") > F.col("seg_len_m") + TOL).withColumn(
    "excess_m", F.col("distance_m") - F.col("seg_len_m")
)
beyond.select(
    F.percentile_approx("excess_m", [0.1, 0.5, 0.9, 0.99]).alias("q10_q50_q90_q99")
).show(truncate=False)

spark.stop()