# scripts/direction_try.py
from pyspark.sql import functions as F

from src import config
from src.spark_jobs.spark_session import build_spark
from src.spark_jobs.silver_cleaning import base_id

spark = build_spark("direction-try")

valid = spark.read.parquet(str(config.SILVER_CLEANED_DIR))

terminus = (
    spark.read.json(str(config.STATIC_DIR / "line_terminus.jsonl.gz"))
    .select(
        "lineid",
        base_id(F.col("terminus_pointid")).alias("terminus_base"),
        F.col("direction").alias("direction_resolved"),
    )
    .distinct()
)

joined = valid.join(
    F.broadcast(terminus),
    (valid["lineid"] == terminus["lineid"])
    & (valid["directionId"] == terminus["terminus_base"]),
    "left",
).drop(terminus["lineid"])

total = valid.count()
total_after = joined.count()
print(f"Avant join : {total:,} | Après join : {total_after:,} (doivent être égaux)")

resolved = joined.filter(F.col("direction_resolved").isNotNull()).count()
print(f"Résolues : {resolved:,} ({100 * resolved / total:.2f} %)")
print(f"Non résolues : {total - resolved:,} ({100 * (total - resolved) / total:.2f} %)")

print("--- Top 15 directionId non résolus ---")
(
    joined.filter(F.col("direction_resolved").isNull())
    .groupBy("lineid", "directionId")
    .count()
    .orderBy(F.desc("count"))
    .show(15, truncate=False)
)
line_stops = (
    spark.read.json(str(config.STATIC_DIR / "line_stops.jsonl.gz"))
    .select("lineid", base_id(F.col("pointid")).alias("stop_base"), "direction")
    .distinct()
)

stop_dir = (
    line_stops.groupBy("lineid", "stop_base")
    .agg(F.countDistinct("direction").alias("n_dir"), F.first("direction").alias("dir_unique"))
)

unresolved = (
    joined.filter(F.col("direction_resolved").isNull())
    .withColumn("stop_base", base_id(F.col("pointId")))
    .join(F.broadcast(stop_dir), ["lineid", "stop_base"], "left")
)

print("--- Parmi les non résolus : l'arrêt donne-t-il un sens unique ? ---")
unresolved.groupBy(
    F.when(F.col("n_dir").isNull(), "arrêt absent de line_stops")
     .when(F.col("n_dir") == 1, "sens unique")
     .otherwise("sens ambigu (2 sens)").alias("cas")
).count().show(truncate=False)
spark.stop()