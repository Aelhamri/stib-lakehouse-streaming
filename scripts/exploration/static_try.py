from pyspark.sql import functions as F

from src import config
from src.spark_jobs.silver_cleaning import parse_bronze
from src.spark_jobs.spark_session import build_spark


def base(col):
    """Retire la lettre de quai finale d'un identifiant d'arrêt (4263F -> 4263)."""
    return F.regexp_replace(col, "[A-Za-z]+$", "")


spark = build_spark("static-try")
parsed = parse_bronze(spark.read.parquet(str(config.BRONZE_DIR)))
stops = spark.read.json(str(config.STATIC_DIR / "stops.jsonl.gz"))

known_exact = stops.select(F.col("pointid").alias("pointId")).distinct().withColumn("known", F.lit(True))
known_base = stops.select(base("pointid").alias("base")).distinct().withColumn("known", F.lit(True))

total = parsed.count()
unknown_exact = (
    parsed.join(F.broadcast(known_exact), "pointId", "left").filter("known IS NULL").count()
)
unknown_base = (
    parsed.withColumn("base", base("pointId"))
    .join(F.broadcast(known_base), "base", "left").filter("known IS NULL").count()
)

print(f"Total lignes            : {total:,}")
print(f"Arrêt inconnu (exact)   : {unknown_exact:,}  ({unknown_exact / total:.2%})")
print(f"Arrêt inconnu (de base) : {unknown_base:,}  ({unknown_base / total:.2%})")

line_stops = spark.read.json(str(config.STATIC_DIR / "line_stops.jsonl.gz"))
terminus = spark.read.json(str(config.STATIC_DIR / "line_terminus.jsonl.gz"))
known_lines = (
    line_stops.select("lineid").union(terminus.select("lineid")).distinct()
    .withColumn("known_line", F.lit(True))
)

print("--- Arrêts encore inconnus après normalisation (top 10) ---")
(
    parsed.withColumn("base", base("pointId"))
    .join(F.broadcast(known_base), "base", "left").filter("known IS NULL")
    .groupBy("pointId").count().orderBy(F.desc("count")).show(10)
)

print("--- Lignes absentes du catalogue ---")
(
    parsed.join(F.broadcast(known_lines), "lineid", "left").filter("known_line IS NULL")
    .groupBy("lineid").count().orderBy(F.desc("count")).show(10)
)

spark.stop()