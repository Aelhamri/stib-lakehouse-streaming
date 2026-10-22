from src import config
from src.spark_jobs.silver_cleaning import parse_bronze, deduplicate
from src.spark_jobs.spark_session import build_spark
from pyspark.sql import functions as F

spark = build_spark("silver-try")

bronze = spark.read.parquet(str(config.BRONZE_DIR))
parsed = parse_bronze(bronze)

parsed.printSchema()
parsed.show(5, truncate=False)

deduped = deduplicate(parsed)
print("Avant dédup :", parsed.count())
print("Après dédup :", deduped.count())

dup = (
    parsed.groupBy("event_id")
    .agg(F.count("*").alias("n"), F.first("distance_m").alias("dist"))
    .filter("n > 1")
)
total_dup = dup.count()
print("Clés dupliquées :", total_dup)
dup.groupBy("n").count().orderBy("n").show()
print("Part avec distance 0 :", dup.filter("dist = 0").count() / total_dup)

spark.stop()
spark.stop()