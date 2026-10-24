from src import config
from src.spark_jobs.silver_cleaning import parse_bronze, classify, load_known_ids 
from src.spark_jobs.spark_session import build_spark
from pyspark.sql import functions as F
spark = build_spark("silver-try")

parsed = parse_bronze(spark.read.parquet(str(config.BRONZE_DIR)))



known_lines, known_stops = load_known_ids(spark, config.STATIC_DIR)
valid, quarantine = classify(parsed, known_lines, known_stops)

n_total = parsed.count()
n_valid = valid.count()
n_quar = quarantine.count()
print(f"Total       : {n_total:,}")
print(f"Valides     : {n_valid:,}")
print(f"Quarantaine : {n_quar:,}")
print(f"Somme       : {n_valid + n_quar:,}  (doit égaler le total)")
quarantine.groupBy("rejection_reason").count().orderBy("count", ascending=False).show()















print("--- Distribution de distance_m (lignes valides) ---")
valid.select("distance_m").describe().show()
print("Quantiles 50 / 90 / 99 / 99.9 / 100 % :",
      valid.approxQuantile("distance_m", [0.5, 0.9, 0.99, 0.999, 1.0], 0.001))
print("Distances négatives :", valid.filter("distance_m < 0").count())

print("--- Lignes avec distance > 2000 m, par ligne de transport ---")
(
    valid.filter("distance_m > 2000")
    .groupBy("lineid").count().orderBy(F.desc("count")).show(10)
)
spark.stop()