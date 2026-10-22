from src import config
from src.spark_jobs.silver_cleaning import parse_bronze
from src.spark_jobs.spark_session import build_spark

spark = build_spark("silver-try")

parsed = parse_bronze(spark.read.parquet(str(config.BRONZE_DIR)))

parsed.printSchema()
parsed.show(5, truncate=False)
print("Lignes :", parsed.count())
print("Lignes malformées :", parsed.filter("is_malformed").count())

spark.stop()