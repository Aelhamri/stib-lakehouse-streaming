from src.spark_jobs.spark_session import build_spark
from src.config import RAW_DIR

spark = build_spark("test")
print(spark.version, RAW_DIR)
spark.stop()