from pyspark.sql import functions as F

from src import config
from src.geometry import build_shapes_m
from src.spark_jobs.silver_geolocation import add_coordinates, load_offsets, locate_on_segment
from src.spark_jobs.spark_session import build_spark

spark = build_spark("geo-points-try")

silver = spark.read.parquet(str(config.SILVER_CLEANED_DIR))
offsets = load_offsets(spark, config.STOP_OFFSETS_PATH)
positioned, _ = locate_on_segment(silver, offsets)
located = add_coordinates(positioned, build_shapes_m(config.STATIC_DIR))

print("Total :", f"{located.count():,}", "(attendu 8,475,073)")
(
    located.filter(F.col("geo_status") != "NO_SEGMENT")
    .agg(
        F.min("lat"), F.max("lat"), F.min("lon"), F.max("lon"),
        F.sum(F.col("lat").isNull().cast("int")).alias("lat_nulles"),
    )
    .show()
)
located.select("lineid", "direction", "pointId", "distance_m", "s_m", "lat", "lon").show(5)

spark.stop()