from pyspark.sql import functions as F

from src import config
from src.spark_jobs.silver_tracking import add_tracklets
from src.spark_jobs.spark_session import build_spark

spark = build_spark("tracking-metrics")

located = spark.read.parquet(str(config.SILVER_LOCATED_DIR))
tracked = add_tracklets(located)

per_tracklet = (
    tracked.groupBy("tracklet_id")
    .agg(
        F.count("*").alias("n_obs"),
        (F.max("ts_s") - F.min("ts_s")).alias("dur_s"),
        (F.max("s_m") - F.min("s_m")).alias("progress_m"),
    )
)

n_tracked = tracked.count()
print(f"Observations suivies : {n_tracked:,}")

per_tracklet.agg(
    F.count("*").alias("n_tracklets"),
    F.round(100 * F.avg((F.col("n_obs") == 1).cast("int")), 2).alias("singletons_pct"),
    F.round(100 * F.avg((F.col("n_obs") < 10).cast("int")), 2).alias("short_lt10_pct"),
    F.round(100 * F.avg(((F.col("n_obs") >= 10) & (F.col("progress_m") < 100)).cast("int")), 2).alias("zero_progress_pct"),
    F.percentile_approx("n_obs", [0.5, 0.95]).alias("n_obs_p50_p95"),
    F.percentile_approx("dur_s", [0.5, 0.95]).alias("dur_s_p50_p95"),
).show(truncate=False)

spark.stop()