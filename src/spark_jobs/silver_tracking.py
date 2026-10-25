from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
import argparse 
from src.spark_jobs.spark_session import build_spark


from src import config
from src.tracking import build_tracklets

SERVICE_DAY_OFFSET_HOURS = 4


def add_tracklets(located: DataFrame) -> DataFrame:
    """Ajoute `tracklet_id` aux observations localisées (celles qui ont un s_m)."""
    tracked_input = (
        located.filter(F.col("s_m").isNotNull())
        .withColumn("ts_s", F.col("event_ts").cast("double"))
        .withColumn(
            "service_date",
            F.to_date(
                F.from_utc_timestamp(F.col("event_ts"), config.TIMEZONE)
                - F.expr(f"INTERVAL {SERVICE_DAY_OFFSET_HOURS} HOURS")
            ),
        )
    )

    out_schema = T.StructType(
        tracked_input.schema.fields + [T.StructField("tracklet_id", T.StringType())]
    )

    def track(pdf):
        pdf = pdf.reset_index(drop=True)
        local_ids = build_tracklets(pdf[["ts_s", "s_m"]])
        prefix = f"{pdf['lineid'].iloc[0]}-{pdf['direction'].iloc[0]}-{pdf['service_date'].iloc[0]}"
        return pdf.assign(tracklet_id=[f"{prefix}-{i}" for i in local_ids])

    return tracked_input.groupBy("lineid", "direction", "service_date").applyInPandas(track, out_schema)

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(config.SILVER_LOCATED_DIR))
    parser.add_argument("--output", default=str(config.SILVER_TRACKED_DIR))
    args = parser.parse_args(argv)

    spark = build_spark("silver-tracking")
    try:
        tracked = add_tracklets(spark.read.parquet(args.input))
        tracked.write.mode("overwrite").partitionBy("service_date").parquet(args.output)
        n = spark.read.parquet(args.output).count()
        print(f"Observations suivies : {n:,}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()