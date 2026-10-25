from pyspark.sql import functions as F

from src.spark_jobs.silver_tracking import add_tracklets


def test_service_day_does_not_split_at_midnight_and_skips_unlocated(spark):
    rows = [
        ("1", "City", "2026-05-04 21:50:00", 100.0),  # 23:50 heure locale
        ("1", "City", "2026-05-04 22:10:00", 200.0),  # 00:10 le lendemain
        ("1", "City", "2026-05-04 22:10:30", None),   # sans position : exclue
    ]
    located = (
        spark.createDataFrame(rows, ["lineid", "direction", "ts", "s_m"])
        .withColumn("event_ts", F.col("ts").cast("timestamp"))
        .drop("ts")
    )
    out = add_tracklets(located).collect()

    assert len(out) == 2
    assert len({r["service_date"] for r in out}) == 1