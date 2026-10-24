import pytest
from pyspark.sql import functions as F

from src.spark_jobs.spark_session import build_spark
from src.spark_jobs.silver_cleaning import base_id, classify, parse_bronze


@pytest.fixture(scope="module")
def spark():
    s = build_spark("test-silver")
    yield s
    s.stop()


def payload(line="12", point="4263F", dist=100.0):
    return (
        '{"poll_ts":"2026-05-04T10:00:00Z","lineid":"%s","directionId":"9600",'
        '"pointId":"%s","distanceFromPoint":%s}' % (line, point, dist)
    )


def run(spark, values):
    bronze = (
        spark.createDataFrame([(v, "f.gz") for v in values], ["value", "source_file"])
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    known_lines = spark.createDataFrame([("12",)], ["lineid"]).withColumn("_known_line", F.lit(True))
    known_stops = spark.createDataFrame([("4263",)], ["stop_base"]).withColumn("_known_stop", F.lit(True))
    return classify(parse_bronze(bronze), known_lines, known_stops)


def test_valid_row_is_kept(spark):
    valid, quarantine = run(spark, [payload()])
    assert valid.count() == 1
    assert quarantine.count() == 0


def test_each_rejection_reason(spark):
    valid, quarantine = run(spark, [
        payload(),                       # valide
        "pas du json",                   # MALFORMED_PAYLOAD
        payload(line="99"),              # UNKNOWN_LINE
        payload(point="9999"),           # UNKNOWN_STOP
        payload(dist=6000.0),            # EXTREME_DISTANCE
        payload(dist=-5.0),              # EXTREME_DISTANCE
    ])
    reasons = sorted(r[0] for r in quarantine.select("rejection_reason").collect())
    assert valid.count() == 1
    assert reasons == [
        "EXTREME_DISTANCE", "EXTREME_DISTANCE",
        "MALFORMED_PAYLOAD", "UNKNOWN_LINE", "UNKNOWN_STOP",
    ]


def test_nothing_is_lost(spark):
    values = [payload(), "pas du json", payload(line="99"), payload(dist=6000.0)]
    valid, quarantine = run(spark, values)
    assert valid.count() + quarantine.count() == len(values)


def test_base_id_strips_trailing_letters(spark):
    df = spark.createDataFrame([("4263F",), ("0636",)], ["p"]).select(base_id(F.col("p")).alias("b"))
    assert [r[0] for r in df.collect()] == ["4263", "0636"]