import pytest
from pyspark.sql import functions as F

from src.spark_jobs.spark_session import build_spark
from src.spark_jobs.silver_cleaning import base_id, classify, parse_bronze, resolve_direction





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
def test_resolve_direction_cascade(spark):
    valid = spark.createDataFrame(
        [
            ("12", "9600", "p1"),  # terminus exact -> City
            ("12", "7777", "p2"),  # arrêt à sens unique -> Suburb
            ("12", "7777", "p3"),  # arrêt à deux sens -> quarantaine
            ("12", "7777", "p4"),  # arrêt absent du catalogue -> quarantaine
            ("12", "9600", "p5"),  # terminus ET arrêt : le terminus doit gagner
        ],
        ["lineid", "directionId", "pointId"],
    )
    terminus = spark.createDataFrame(
        [("12", "9600", "City")], ["t_lineid", "terminus_base", "dir_terminus"]
    )
    stop_dirs = spark.createDataFrame(
        [
            ("12", "p2", 1, "Suburb"),
            ("12", "p3", 2, "City"),
            ("12", "p5", 1, "Suburb"),
        ],
        ["lineid", "stop_base", "n_dir", "dir_stop"],
    )

    resolved, unresolved = resolve_direction(valid, terminus, stop_dirs)
    res = {r["pointId"]: (r["direction"], r["direction_method"]) for r in resolved.collect()}

    assert res["p1"] == ("City", "EXACT_TERMINUS")
    assert res["p2"] == ("Suburb", "STOP_UNIQUE_DIRECTION")
    assert res["p5"] == ("City", "EXACT_TERMINUS")
    assert sorted(r["pointId"] for r in unresolved.collect()) == ["p3", "p4"]
    assert resolved.count() + unresolved.count() == 5