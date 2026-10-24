from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
import argparse
from src.spark_jobs.spark_session import build_spark
from src import config
from pathlib import Path

PAYLOAD_SCHEMA = T.StructType([
    T.StructField("poll_ts", T.StringType()),
    T.StructField("lineid", T.StringType()),
    T.StructField("directionId", T.StringType()),
    T.StructField("pointId", T.StringType()),
    T.StructField("distanceFromPoint", T.DoubleType()),
])


def parse_bronze(bronze: DataFrame) -> DataFrame:
    """Décode le JSON brut. Ne supprime rien : marque les lignes illisibles."""
    parsed = bronze.withColumn("j", F.from_json("value", PAYLOAD_SCHEMA))
    event_ts = F.col("j.poll_ts").try_cast("timestamp")

    is_malformed = (
        F.col("j").isNull()
        | event_ts.isNull()
        | F.col("j.lineid").isNull()
        | F.col("j.directionId").isNull()
        | F.col("j.pointId").isNull()
        | F.col("j.distanceFromPoint").isNull()
    )

    # Empreinte du contenu. Ce n'est PAS un identifiant unique : plusieurs véhicules
    # peuvent avoir exactement les mêmes valeurs (ex. à l'arrêt, distance 0).
    obs_key = F.sha2(
        F.concat_ws(
            "|",
            F.col("j.poll_ts"), F.col("j.lineid"), F.col("j.directionId"),
            F.col("j.pointId"), F.col("j.distanceFromPoint").cast("string"),
        ),
        256,
    )

    return parsed.select(
        obs_key.alias("obs_key"),
        event_ts.alias("event_ts"),
        F.to_date(F.from_utc_timestamp(event_ts, config.TIMEZONE)).alias("event_date"),
        F.col("j.lineid").alias("lineid"),
        F.col("j.directionId").alias("directionId"),
        F.col("j.pointId").alias("pointId"),
        F.col("j.distanceFromPoint").alias("distance_m"),
        is_malformed.alias("is_malformed"),
        "source_file",
        "ingestion_ts",
        "value",
    )

def base_id(col):
    """Retire la lettre de quai finale d'un identifiant d'arrêt (4263F -> 4263)."""
    return F.regexp_replace(col, "[A-Za-z]+$", "")

def load_known_ids(spark, static_dir: Path) -> tuple[DataFrame, DataFrame]:
    """Petites tables de référence : lignes connues et arrêts connus (identifiants de base)."""
    stops = spark.read.json(str(static_dir / "stops.jsonl.gz"))
    line_stops = spark.read.json(str(static_dir / "line_stops.jsonl.gz"))
    terminus = spark.read.json(str(static_dir / "line_terminus.jsonl.gz"))

    known_stops = (
        stops.select(base_id("pointid").alias("stop_base")).distinct()
        .withColumn("_known_stop", F.lit(True))
    )
    known_lines = (
        line_stops.select("lineid").union(terminus.select("lineid")).distinct()
        .withColumn("_known_line", F.lit(True))
    )
    return known_lines, known_stops

def classify(parsed: DataFrame, known_lines: DataFrame, known_stops: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Sépare les lignes en (valides, quarantaine). Chaque ligne a au plus UN motif de rejet."""
    d = (
        parsed.withColumn("stop_base", base_id("pointId"))
        .join(F.broadcast(known_lines), "lineid", "left")
        .join(F.broadcast(known_stops), "stop_base", "left")
    )

    reason = (
        F.when(F.col("is_malformed"), "MALFORMED_PAYLOAD")
        .when(F.col("_known_line").isNull(), "UNKNOWN_LINE")
        .when(F.col("_known_stop").isNull(), "UNKNOWN_STOP")
        .when(
    (F.col("distance_m") < 0) | (F.col("distance_m") > config.MAX_DISTANCE_M),
    "EXTREME_DISTANCE",
)
    )
    d = d.withColumn("rejection_reason", reason)

    helper_cols = ["stop_base", "_known_line", "_known_stop", "rejection_reason"]
    valid = d.filter(F.col("rejection_reason").isNull()).drop(*helper_cols)

    quarantine = (
        d.filter(F.col("rejection_reason").isNotNull())
        .drop("stop_base", "_known_line", "_known_stop")
        .withColumn("quarantine_stage", F.lit("silver1_cleaning"))
        .withColumn("quarantined_at", F.current_timestamp())
    )
    return valid, quarantine
def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", default=str(config.BRONZE_DIR))
    parser.add_argument("--static-dir", default=str(config.STATIC_DIR))
    parser.add_argument("--output", default=str(config.SILVER_CLEANED_DIR))
    parser.add_argument("--quarantine", default=str(config.SILVER_QUARANTINE_DIR))
    args = parser.parse_args(argv)

    spark = build_spark("silver-cleaning")
    try:
        bronze = spark.read.parquet(args.bronze)
        known_lines, known_stops = load_known_ids(spark, Path(args.static_dir))
        valid, quarantine = classify(parse_bronze(bronze), known_lines, known_stops)

        valid.write.mode("overwrite").partitionBy("event_date").parquet(args.output)
        quarantine.write.mode("overwrite").parquet(args.quarantine)

        n_valid = spark.read.parquet(args.output).count()
        n_quar = spark.read.parquet(args.quarantine).count()
        print(f"Valides: {n_valid:,} | Quarantaine: {n_quar:,} | Somme: {n_valid + n_quar:,}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()