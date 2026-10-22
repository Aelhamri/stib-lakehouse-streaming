from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src import config

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
    fingerprint = F.sha2(
    F.concat_ws(
        "|",
        F.col("j.poll_ts"), F.col("j.lineid"), F.col("j.directionId"),
        F.col("j.pointId"), F.col("j.distanceFromPoint").cast("string"),
    ),
    256,
    )
   
    event_id = F.when(is_malformed, F.sha2(F.coalesce(F.col("value"), F.lit("")), 256)).otherwise(fingerprint)

    return parsed.select(
        event_id.alias("event_id"),
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
def deduplicate(parsed: DataFrame) -> DataFrame:

    return parsed.dropDuplicates(["event_id"])