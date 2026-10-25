import shapely
import argparse 
from pathlib import Path 
from src.geometry import build_shapes_m
from src.spark_jobs.spark_session import build_spark

from pyproj import Transformer
from pyspark.sql import types as T

from src import config
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.spark_jobs.silver_cleaning import base_id

TOL_M = 100  # tolérance (bruit de projection, arrêt hors tracé)


def load_offsets(spark, path) -> DataFrame:
    """Une ligne par (ligne, direction, arrêt de base), avec la longueur du tronçon suivant."""
    w = Window.partitionBy("lineid", "direction", "stop_base").orderBy(F.col("seg_len_m").desc())
    return (
        spark.read.parquet(str(path))
        .withColumn(
            "seg_len_m",
            F.coalesce(F.col("next_gap_m"), F.col("shape_length_m") - F.col("s0_m")),
        )
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("lineid", "direction", "stop_base", "s0_m", "seg_len_m", "snap_m", "shape_source")
    )


def locate_on_segment(silver: DataFrame, offsets: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Calcule s = s0 + distance. Met en quarantaine ce qui dépasse le tronçon."""
    j = (
        silver.withColumn("stop_base", base_id(F.col("pointId")))
        .join(F.broadcast(offsets), ["lineid", "direction", "stop_base"], "left")
    )

    beyond = (
    F.col("seg_len_m").isNotNull()
    & (F.col("shape_source") == "OWN")
    & (F.col("distance_m") > F.col("seg_len_m") + TOL_M)
)

    quarantine = (
        j.filter(beyond)
        .drop("stop_base", "s0_m", "seg_len_m", "snap_m")
        .withColumn("rejection_reason", F.lit("BEYOND_SEGMENT"))
        .withColumn("quarantine_stage", F.lit("silver2_geolocation"))
        .withColumn("quarantined_at", F.current_timestamp())
    )

    positioned = (
        j.filter(~beyond)
        .withColumn("s_m", F.col("s0_m") + F.col("distance_m"))
                .withColumn(
            "geo_status",
            F.when(F.col("seg_len_m").isNull(), "NO_SEGMENT")
            .when(F.col("shape_source") == "OWN", "LOCATABLE")
            .otherwise("LOCATABLE_APPROX"),
        )
    )
    return positioned, quarantine
def add_coordinates(positioned: DataFrame, shapes_m: dict) -> DataFrame:
    """Place chaque observation sur son tracé : x_m, y_m (Lambert 72) et lon, lat (WGS84)."""
    with_s = positioned.filter(F.col("s_m").isNotNull())
    without_s = positioned.filter(F.col("s_m").isNull())

    out_schema = T.StructType(
        with_s.schema.fields
        + [T.StructField(n, T.DoubleType()) for n in ("x_m", "y_m", "lon", "lat")]
    )

    def interpolate(pdf):
        line = shapes_m[(pdf["lineid"].iloc[0], pdf["direction"].iloc[0])]
        s = pdf["s_m"].clip(lower=0, upper=line.length).to_numpy()
        pts = shapely.line_interpolate_point(line, s)
        x, y = shapely.get_x(pts), shapely.get_y(pts)
        lon, lat = Transformer.from_crs(
            config.CRS_METRIC, config.CRS_WGS84, always_xy=True
        ).transform(x, y)
        return pdf.assign(x_m=x, y_m=y, lon=lon, lat=lat)

    located = with_s.groupBy("lineid", "direction").applyInPandas(interpolate, out_schema)
    return located.unionByName(without_s, allowMissingColumns=True)

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", default=str(config.SILVER_CLEANED_DIR))
    parser.add_argument("--offsets", default=str(config.STOP_OFFSETS_PATH))
    parser.add_argument("--static-dir", default=str(config.STATIC_DIR))
    parser.add_argument("--output", default=str(config.SILVER_LOCATED_DIR))
    parser.add_argument("--quarantine", default=str(config.SILVER_GEO_QUARANTINE_DIR))
    args = parser.parse_args(argv)

    spark = build_spark("silver-geolocation")
    try:
        silver = spark.read.parquet(args.silver)
        offsets = load_offsets(spark, args.offsets)
        positioned, quarantine = locate_on_segment(silver, offsets)
        located = add_coordinates(positioned, build_shapes_m(Path(args.static_dir)))

        located.write.mode("overwrite").partitionBy("event_date").parquet(args.output)
        quarantine.write.mode("overwrite").parquet(args.quarantine)

        n_loc = spark.read.parquet(args.output).count()
        n_q = spark.read.parquet(args.quarantine).count()
        print(f"Localisées : {n_loc:,} | Quarantaine : {n_q:,} | Somme : {n_loc + n_q:,}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()