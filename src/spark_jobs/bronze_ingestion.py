import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.spark_jobs.spark_session import build_spark


def read_raw(spark: SparkSession, input_dir: str) -> DataFrame:
    """Lit les fichiers bruts, une ligne = une chaîne de texte (colonne 'value')."""
    pattern = (Path(input_dir) / "*.jsonl.gz").as_posix()
    return spark.read.text(pattern)


def add_metadata(df: DataFrame) -> DataFrame:
    """Ajoute source_file, ingestion_ts, ingestion_date."""
    return   (df
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("ingestion_date", F.to_date("ingestion_ts")))                           

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ingestion Bronze (fichiers bruts -> Parquet)")
    parser.add_argument("--input-dir", default=str(config.RAW_DIR))
    parser.add_argument("--output", default=str(config.BRONZE_DIR))
    args = parser.parse_args(argv)

    spark = build_spark("bronze-ingestion")
    try:
        bronze = add_metadata(read_raw(spark, args.input_dir))
        bronze.write.mode("overwrite").parquet(args.output)
        n = spark.read.parquet(args.output).count()
        print(f"[bronze] {n:,} lignes écrites dans {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()