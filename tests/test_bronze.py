import pytest
from pyspark.sql import functions as F

from src.spark_jobs.bronze_ingestion import add_metadata
from src.spark_jobs.spark_session import build_spark


@pytest.fixture(scope="session")
def spark():
    session = build_spark("tests")
    yield session
    session.stop()


def test_add_metadata_adds_traceability_columns(spark, tmp_path):
    fichier = tmp_path / "mini.jsonl"
    fichier.write_text('{"a": 1}\n{"a": 2}\n', encoding="utf-8")

    df = spark.read.text(fichier.as_posix())
    out = add_metadata(df)

    assert set(out.columns) == {"value", "source_file", "ingestion_ts", "ingestion_date"}
    assert out.count() == 2

    # 1) aucune ligne sans fichier source
    assert out.filter(F.col("source_file").isNull()).count() == 0

    # 2) le texte brut d'origine est conservé tel quel
    valeurs = sorted(r.value for r in out.select("value").collect())
    assert valeurs == ['{"a": 1}', '{"a": 2}']