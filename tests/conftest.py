import pytest

from src.spark_jobs.spark_session import build_spark


@pytest.fixture(scope="session")
def spark():
    s = build_spark("tests")
    s.conf.set("spark.sql.shuffle.partitions", "2")
    s.conf.set("spark.sql.adaptive.enabled", "false")
    yield s
    s.stop()