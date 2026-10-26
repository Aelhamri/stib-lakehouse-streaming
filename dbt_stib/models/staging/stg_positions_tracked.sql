select * exclude (value)
from read_parquet(
    '{{ var("silver_root") }}/positions_tracked/*/*.parquet',
    hive_partitioning = true
)