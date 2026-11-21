# stib-lakehouse-streaming

A batch medallion pipeline (Bronze / Silver / Gold) that turns one week of raw real-time vehicle positions from the Brussels public transport operator (STIB/MIVB) into three analytical tables: commercial speed, headway regularity and data-collection health.

> **Honest scope note.** Despite the repository name, this is a **batch** project on **Parquet**. There is no Kafka and no Structured Streaming, and no Delta Lake yet (see [Limits and next steps](#limits-and-next-steps)).

## Problem and data

The STIB open-data API returns, for each stop, the next vehicles expected. It was polled every 30 seconds from **4 to 10 May 2026**, which gives about **9.2 million raw rows** (`data/raw/*.jsonl.gz`, one file per day). Static reference data (stops, line stops, line terminus, line shapes) lives in `data/static/`.

The raw feed is not directly usable: some lines and stops are unknown, the direction of travel is missing for many rows, and a vehicle is never identified by a stable id. The pipeline has to clean it, locate each vehicle along its line, rebuild vehicle trajectories, and only then compute indicators.

`data/` is not versioned (data volume).

## Architecture

```mermaid
flowchart LR
    RAW["Raw JSONL.gz<br/>(9.2 M rows)"] --> B["Bronze<br/>raw text + metadata"]
    B --> S1["Silver 1<br/>parse, validate,<br/>resolve direction"]
    S1 --> S2["Silver 2<br/>geolocation on line shapes<br/>(Lambert 72)"]
    S2 --> S3["Silver 3<br/>vehicle tracking<br/>(tracklets)"]
    S1 -. rejected rows .-> Q["Quarantine<br/>+ rejection_reason"]
    S2 -. rejected rows .-> Q
    S3 --> G["Gold (dbt + DuckDB)<br/>3 fct_ tables, 17 tests"]
    G --> E["Parquet / CSV export"]
    E --> PBI["Power BI dashboard"]
    D["Dagster<br/>orchestrates every step"] -.-> B
    D -.-> G
```

| Layer | Tool | What it does |
|---|---|---|
| Bronze | PySpark | Reads raw files as text, adds `source_file` and ingestion timestamps. Nothing is dropped. |
| Silver 1 | PySpark | Parses the JSON, checks lines and stops against the static data, resolves the direction, sends invalid rows to quarantine. |
| Silver 2 | PySpark + pandas UDF | Projects every position onto the line shape (Belgian Lambert 72, metres) to get a curvilinear position `s_m`. |
| Silver 3 | PySpark + pandas UDF | Rebuilds vehicle trajectories ("tracklets") with a greedy curvilinear matching, per line / direction / service day. |
| Gold | dbt Core + DuckDB | 6 models (1 staging view, 2 intermediate tables, 3 marts) and 17 tests. |
| Serving | DuckDB, Parquet, Power BI | `src/export_gold.py` exports the 3 marts for BI. |
| Orchestration | Dagster | One asset per Spark job, dbt models as assets, export as the last asset. |

## Key design choices

- **Quarantine with a rejection reason.** Invalid rows are never deleted: they are written to a separate dataset with `rejection_reason` (`UNKNOWN_LINE`, `UNKNOWN_STOP`, `EXTREME_DISTANCE`, `UNRESOLVED_DIRECTION`, `BEYOND_SEGMENT`), the stage that rejected them and a timestamp. On the 9,213,400 input rows of Silver 1: 9,001,973 valid, 211,427 quarantined. Row counts always add up, and a rule can be revised without re-reading the source.
- **No content-based deduplication.** 3.35 % of rows look like duplicates, but 98 % of them are at distance 0 (several vehicles at the same stop), so they are kept. Details in [docs/findings.md](docs/findings.md).
- **Direction resolution cascade.** First by exact terminus match, then by "this stop belongs to only one direction". Each row records the method used (`direction_method`).
- **Lambert 72 (EPSG:31370).** Distances are computed in a metric projection, not on raw latitude / longitude. Each stop-to-stop segment gets its real length, which replaced a provisional 5,000 m threshold.
- **Tracking.** Vehicles have no id, so trajectories are rebuilt by matching consecutive observations on position along the line, with limits on speed and backward movement. The service day is shifted by 4 hours so trips are not cut at midnight.
- **Speed from travel times between stops.** The metro is only observed at stations, so speed between 30-second polls would be meaningless. The speed is computed from the time between consecutive stop arrivals.
- **dbt tests on grain and plausibility.** `unique` / `not_null` / `accepted_values` plus custom tests: grain of each table, no missing hour in the calendar (`assert_no_missing_hours`), plausible speeds bounded above and below, completeness not above 101 %.

## Results

Silver output: 8,475,073 located rows, 7,939,963 tracked observations, 126,304 tracklets (median duration 29.5 min).

Three Gold tables:

| Table | Content |
|---|---|
| `fct_line_speeds` | Commercial speed per line, direction and local hour. |
| `fct_headway_regularity` | Median headway, coefficient of variation, % of bunching and of gaps, per line, direction and hour. |
| `fct_collection_health` | Polls received per local hour against the 120 expected. |

What the data shows: the metro stays at 26 to 28 km/h at any hour, while surface lines slow down at rush hours. The 10 lines most affected by bunching are all bus or tram lines. A drop in collection on 6 May afternoon points to a collector outage.

![Power BI dashboard](docs/img/powerbi_dashboard.png)

The report file is [bi/stib_dashboard.pbix](bi/stib_dashboard.pbix). Analysis notes and data findings are in [docs/findings.md](docs/findings.md).

## How to run

Requirements: Python 3.11, Java 17+ (for Spark; Java 19 was used), and on Windows `winutils.exe` / `hadoop.dll` in `C:\hadoop` (or `HADOOP_HOME`). The raw and static data are not in the repository, so the Spark jobs need your own copy in `data/raw/` and `data/static/`.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Tests** (from the repository root; the Spark tests take a few minutes):

```powershell
python -m pytest
```

**Spark jobs** (from the root; each job reads the previous layer, and the full run is long):

```powershell
python -m src.spark_jobs.bronze_ingestion
python -m src.spark_jobs.silver_cleaning
python -m scripts.build_stop_offsets
python -m src.spark_jobs.silver_geolocation
python -m src.spark_jobs.silver_tracking
```

**Gold** (from `dbt_stib/`, once Silver exists):

```powershell
cd dbt_stib
dbt build --profiles-dir .
dbt docs generate --profiles-dir .
```

**Export for BI**, then **orchestration** (from the root):

```powershell
python -m src.export_gold
dagster dev -m orchestration.definitions
```

## Limits and next steps

Known limits:

- Batch only: one fixed week, no incremental processing, jobs overwrite their output.
- Tracking has no chain-merge step: 9.29 % of tracklets are short fragments, which can inflate the bunching indicator. The bunching average is not weighted by number of headways.
- A poll with no vehicle is indistinguishable from a stopped collector, so "no service" and "outage" cannot be separated without a heartbeat log on the collection side.
- Line 4 has no shape in the static data (206,130 rows without position).
- Thresholds (for example the tracking limits) were tuned by hand on this week of data.
- The raw collector is not part of this repository.

Next steps, in the order I plan to do them:

1. **Docker**: one image with Python, Java, Spark, dbt and Dagster, plus a docker-compose file.
2. **CI**: GitHub Actions running pytest on small fixtures and dbt checks.
3. **Delta Lake** instead of Parquet for Silver (ACID writes, time travel, merge-based reruns).
4. **Kafka / Redpanda + Spark Structured Streaming** to replace the polling files, with deduplication on message identity.
5. **Superset** (or similar) as a BI layer that can run in a container, instead of a Power BI file.
6. Chain-merge of tracklets and a heartbeat log for the collector.

## License

See [LICENSE](LICENSE).
