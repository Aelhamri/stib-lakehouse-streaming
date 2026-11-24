import duckdb

con = duckdb.connect("data/gold/stib.duckdb", read_only=True)
for t in ["fct_line_speeds", "fct_headway_regularity", "fct_collection_health"]:
    n = con.execute(f"select count(*) from {t}").fetchone()[0]
    print(t, n)