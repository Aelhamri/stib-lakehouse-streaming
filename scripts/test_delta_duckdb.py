import duckdb

con = duckdb.connect()
con.sql("install delta")
con.sql("load delta")
print(con.sql("select count(*) as n from delta_scan('data/delta/silver/positions_tracked')"))