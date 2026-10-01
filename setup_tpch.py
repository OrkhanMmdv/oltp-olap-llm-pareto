import duckdb
import time

start = time.time()

# Create (or open) the database file
con = duckdb.connect("tpch_sf1.duckdb")

# Load the built-in TPC-H extension and generate data at scale factor 1 (~1 GB)
con.execute("INSTALL tpch; LOAD tpch;")
con.execute("CALL dbgen(sf=1);")

# Show all tables and their row counts
tables = con.execute(
    "SELECT table_name FROM information_schema.tables ORDER BY table_name"
).fetchall()
for (name,) in tables:
    rows = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    print(f"{name:10s} {rows:>12,}")

print(f"Done in {time.time() - start:.1f} seconds")
con.close()