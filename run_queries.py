import duckdb, time, statistics, csv, platform, sys

con = duckdb.connect("tpch_sf1.duckdb", read_only=True)
con.execute("LOAD tpch;")

# Environment info (needed for the thesis)
print("Python", sys.version.split()[0], "| DuckDB", duckdb.__version__)
print(platform.platform(), "|", platform.processor())
print()

# The 22 official TPC-H queries are built into DuckDB
queries = con.execute(
    "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr"
).fetchall()

results = []
for nr, q in queries:
    con.execute(q).fetchall()              # 1 warm-up run (not measured)
    times = []
    for _ in range(3):                     # 3 measured runs
        t = time.perf_counter()
        con.execute(q).fetchall()
        times.append(time.perf_counter() - t)
    med = statistics.median(times) * 1000  # median, in milliseconds
    results.append((nr, round(med, 1)))
    print(f"Q{nr:02d} {med:9.1f} ms")

total = sum(r[1] for r in results)
print(f"\nTotal (22 queries): {total:.1f} ms")

with open("results_original.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["query", "median_ms"])
    w.writerows(results)
print("Saved to results_original.csv")