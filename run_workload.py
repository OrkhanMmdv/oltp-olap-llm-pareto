import duckdb, time, statistics, csv, os, json
from workload import ORIGINAL, STAR, QUERIES, to_sql

HEURISTIC = json.load(open("heuristic_schema.json"))
SCHEMAS = [ORIGINAL, STAR, HEURISTIC]
RUNS = 3

def normalise(rows):
    """Round numbers so results from different schemas can be compared."""
    out = []
    for r in rows:
        out.append(tuple(round(float(v), 2) if isinstance(v, (int, float)) or
                         type(v).__name__ == "Decimal" else v for v in r))
    return sorted(out, key=str)

results = {}
rows_out = []
for sch in SCHEMAS:
    con = duckdb.connect(sch["db"], read_only=True)
    size_mb = os.path.getsize(sch["db"]) / 1024 / 1024
    print(f"\n=== {sch['name']}  ({size_mb:.1f} MB, {len(set(sch['nodes'].values()))} tables) ===")
    total = 0
    for qn in QUERIES:
        sql, joins = to_sql(sch, qn)
        res = con.execute(sql).fetchall()          # warm-up run (also the answer)
        times = []
        for _ in range(RUNS):
            t = time.perf_counter()
            con.execute(sql).fetchall()
            times.append(time.perf_counter() - t)
        med = statistics.median(times) * 1000
        total += med
        results[(sch["name"], qn)] = normalise(res)
        rows_out.append([sch["name"], qn, joins, len(res), round(med, 1)])
        print(f"{qn}  joins={joins}  rows={len(res):>3}  {med:8.1f} ms")
    print(f"Total: {total:.1f} ms")
    con.close()

# ---- Correctness check: every schema must return the same answers ----
print("\n=== Correctness check ===")
ok = True
for qn in QUERIES:
    ref = results[(SCHEMAS[0]["name"], qn)]
    for sch in SCHEMAS[1:]:
        if results[(sch["name"], qn)] != ref:
            ok = False
            print(f"MISMATCH in {qn} for schema {sch['name']}")
print("All results identical across schemas" if ok else "Some results differ!")

with open("results_workload.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["schema", "query", "joins", "result_rows", "median_ms"])
    w.writerows(rows_out)
print("Saved to results_workload.csv")
