import duckdb, os, csv, json
from collections import defaultdict
from workload import ORIGINAL, STAR

HEURISTIC = json.load(open("heuristic_schema.json"))
SCHEMAS = [ORIGINAL, STAR, HEURISTIC]

# Query results from run_workload.py
time_ms, joins = defaultdict(float), defaultdict(int)
with open("results_workload.csv") as f:
    for r in csv.DictReader(f):
        time_ms[r["schema"]] += float(r["median_ms"])
        joins[r["schema"]] += int(r["joins"])

rows_out = []
for sch in SCHEMAS:
    con = duckdb.connect(sch["db"], read_only=True)
    tables = [t for (t,) in con.execute(
        "SELECT table_name FROM information_schema.tables").fetchall()]
    n_cols = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns").fetchone()[0]
    n_rows = sum(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables)
    con.close()
    size = os.path.getsize(sch["db"]) / 1024 / 1024
    rows_out.append([sch["name"], len(tables), n_cols, n_rows, round(size, 1),
                     joins[sch["name"]], round(time_ms[sch["name"]], 1)])

hdr = ["schema", "tables", "columns", "total_rows", "size_MB", "joins_13q", "time_13q_ms"]
print(f"{hdr[0]:10s} {hdr[1]:>6s} {hdr[2]:>8s} {hdr[3]:>12s} {hdr[4]:>8s} {hdr[5]:>9s} {hdr[6]:>11s}")
for r in rows_out:
    print(f"{r[0]:10s} {r[1]:>6d} {r[2]:>8d} {r[3]:>12,} {r[4]:>8.1f} {r[5]:>9d} {r[6]:>11.1f}")

with open("summary_baselines.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(hdr); w.writerows(rows_out)
print("\nSaved to summary_baselines.csv")