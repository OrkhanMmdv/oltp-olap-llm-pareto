"""
Baseline 3: rule-based heuristic transformation (no semantic knowledge).

Rules (applied mechanically to the source schema metadata):
  H1. The table with the largest number of rows becomes the fact table.
  H2. Every table referenced directly by the fact table becomes a dimension.
  H3. Tables referenced by a dimension (and not directly by the fact table)
      are merged into that dimension (denormalisation of hierarchies).
  H4. All columns are kept (the heuristic cannot judge which are useful).
"""
import duckdb, os, time, json, re
from workload import ORIGINAL

SRC, DST = "tpch_sf1.duckdb", "heuristic_sf1.duckdb"

# Foreign keys of the TPC-H schema: (child, parent, join condition)
FKS = [
    ("lineitem", "orders",   "lineitem.l_orderkey = orders.o_orderkey"),
    ("lineitem", "part",     "lineitem.l_partkey = part.p_partkey"),
    ("lineitem", "supplier", "lineitem.l_suppkey = supplier.s_suppkey"),
    ("lineitem", "partsupp", "lineitem.l_partkey = partsupp.ps_partkey "
                             "AND lineitem.l_suppkey = partsupp.ps_suppkey"),
    ("orders",   "customer", "orders.o_custkey = customer.c_custkey"),
    ("customer", "nation",   "customer.c_nationkey = nation.n_nationkey"),
    ("supplier", "nation",   "supplier.s_nationkey = nation.n_nationkey"),
    ("nation",   "region",   "nation.n_regionkey = region.r_regionkey"),
    ("partsupp", "part",     "partsupp.ps_partkey = part.p_partkey"),
    ("partsupp", "supplier", "partsupp.ps_suppkey = supplier.s_suppkey"),
]

if os.path.exists(DST):
    os.remove(DST)
start = time.time()
con = duckdb.connect(DST)
con.execute(f"ATTACH '{SRC}' AS src (READ_ONLY)")

# H1: fact = largest table
tables = [t for (t,) in con.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'src'").fetchall()]
sizes = {t: con.execute(f"SELECT COUNT(*) FROM src.{t}").fetchone()[0] for t in tables}
fact = max(sizes, key=sizes.get)

# H2: dimensions = tables referenced directly by the fact
dims = [p for c, p, _ in FKS if c == fact]

# H3: merge each dimension's ancestors (that are not dimensions themselves)
merged = {}
for d in dims:
    chain, todo = [], [d]
    while todo:
        cur = todo.pop(0)
        for c, p, cond in FKS:
            if c == cur and p not in dims and p != fact and p not in chain:
                chain.append(p)
                todo.append(p)
    merged[d] = chain

# Build the tables (H4: SELECT * keeps all columns)
con.execute(f"CREATE TABLE fact_{fact} AS SELECT * FROM src.{fact}")
for d in dims:
    sql = f"SELECT * FROM src.{d} AS {d}"
    joined = {d}
    for p in merged[d]:
        cond = next(cnd for c, pp, cnd in FKS if pp == p and c in joined)
        sql += f" JOIN src.{p} AS {p} ON {cond}"
        joined.add(p)
    con.execute(f"CREATE TABLE dim_{d} AS {sql}")
con.execute("CHECKPOINT")
con.execute("DETACH src")

# ---- Schema description for the workload translator ----
new_alias = {fact: "f", **{d: f"d_{d}" for d in dims}}
nodes = {"f": f"fact_{fact}", **{f"d_{d}": f"dim_{d}" for d in dims}}

# Map every original alias to the new table that now holds its columns
parent = {ORIGINAL["root"]: None}
queue = [ORIGINAL["root"]]
while queue:
    n = queue.pop(0)
    for a, b, _ in ORIGINAL["edges"]:
        for x, y in ((a, b), (b, a)):
            if x == n and y not in parent:
                parent[y] = n
                queue.append(y)
alias_map = {}
for alias in ORIGINAL["nodes"]:
    cur = alias
    while ORIGINAL["nodes"][cur] not in new_alias:
        cur = parent[cur]
    alias_map[alias] = new_alias[ORIGINAL["nodes"][cur]]

edges = []
for c, p, cond in FKS:
    if c == fact:
        new = cond.replace(f"{fact}.", "f.").replace(f"{p}.", f"d_{p}.")
        edges.append(("f", f"d_{p}", new))

attrs = {}
for a, (al, expr) in ORIGINAL["attrs"].items():
    new_expr = re.sub(r"\b(\w+)\.(?=[a-z])",
                      lambda m: alias_map.get(m.group(1), m.group(1)) + ".", expr)
    attrs[a] = (alias_map[al], new_expr)

schema = {"name": "heuristic", "db": DST, "root": "f",
          "nodes": nodes, "edges": edges, "attrs": attrs}
with open("heuristic_schema.json", "w") as fh:
    json.dump(schema, fh, indent=2)

# ---- Report ----
print(f"Fact table (H1): {fact}")
print(f"Dimensions (H2): {', '.join(dims)}")
for d in dims:
    print(f"  dim_{d} merges (H3): {', '.join(merged[d]) or '-'}")
print()
for (name,) in con.execute(
        "SELECT table_name FROM information_schema.tables ORDER BY table_name").fetchall():
    rows = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    cols = con.execute(f"SELECT COUNT(*) FROM information_schema.columns "
                       f"WHERE table_name = '{name}'").fetchone()[0]
    print(f"{name:15s} {rows:>12,} rows  {cols:>3} columns")
con.close()
print(f"\nFile size: {os.path.getsize(DST) / 1024 / 1024:.1f} MB")
print(f"Built in {time.time() - start:.1f} seconds")
print("Schema description saved to heuristic_schema.json")