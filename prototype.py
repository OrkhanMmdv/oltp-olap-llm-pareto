"""
Prototype v1: automated OLTP-to-OLAP schema transformation using
OLAP-specific transformation rules, LLM-based semantic assessment and
multi-criteria Pareto optimisation.

Usage:  python prototype.py            (default budget: 25 candidate schemas)
        python prototype.py --budget 40
"""
import argparse, csv, json, os, re, statistics, time
import duckdb
from dotenv import load_dotenv
from workload import QUERIES, ORIGINAL, STAR, to_sql

SRC = "tpch_sf1.duckdb"
WORK_DIR = "candidates"
RUNS = 3

# ======================================================================
# 1. OLAP-specific transformation rules (rule database)
#    A candidate schema = a vector of 7 binary decisions.
# ======================================================================
RULES = [
    ("R1", "Merge ORDERS into the fact table (grain: order line)"),
    ("R2", "Denormalise customer hierarchy (NATION, REGION into CUSTOMER)"),
    ("R3", "Denormalise supplier hierarchy (NATION, REGION into SUPPLIER)"),
    ("R4", "Create a date dimension from the order date"),
    ("R5", "Pre-calculate measures (revenue, supply cost) in the fact table"),
    ("R6", "Remove descriptive comment columns"),
    ("R7", "Sort the fact table by order date (sort key)"),
]
N = len(RULES)


def is_valid_config(cfg):
    """Dependencies between rules."""
    r = dict(zip([x[0] for x in RULES], cfg))
    if r["R7"] and not r["R1"]:      # sort key needs the order date in the fact table
        return False
    return True


def cfg_id(cfg):
    return "S" + "".join(str(b) for b in cfg)


# ======================================================================
# 2. Schema builder: config -> physical DuckDB database + description
# ======================================================================
def cols(con, table, drop_comments):
    c = [r[0] for r in con.execute(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='src' AND table_name='{table}' ORDER BY ordinal_position").fetchall()]
    return [x for x in c if not (drop_comments and x.endswith("_comment"))]


def build_schema(cfg, path):
    R1, R2, R3, R4, R5, R6, R7 = cfg
    if os.path.exists(path):
        os.remove(path)
    con = duckdb.connect(path)
    con.execute(f"ATTACH '{SRC}' AS src (READ_ONLY)")
    sel = lambda t, a: ", ".join(f"{a}.{c}" for c in cols(con, t, R6))
    datekey = "CAST(strftime(o.o_orderdate, '%Y%m%d') AS INTEGER) AS o_orderdatekey"

    # ---- fact table ----
    parts = [sel("lineitem", "l")]
    frm = "src.lineitem l"
    if R1:
        parts.append(sel("orders", "o").replace("o.o_orderkey, ", ""))
        frm += " JOIN src.orders o ON l.l_orderkey = o.o_orderkey"
        if R4:
            parts.append(datekey)
    if R5:
        parts.append("l.l_extendedprice * (1 - l.l_discount) AS lo_revenue")
        parts.append("ps.ps_supplycost * l.l_quantity AS lo_supplycost")
        frm += (" JOIN src.partsupp ps ON l.l_partkey = ps.ps_partkey"
                " AND l.l_suppkey = ps.ps_suppkey")
    order = " ORDER BY o.o_orderdate" if R7 else ""
    con.execute(f"CREATE TABLE fact AS SELECT {', '.join(parts)} FROM {frm}{order}")

    # ---- orders (if not merged) ----
    if not R1:
        extra = f", {datekey}" if R4 else ""
        con.execute(f"CREATE TABLE orders AS SELECT {sel('orders', 'o')}{extra} FROM src.orders o")

    # ---- customer / supplier hierarchies ----
    def entity(name, a, merge):
        if merge:
            con.execute(f"""CREATE TABLE {name} AS SELECT {sel(name, a)},
                n.n_name AS {a}_nation, r.r_name AS {a}_region
                FROM src.{name} {a}
                JOIN src.nation n ON {a}.{a}_nationkey = n.n_nationkey
                JOIN src.region r ON n.n_regionkey = r.r_regionkey""")
        else:
            con.execute(f"CREATE TABLE {name} AS SELECT {sel(name, a)} FROM src.{name} {a}")
    entity("customer", "c", R2)
    entity("supplier", "s", R3)
    if not (R2 and R3):
        con.execute(f"CREATE TABLE nation AS SELECT {sel('nation', 'n')} FROM src.nation n")
        con.execute(f"CREATE TABLE region AS SELECT {sel('region', 'r')} FROM src.region r")

    con.execute(f"CREATE TABLE part AS SELECT {sel('part', 'p')} FROM src.part p")
    if not R5:
        con.execute(f"CREATE TABLE partsupp AS SELECT {sel('partsupp', 'ps')} FROM src.partsupp ps")
    if R4:
        con.execute("""CREATE TABLE dim_date AS SELECT DISTINCT
            CAST(strftime(o_orderdate, '%Y%m%d') AS INTEGER) AS d_datekey,
            o_orderdate AS d_date, year(o_orderdate) AS d_year,
            month(o_orderdate) AS d_month,
            year(o_orderdate) * 100 + month(o_orderdate) AS d_yearmonthnum,
            weekofyear(o_orderdate) AS d_weeknuminyear
            FROM src.orders""")
    con.execute("CHECKPOINT")
    con.execute("DETACH src")
    con.close()
    return describe(cfg, path)


def describe(cfg, path):
    """Schema description for the workload translator."""
    R1, R2, R3, R4, R5, R6, R7 = cfg
    nodes = {"f": "fact", "c": "customer", "s": "supplier", "p": "part"}
    edges = [("f", "s", "f.l_suppkey = s.s_suppkey"),
             ("f", "p", "f.l_partkey = p.p_partkey")]
    if R1:
        edges.append(("f", "c", "f.o_custkey = c.c_custkey"))
        odate, dsrc = "f.o_orderdate", "f"
    else:
        nodes["o"] = "orders"
        edges += [("f", "o", "f.l_orderkey = o.o_orderkey"),
                  ("o", "c", "o.o_custkey = c.c_custkey")]
        odate, dsrc = "o.o_orderdate", "o"
    attrs = {
        "extendedprice": ("f", "f.l_extendedprice"),
        "discount": ("f", "f.l_discount"),
        "quantity": ("f", "f.l_quantity"),
        "c_mktsegment": ("c", "c.c_mktsegment"),
        "p_mfgr": ("p", "p.p_mfgr"), "p_brand": ("p", "p.p_brand"),
        "p_type": ("p", "p.p_type"),
    }
    if R4:
        nodes["d"] = "dim_date"
        edges.append((dsrc, "d", f"{dsrc}.o_orderdatekey = d.d_datekey"))
        attrs.update(year=("d", "d.d_year"), yearmonthnum=("d", "d.d_yearmonthnum"),
                     weeknuminyear=("d", "d.d_weeknuminyear"))
    else:
        attrs.update(year=(dsrc, f"year({odate})"),
                     yearmonthnum=(dsrc, f"year({odate}) * 100 + month({odate})"),
                     weeknuminyear=(dsrc, f"weekofyear({odate})"))
    if R5:
        attrs.update(revenue=("f", "f.lo_revenue"), supplycost=("f", "f.lo_supplycost"))
    else:
        nodes["ps"] = "partsupp"
        edges.append(("f", "ps", "f.l_partkey = ps.ps_partkey AND f.l_suppkey = ps.ps_suppkey"))
        attrs.update(revenue=("f", "f.l_extendedprice * (1 - f.l_discount)"),
                     supplycost=("ps", "ps.ps_supplycost * f.l_quantity"))
    for a, merged, key in (("c", R2, "customer"), ("s", R3, "supplier")):
        if merged:
            attrs[f"{a}_nation"] = (a, f"{a}.{a}_nation")
            attrs[f"{a}_region"] = (a, f"{a}.{a}_region")
        else:
            n, r = f"{a}n", f"{a}r"
            nodes[n], nodes[r] = "nation", "region"
            edges += [(a, n, f"{a}.{a}_nationkey = {n}.n_nationkey"),
                      (n, r, f"{n}.n_regionkey = {r}.r_regionkey")]
            attrs[f"{a}_nation"] = (n, f"{n}.n_name")
            attrs[f"{a}_region"] = (r, f"{r}.r_name")
    return {"name": cfg_id(cfg), "db": path, "root": "f",
            "nodes": nodes, "edges": edges, "attrs": attrs}


# ======================================================================
# 3. Quantitative evaluation (K1, K2, K3 + validity)
# ======================================================================
def normalise(rows):
    return sorted((tuple(round(float(v), 2) if isinstance(v, (int, float)) or
                         type(v).__name__ == "Decimal" else v for v in r) for r in rows),
                  key=str)


def run_workload(schema, reference=None):
    con = duckdb.connect(schema["db"], read_only=True)
    total, joins, valid, answers = 0.0, 0, True, {}
    for qn in QUERIES:
        sql, j = to_sql(schema, qn)
        res = con.execute(sql).fetchall()            # warm-up + answer
        times = []
        for _ in range(RUNS):
            t = time.perf_counter()
            con.execute(sql).fetchall()
            times.append(time.perf_counter() - t)
        total += statistics.median(times) * 1000
        joins += j
        answers[qn] = normalise(res)
        if reference is not None and answers[qn] != reference[qn]:
            valid = False
    con.close()
    size = os.path.getsize(schema["db"]) / 1024 / 1024
    return {"K1_ms": round(total, 1), "K2_MB": round(size, 1), "K3_joins": joins,
            "valid": valid, "answers": answers}


# ======================================================================
# 4. LLM-based semantic assessment (K4)
# ======================================================================
PROMPT = """You are an expert data warehouse architect. Evaluate the SEMANTIC QUALITY of
the following analytical (OLAP) database schema, which was derived from the
TPC-H order-processing database. Do not evaluate performance or storage.

Assess these five aspects, each on a scale from 1 (very poor) to 10 (excellent):
1. fact_dimension: are facts (measurable business events) and dimensions
   (descriptive context) identified correctly?
2. grain: is the grain of the fact table clear and consistent?
3. hierarchies_time: are meaningful dimension hierarchies and a usable time
   dimension available for analysis?
4. naming: are table and column names clear and unambiguous?
5. redundancy: is the schema free of irrelevant or redundant attributes and tables?

Then give an overall score from 1 to 10.

Example of the expected answer format:
{"fact_dimension": 8, "grain": 9, "hierarchies_time": 6, "naming": 7,
 "redundancy": 7, "overall": 7, "justification": "One or two sentences."}

Answer ONLY with a JSON object in exactly this format.

SCHEMA:
"""


def schema_text(schema):
    """Plain-text description of tables, columns, row counts and joins."""
    con = duckdb.connect(schema["db"], read_only=True)
    lines = []
    for t in sorted(set(schema["nodes"].values())):
        cs = con.execute(f"SELECT column_name, data_type FROM information_schema.columns "
                         f"WHERE table_name='{t}' ORDER BY ordinal_position").fetchall()
        n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        lines.append(f"TABLE {t} ({n:,} rows): " + ", ".join(f"{c} {d}" for c, d in cs))
    con.close()
    lines.append("RELATIONSHIPS:")
    for a, b, cond in schema["edges"]:
        lines.append(f"  {schema['nodes'][a]} -> {schema['nodes'][b]} ON {cond}")
    return "\n".join(lines)


def llm_assess(schema, cache):
    key = schema["name"]
    if key in cache:
        return cache[key]
    import anthropic
    client = anthropic.Anthropic()
    msg = client.messages.create(
        model=os.environ["CLAUDE_MODEL"], max_tokens=400,
        messages=[{"role": "user", "content": PROMPT + schema_text(schema)}])
    text = msg.content[0].text
    m = re.search(r"\{.*\}", text, re.S)
    result = json.loads(m.group(0))
    cache[key] = result
    with open("llm_cache.json", "w") as f:
        json.dump(cache, f, indent=2)
    return result


# ======================================================================
# 5. Pareto optimisation (K1, K2, K3 minimised; K4 maximised)
# ======================================================================
def objectives(r):
    return (r["K1_ms"], r["K2_MB"], r["K3_joins"], -r["K4"])


def dominates(a, b):
    oa, ob = objectives(a), objectives(b)
    return all(x <= y for x, y in zip(oa, ob)) and any(x < y for x, y in zip(oa, ob))


def pareto_front(results):
    return [r for r in results if not any(dominates(o, r) for o in results if o is not r)]


# ======================================================================
# 6. Adaptive neighbourhood search (after Auziņš et al., 2018)
# ======================================================================
def neighbours(cfg):
    for i in range(N):
        n = list(cfg)
        n[i] = 1 - n[i]
        n = tuple(n)
        if is_valid_config(n):
            yield n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=25)
    args = ap.parse_args()
    load_dotenv()
    os.makedirs(WORK_DIR, exist_ok=True)
    cache = json.load(open("llm_cache.json")) if os.path.exists("llm_cache.json") else {}

    print("Reference answers from the original schema ...")
    reference = run_workload(ORIGINAL)["answers"]

    evaluated, results = set(), []

    def evaluate(cfg):
        path = os.path.join(WORK_DIR, cfg_id(cfg) + ".duckdb")
        t0 = time.time()
        schema = build_schema(cfg, path)
        m = run_workload(schema, reference)
        llm = llm_assess(schema, cache)
        os.remove(path)                               # free disk space
        r = {"id": cfg_id(cfg), **{RULES[i][0]: cfg[i] for i in range(N)},
             "K1_ms": m["K1_ms"], "K2_MB": m["K2_MB"], "K3_joins": m["K3_joins"],
             "K4": llm["overall"], "valid": m["valid"],
             "justification": llm.get("justification", "")}
        evaluated.add(cfg)
        print(f"{len(evaluated):>3}. {r['id']}  K1={r['K1_ms']:7.1f} ms  K2={r['K2_MB']:6.1f} MB"
              f"  K3={r['K3_joins']:>2}  K4={r['K4']:>2}  valid={r['valid']}"
              f"  ({time.time() - t0:.0f} s)")
        if r["valid"]:
            results.append(r)
        return r

    # Start from the untransformed schema and explore neighbourhoods of
    # Pareto-optimal candidates until the budget is used.
    start = tuple([0] * N)
    evaluate(start)
    explored = set()
    while len(evaluated) < args.budget:
        front = pareto_front(results)
        todo = [r for r in front if r["id"] not in explored]
        if not todo:
            break
        current = min(todo, key=lambda r: r["K1_ms"])
        explored.add(current["id"])
        cfg = tuple(current[RULES[i][0]] for i in range(N))
        for n in neighbours(cfg):
            if n not in evaluated and len(evaluated) < args.budget:
                evaluate(n)

    front_ids = {r["id"] for r in pareto_front(results)}
    for r in results:
        r["pareto"] = r["id"] in front_ids

    # ---- Semantic score of the baseline schemas (for comparison) ----
    baselines = []
    heur = json.load(open("heuristic_schema.json"))
    for sch in (ORIGINAL, STAR, heur):
        llm = llm_assess(sch, cache)
        m = run_workload(sch)
        baselines.append({"id": sch["name"], "K1_ms": m["K1_ms"], "K2_MB": m["K2_MB"],
                          "K3_joins": m["K3_joins"], "K4": llm["overall"],
                          "justification": llm.get("justification", "")})

    # ---- Save results ----
    fields = ["id"] + [x[0] for x in RULES] + ["K1_ms", "K2_MB", "K3_joins", "K4",
                                                 "valid", "pareto", "justification"]
    with open("results_prototype.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)
    with open("results_baselines_llm.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "K1_ms", "K2_MB", "K3_joins", "K4",
                                          "justification"])
        w.writeheader()
        w.writerows(baselines)

    print(f"\nEvaluated: {len(evaluated)}  valid: {len(results)}  "
          f"Pareto-optimal: {len(front_ids)}")
    print("\nPareto front:")
    for r in sorted(pareto_front(results), key=lambda r: r["K1_ms"]):
        rules = ",".join(x[0] for x in RULES if r[x[0]])
        print(f"  {r['id']}  K1={r['K1_ms']:7.1f}  K2={r['K2_MB']:6.1f}  "
              f"K3={r['K3_joins']:>2}  K4={r['K4']:>2}  rules: {rules or '-'}")
    print("\nBaselines:")
    for b in baselines:
        print(f"  {b['id']:10s} K1={b['K1_ms']:7.1f}  K2={b['K2_MB']:6.1f}  "
              f"K3={b['K3_joins']:>2}  K4={b['K4']:>2}")
    plot(results, baselines)


def plot(results, baselines):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    INK, MUTED, GRID = "#0b0b0b", "#9a9893", "#e5e4e0"
    PARETO, BASE = "#2a78d6", "#eb6834"
    fig, ax = plt.subplots(figsize=(7, 4.5), dpi=200)
    dom = [r for r in results if not r["pareto"]]
    par = [r for r in results if r["pareto"]]
    ax.scatter([r["K1_ms"] for r in dom], [r["K4"] for r in dom], s=40,
               color=MUTED, edgecolor="white", linewidth=1.5, label="Dominated candidate", zorder=2)
    ax.scatter([r["K1_ms"] for r in par], [r["K4"] for r in par], s=60,
               color=PARETO, edgecolor="white", linewidth=1.5, label="Pareto-optimal candidate", zorder=3)
    ax.scatter([b["K1_ms"] for b in baselines], [b["K4"] for b in baselines], s=70,
               marker="D", color=BASE, edgecolor="white", linewidth=1.5, label="Baseline schema", zorder=4)
    for b in baselines:
        ax.annotate(b["id"], (b["K1_ms"], b["K4"]), textcoords="offset points",
                    xytext=(6, 6), fontsize=8, color=INK)
    ax.set_xlabel("K1: total query execution time, ms", color=INK, fontsize=9)
    ax.set_ylabel("K4: semantic quality (LLM), points", color=INK, fontsize=9)
    ax.set_ylim(0, 10.5)
    ax.grid(color=GRID, linewidth=0.8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(colors=INK, labelsize=8)
    ax.legend(frameon=False, fontsize=8, loc="lower right")
    fig.tight_layout()
    fig.savefig("pareto_front.png")
    print("\nChart saved to pareto_front.png")


if __name__ == "__main__":
    main()