"""
Prototype v2: automated OLTP-to-OLAP schema transformation with
OLAP-specific rules, structural checks, LLM-based semantic assessment and
multi-criteria Pareto optimisation.

Study condition: ALL SOURCE INFORMATION IS PRESERVED. Every column of the
TPC-H source schema must remain available in every candidate schema.

Commands (run in this order):
  python prototype2.py enumerate              build + evaluate all 96 candidates (SF 1)
  python prototype2.py analyse                fronts with/without LLM, search simulation,
                                              overall Pareto set incl. baselines, rule effects
  python prototype2.py consistency            repeated LLM scoring of selected schemas
  python prototype2.py setup --sf 5           generate a larger TPC-H database
  python prototype2.py remeasure --sf 1       precise re-measurement (5 randomised cycles)
  python prototype2.py remeasure --sf 5       the same on the larger dataset
"""
import argparse, csv, itertools, json, math, os, random, re, statistics, time
import duckdb
from dotenv import load_dotenv
from workload import QUERIES, ORIGINAL, STAR, to_sql

THREADS = 4                 # fixed DuckDB settings for all measurements
MEMORY_LIMIT = "6GB"
WORK_DIR = "candidates"

def src_db(sf): return f"tpch_sf{sf}.duckdb"

# ======================================================================
# 1. Rule database
# ======================================================================
RULES = [
    ("R1", "Move order-level descriptive attributes into the line fact table; "
           "order-level measures stay in a separate order fact table"),
    ("R2", "Denormalise customer hierarchy (nation and region names into DIM_CUSTOMER)"),
    ("R3", "Denormalise supplier hierarchy (nation and region names into DIM_SUPPLIER)"),
    ("R4", "Create a date dimension from the order date"),
    ("R5", "Pre-calculate revenue and supply cost in the line fact table"),
    ("R6", "Move comment columns into separate side tables (vertical partitioning)"),
    ("R7", "Sort the line fact table by order date (requires R1)"),
]
N = len(RULES)
RULE_IDS = [r[0] for r in RULES]

def is_valid_config(cfg):
    r = dict(zip(RULE_IDS, cfg))
    return not (r["R7"] and not r["R1"])

def cfg_id(cfg): return "S" + "".join(str(b) for b in cfg)
def all_configs(): return [c for c in itertools.product([0, 1], repeat=N) if is_valid_config(c)]

# Keys of the source tables (used for side tables of comment columns)
KEYS = {"lineitem": ["l_orderkey", "l_linenumber"], "orders": ["o_orderkey"],
        "customer": ["c_custkey"], "supplier": ["s_suppkey"], "part": ["p_partkey"],
        "partsupp": ["ps_partkey", "ps_suppkey"], "nation": ["n_nationkey"],
        "region": ["r_regionkey"]}
ORDER_MEASURES = ["o_totalprice"]          # order-level additive measures

# ======================================================================
# 2. Schema builder
# ======================================================================
def _cols(con, table):
    return [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='src' AND table_name='{table}' ORDER BY ordinal_position").fetchall()]

def build_schema(cfg, path, src):
    R1, R2, R3, R4, R5, R6, R7 = cfg
    if os.path.exists(path):
        os.remove(path)
    con = duckdb.connect(path)
    con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
    keep = lambda t: [c for c in _cols(con, t) if not (R6 and c.endswith("_comment"))]
    sel = lambda t, a, cols=None: ", ".join(f"{a}.{c}" for c in (cols or keep(t)))
    datekey = "CAST(strftime(o.o_orderdate, '%Y%m%d') AS INTEGER) AS o_orderdatekey"

    # ---- line-grain fact table ----
    parts, frm = [sel("lineitem", "l")], "src.lineitem l"
    if R1:
        o_desc = [c for c in keep("orders") if c not in ["o_orderkey"] + ORDER_MEASURES
                  and not c.endswith("_comment")]
        parts.append(sel("orders", "o", o_desc))
        frm += " JOIN src.orders o ON l.l_orderkey = o.o_orderkey"
        if R4:
            parts.append(datekey)
    if R5:
        parts += ["l.l_extendedprice * (1 - l.l_discount) AS lo_revenue",
                  "ps.ps_supplycost * l.l_quantity AS lo_supplycost"]
        frm += (" JOIN src.partsupp ps ON l.l_partkey = ps.ps_partkey"
                " AND l.l_suppkey = ps.ps_suppkey")
    order = " ORDER BY o.o_orderdate" if R7 else ""
    con.execute(f"CREATE TABLE fact_lineorder AS SELECT {', '.join(parts)} FROM {frm}{order}")

    # ---- order-grain fact table (always kept: order-level measures live here) ----
    if R1:
        ocols = ["o_orderkey"] + ORDER_MEASURES + [c for c in keep("orders") if c.endswith("_comment")]
        con.execute(f"CREATE TABLE fact_orders AS SELECT {sel('orders', 'o', ocols)} FROM src.orders o")
    else:
        extra = f", {datekey}" if R4 else ""
        con.execute(f"CREATE TABLE fact_orders AS SELECT {sel('orders', 'o')}{extra} FROM src.orders o")

    # ---- dimensions (nation/region always kept so no information is lost) ----
    def dim(table, a, merge):
        if merge:
            con.execute(f"""CREATE TABLE dim_{table} AS SELECT {sel(table, a)},
                n.n_name AS {a}_nation, r.r_name AS {a}_region FROM src.{table} {a}
                JOIN src.nation n ON {a}.{a}_nationkey = n.n_nationkey
                JOIN src.region r ON n.n_regionkey = r.r_regionkey""")
        else:
            con.execute(f"CREATE TABLE dim_{table} AS SELECT {sel(table, a)} FROM src.{table} {a}")
    dim("customer", "c", R2)
    dim("supplier", "s", R3)
    con.execute(f"CREATE TABLE dim_nation AS SELECT {sel('nation', 'n')} FROM src.nation n")
    con.execute(f"CREATE TABLE dim_region AS SELECT {sel('region', 'r')} FROM src.region r")
    con.execute(f"CREATE TABLE dim_part AS SELECT {sel('part', 'p')} FROM src.part p")
    con.execute(f"CREATE TABLE fact_partsupp AS SELECT {sel('partsupp', 'ps')} FROM src.partsupp ps")
    if R4:
        con.execute("""CREATE TABLE dim_date AS SELECT DISTINCT
            CAST(strftime(o_orderdate, '%Y%m%d') AS INTEGER) AS d_datekey,
            o_orderdate AS d_date, year(o_orderdate) AS d_year, month(o_orderdate) AS d_month,
            year(o_orderdate) * 100 + month(o_orderdate) AS d_yearmonthnum,
            weekofyear(o_orderdate) AS d_weeknuminyear FROM src.orders""")

    # ---- R6: comment columns in side tables (vertical partitioning) ----
    if R6:
        for t, k in KEYS.items():
            c = [x for x in _cols(con, t) if x.endswith("_comment")]
            if c:
                con.execute(f"CREATE TABLE {t}_comments AS SELECT {', '.join(k + c)} FROM src.{t}")
    con.execute("CHECKPOINT")
    con.execute("DETACH src")
    con.close()
    return describe(cfg, path)

def describe(cfg, path):
    """Schema description for the workload translator."""
    R1, R2, R3, R4, R5, R6, R7 = cfg
    nodes = {"f": "fact_lineorder", "o": "fact_orders", "c": "dim_customer",
             "s": "dim_supplier", "p": "dim_part"}
    edges = [("f", "s", "f.l_suppkey = s.s_suppkey"), ("f", "p", "f.l_partkey = p.p_partkey"),
             ("f", "o", "f.l_orderkey = o.o_orderkey")]
    if R1:
        edges.append(("f", "c", "f.o_custkey = c.c_custkey")); odate, dsrc = "f.o_orderdate", "f"
    else:
        edges.append(("o", "c", "o.o_custkey = c.c_custkey")); odate, dsrc = "o.o_orderdate", "o"
    attrs = {"extendedprice": ("f", "f.l_extendedprice"), "discount": ("f", "f.l_discount"),
             "quantity": ("f", "f.l_quantity"), "c_mktsegment": ("c", "c.c_mktsegment"),
             "p_mfgr": ("p", "p.p_mfgr"), "p_brand": ("p", "p.p_brand"), "p_type": ("p", "p.p_type")}
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
        nodes["ps"] = "fact_partsupp"
        edges.append(("f", "ps", "f.l_partkey = ps.ps_partkey AND f.l_suppkey = ps.ps_suppkey"))
        attrs.update(revenue=("f", "f.l_extendedprice * (1 - f.l_discount)"),
                     supplycost=("ps", "ps.ps_supplycost * f.l_quantity"))
    for a, merged in (("c", R2), ("s", R3)):
        if merged:
            attrs[f"{a}_nation"] = (a, f"{a}.{a}_nation"); attrs[f"{a}_region"] = (a, f"{a}.{a}_region")
        else:
            n, r = f"{a}n", f"{a}r"
            nodes[n], nodes[r] = "dim_nation", "dim_region"
            edges += [(a, n, f"{a}.{a}_nationkey = {n}.n_nationkey"),
                      (n, r, f"{n}.n_regionkey = {r}.r_regionkey")]
            attrs[f"{a}_nation"] = (n, f"{n}.n_name"); attrs[f"{a}_region"] = (r, f"{r}.r_name")
    return {"name": cfg_id(cfg), "db": path, "root": "f", "nodes": nodes, "edges": edges, "attrs": attrs}

# ---- baseline builders (so they can also be created for larger datasets) ----
def build_star(src, dst):
    if os.path.exists(dst): os.remove(dst)
    con = duckdb.connect(dst); con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
    con.execute("""CREATE TABLE dim_date AS SELECT DISTINCT
        CAST(strftime(o_orderdate, '%Y%m%d') AS INTEGER) AS d_datekey, o_orderdate AS d_date,
        year(o_orderdate) AS d_year, month(o_orderdate) AS d_month,
        year(o_orderdate) * 100 + month(o_orderdate) AS d_yearmonthnum,
        weekofyear(o_orderdate) AS d_weeknuminyear, dayname(o_orderdate) AS d_dayofweek FROM src.orders""")
    for t, a in (("customer", "c"), ("supplier", "s")):
        extra = ", c.c_mktsegment" if t == "customer" else ""
        con.execute(f"""CREATE TABLE dim_{t} AS SELECT {a}.{a}_{'custkey' if a == 'c' else 'suppkey'},
            {a}.{a}_name, {a}.{a}_address, {a}.{a}_phone{extra},
            n.n_name AS {a}_nation, r.r_name AS {a}_region FROM src.{t} {a}
            JOIN src.nation n ON {a}.{a}_nationkey = n.n_nationkey
            JOIN src.region r ON n.n_regionkey = r.r_regionkey""")
    con.execute("CREATE TABLE dim_part AS SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container FROM src.part")
    con.execute("""CREATE TABLE fact_lineorder AS SELECT l.l_orderkey AS lo_orderkey,
        l.l_linenumber AS lo_linenumber, o.o_custkey AS lo_custkey, l.l_partkey AS lo_partkey,
        l.l_suppkey AS lo_suppkey, CAST(strftime(o.o_orderdate, '%Y%m%d') AS INTEGER) AS lo_orderdatekey,
        o.o_orderpriority AS lo_orderpriority, o.o_shippriority AS lo_shippriority,
        l.l_quantity AS lo_quantity, l.l_extendedprice AS lo_extendedprice,
        l.l_discount AS lo_discount, l.l_tax AS lo_tax,
        l.l_extendedprice * (1 - l.l_discount) AS lo_revenue,
        ps.ps_supplycost * l.l_quantity AS lo_supplycost, l.l_shipmode AS lo_shipmode
        FROM src.lineitem l JOIN src.orders o ON l.l_orderkey = o.o_orderkey
        JOIN src.partsupp ps ON l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey""")
    con.execute("CHECKPOINT"); con.execute("DETACH src"); con.close()
    return dict(STAR, db=dst)

def build_heuristic(src, dst):
    if os.path.exists(dst): os.remove(dst)
    con = duckdb.connect(dst); con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
    con.execute("CREATE TABLE fact_lineitem AS SELECT * FROM src.lineitem")
    con.execute("""CREATE TABLE dim_orders AS SELECT * FROM src.orders AS orders
        JOIN src.customer AS customer ON orders.o_custkey = customer.c_custkey
        JOIN src.nation AS nation ON customer.c_nationkey = nation.n_nationkey
        JOIN src.region AS region ON nation.n_regionkey = region.r_regionkey""")
    con.execute("CREATE TABLE dim_part AS SELECT * FROM src.part")
    con.execute("""CREATE TABLE dim_supplier AS SELECT * FROM src.supplier AS supplier
        JOIN src.nation AS nation ON supplier.s_nationkey = nation.n_nationkey
        JOIN src.region AS region ON nation.n_regionkey = region.r_regionkey""")
    con.execute("CREATE TABLE dim_partsupp AS SELECT * FROM src.partsupp")
    con.execute("CHECKPOINT"); con.execute("DETACH src"); con.close()
    return HEURISTIC_DESC(dst)

def HEURISTIC_DESC(db):
    O, S = "d_orders", "d_supplier"
    return {"name": "heuristic", "db": db, "root": "f",
            "nodes": {"f": "fact_lineitem", O: "dim_orders", "d_part": "dim_part",
                      S: "dim_supplier", "d_partsupp": "dim_partsupp"},
            "edges": [("f", O, f"f.l_orderkey = {O}.o_orderkey"),
                      ("f", "d_part", "f.l_partkey = d_part.p_partkey"),
                      ("f", S, f"f.l_suppkey = {S}.s_suppkey"),
                      ("f", "d_partsupp", "f.l_partkey = d_partsupp.ps_partkey AND f.l_suppkey = d_partsupp.ps_suppkey")],
            "attrs": {"extendedprice": ("f", "f.l_extendedprice"), "discount": ("f", "f.l_discount"),
                      "quantity": ("f", "f.l_quantity"),
                      "revenue": ("f", "f.l_extendedprice * (1 - f.l_discount)"),
                      "supplycost": ("d_partsupp", "d_partsupp.ps_supplycost * f.l_quantity"),
                      "year": (O, f"year({O}.o_orderdate)"),
                      "yearmonthnum": (O, f"year({O}.o_orderdate) * 100 + month({O}.o_orderdate)"),
                      "weeknuminyear": (O, f"weekofyear({O}.o_orderdate)"),
                      "c_nation": (O, f"{O}.n_name"), "c_region": (O, f"{O}.r_name"),
                      "c_mktsegment": (O, f"{O}.c_mktsegment"),
                      "s_nation": (S, f"{S}.n_name"), "s_region": (S, f"{S}.r_name"),
                      "p_mfgr": ("d_part", "d_part.p_mfgr"), "p_brand": ("d_part", "d_part.p_brand"),
                      "p_type": ("d_part", "d_part.p_type")}}

def baselines(sf, folder):
    os.makedirs(folder, exist_ok=True)
    src = src_db(sf)
    return [dict(ORIGINAL, db=src),
            build_star(src, os.path.join(folder, "star.duckdb")),
            build_heuristic(src, os.path.join(folder, "heuristic.duckdb"))]

# ======================================================================
# 3. Structural checks (independent of the LLM)
# ======================================================================
# measure columns: name -> (grain, aggregation)
MEASURES = {"l_quantity": ("line", "additive"), "l_extendedprice": ("line", "additive"),
            "lo_quantity": ("line", "additive"), "lo_extendedprice": ("line", "additive"),
            "lo_revenue": ("line", "additive"), "lo_supplycost": ("line", "additive"),
            "o_totalprice": ("order", "additive"), "ps_availqty": ("partsupp", "additive"),
            "c_acctbal": ("customer", "semi-additive"), "s_acctbal": ("supplier", "semi-additive")}
STAR_ALIASES = {"lo_orderkey": "l_orderkey", "lo_linenumber": "l_linenumber", "lo_custkey": "o_custkey",
                "lo_partkey": "l_partkey", "lo_suppkey": "l_suppkey", "lo_orderpriority": "o_orderpriority",
                "lo_shippriority": "o_shippriority", "lo_quantity": "l_quantity",
                "lo_extendedprice": "l_extendedprice", "lo_discount": "l_discount", "lo_tax": "l_tax",
                "lo_shipmode": "l_shipmode", "d_date": "o_orderdate"}

def table_columns(db):
    con = duckdb.connect(db, read_only=True)
    rows = con.execute("SELECT table_name, column_name FROM information_schema.columns").fetchall()
    con.close()
    out = {}
    for t, c in rows:
        out.setdefault(t, []).append(c)
    return out

def grain_check(db):
    """A table must not mix additive measures of different grains."""
    problems = []
    for t, cols in table_columns(db).items():
        grains = {MEASURES[c][0] for c in cols if c in MEASURES}
        if len(grains) > 1:
            bad = [c for c in cols if c in MEASURES]
            problems.append(f"{t}: {', '.join(bad)} ({' + '.join(sorted(grains))})")
    return problems

SOURCE_COLUMNS = None
def info_check(db, src):
    """Share of source columns that are still available in the schema."""
    global SOURCE_COLUMNS
    if SOURCE_COLUMNS is None:
        SOURCE_COLUMNS = {c for cols in table_columns(src).values() for c in cols}
    present = {STAR_ALIASES.get(c, c) for cols in table_columns(db).values() for c in cols}
    missing = sorted(SOURCE_COLUMNS - present)
    return 1 - len(missing) / len(SOURCE_COLUMNS), missing

# ======================================================================
# 4. Measurement (fixed settings, all individual timings recorded)
# ======================================================================
def normalise(rows):
    return sorted((tuple(round(float(v), 2) if isinstance(v, (int, float)) or
                         type(v).__name__ == "Decimal" else v for v in r) for r in rows), key=str)

def connect(db):
    con = duckdb.connect(db, read_only=True)
    con.execute(f"SET threads = {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    return con

def run_workload(schema, reference=None, runs=3, log=None, cycle=0):
    con = connect(schema["db"])
    total, joins, valid, answers = 0.0, 0, True, {}
    for qn in QUERIES:
        sql, j = to_sql(schema, qn)
        res = con.execute(sql).fetchall()                    # warm-up + answer
        times = []
        for r in range(runs):
            t = time.perf_counter(); con.execute(sql).fetchall()
            ms = (time.perf_counter() - t) * 1000; times.append(ms)
            if log is not None:
                log.append([schema["name"], cycle, qn, r + 1, round(ms, 3)])
        total += statistics.median(times); joins += j
        answers[qn] = normalise(res)
        if reference is not None and answers[qn] != reference[qn]:
            valid = False
    con.close()
    return {"K1_ms": round(total, 1), "K2_MB": round(os.path.getsize(schema["db"]) / 2**20, 1),
            "K3_joins": joins, "valid": valid, "answers": answers}

# ======================================================================
# 5. LLM semantic assessment
# ======================================================================
PROMPT = """You are an expert data warehouse architect. Evaluate the SEMANTIC QUALITY of
the following analytical (OLAP) database schema derived from the TPC-H order-processing
database. Do not evaluate performance or storage size.
Study condition: all source information must be preserved, so keeping source columns or
tables is NOT a weakness. Judge how clearly the schema supports analysis.

Score each aspect from 1 to 10 using these anchors:
1-2 = wrong or misleading design, 3-4 = major weaknesses, 5-6 = usable with clear weaknesses,
7-8 = good with minor weaknesses, 9-10 = textbook dimensional design.
1. fact_dimension: facts (measurable events) and dimensions (descriptive context) correctly identified?
2. grain: is the grain of every fact table clear, and are measures of different grains kept apart?
3. hierarchies_time: are meaningful hierarchies and a usable time dimension available?
4. naming: are table and column names clear and unambiguous?
5. redundancy: is the schema free of duplicated or ambiguous attributes and tables?
Then give an overall score from 1 to 10.

Answer ONLY with a JSON object, for example:
{"fact_dimension": 8, "grain": 9, "hierarchies_time": 6, "naming": 7, "redundancy": 7,
 "overall": 7, "justification": "One or two sentences."}

SCHEMA:
"""
ASPECTS = ["fact_dimension", "grain", "hierarchies_time", "naming", "redundancy", "overall"]

def schema_text(schema):
    con = duckdb.connect(schema["db"], read_only=True)
    lines = []
    for (t,) in con.execute("SELECT table_name FROM information_schema.tables ORDER BY table_name").fetchall():
        cs = con.execute("SELECT column_name, data_type FROM information_schema.columns "
                         f"WHERE table_name='{t}' ORDER BY ordinal_position").fetchall()
        n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        lines.append(f"TABLE {t} ({n:,} rows): " + ", ".join(f"{c} {d}" for c, d in cs))
    con.close()
    lines.append("RELATIONSHIPS:")
    for a, b, cond in schema["edges"]:
        lines.append(f"  {schema['nodes'][a]} -> {schema['nodes'][b]} ON {cond}")
    return "\n".join(lines)

def ask_llm(text):
    import anthropic
    client = anthropic.Anthropic()
    msg = client.messages.create(model=os.environ["CLAUDE_MODEL"], max_tokens=400,
                                 messages=[{"role": "user", "content": PROMPT + text}])
    return json.loads(re.search(r"\{.*\}", msg.content[0].text, re.S).group(0))

def load_json(path, default):
    return json.load(open(path, encoding="utf-8")) if os.path.exists(path) else default

def save_json(path, obj):
    json.dump(obj, open(path, "w", encoding="utf-8"), indent=1)

# ======================================================================
# 6. Pareto utilities and search
# ======================================================================
def objectives(r, use_llm):
    o = (r["K1_ms"], r["K2_MB"], r["K3_joins"])
    return o + ((-r["K4"],) if use_llm else ())

def dominates(a, b, use_llm):
    oa, ob = objectives(a, use_llm), objectives(b, use_llm)
    return all(x <= y for x, y in zip(oa, ob)) and any(x < y for x, y in zip(oa, ob))

def pareto(items, use_llm=True):
    return [r for r in items if not any(dominates(o, r, use_llm) for o in items if o is not r)]

def neighbours(cfg):
    for i in range(N):
        n = list(cfg); n[i] = 1 - n[i]; n = tuple(n)
        if is_valid_config(n):
            yield n

def simulate_search(by_cfg, use_llm, budget, start=None):
    """Neighbourhood search of Chapter 3, replayed on already measured candidates,
    so that both variants use identical data and measurement conditions."""
    start = start or tuple([0] * N)
    evaluated, explored = [start], set()
    while len(evaluated) < budget:
        ok = [by_cfg[c] for c in evaluated if by_cfg[c]["admissible"]]
        todo = [r for r in pareto(ok, use_llm) if r["id"] not in explored]
        if not todo:
            break
        cur = min(todo, key=lambda r: r["K1_ms"]); explored.add(cur["id"])
        for n in neighbours(cur["cfg"]):
            if n not in evaluated and len(evaluated) < budget:
                evaluated.append(n)
    ok = [by_cfg[c] for c in evaluated if by_cfg[c]["admissible"]]
    return evaluated, pareto(ok, use_llm)

# ======================================================================
# 7. Commands
# ======================================================================
def cmd_enumerate(args):
    load_dotenv(); os.makedirs(WORK_DIR, exist_ok=True)
    src = src_db(1)
    cache = load_json("llm_cache_v2.json", {}); texts = load_json("schema_texts.json", {})
    print("Reference answers from the original schema ...")
    reference = run_workload(dict(ORIGINAL, db=src))["answers"]
    rows, log = [], []
    configs = all_configs()
    for k, cfg in enumerate(configs, 1):
        t0 = time.time(); path = os.path.join(WORK_DIR, cfg_id(cfg) + ".duckdb")
        sch = build_schema(cfg, path, src)
        m = run_workload(sch, reference, log=log)
        grain = grain_check(path); info, missing = info_check(path, src)
        texts[sch["name"]] = schema_text(sch)
        if sch["name"] not in cache:
            cache[sch["name"]] = ask_llm(texts[sch["name"]]); save_json("llm_cache_v2.json", cache)
        llm = cache[sch["name"]]
        os.remove(path)
        row = {"id": sch["name"], **dict(zip(RULE_IDS, cfg)), "K1_ms": m["K1_ms"], "K2_MB": m["K2_MB"],
               "K3_joins": m["K3_joins"], "K4": llm["overall"], "valid": m["valid"],
               "grain_ok": not grain, "info_preserved": round(info, 3),
               **{f"llm_{a}": llm.get(a) for a in ASPECTS[:-1]},
               "justification": llm.get("justification", "")}
        rows.append(row)
        print(f"{k:>3}/{len(configs)} {row['id']} K1={row['K1_ms']:7.1f} K2={row['K2_MB']:6.1f} "
              f"K3={row['K3_joins']:>2} K4={row['K4']:>2} valid={row['valid']} grain_ok={row['grain_ok']} "
              f"info={row['info_preserved']} ({time.time() - t0:.0f} s)")
    save_json("schema_texts.json", texts)
    print("\nBaselines ...")
    for sch in baselines(1, "baselines_sf1"):
        m = run_workload(sch, reference, log=log)
        grain = grain_check(sch["db"]); info, missing = info_check(sch["db"], src)
        texts[sch["name"]] = schema_text(sch)
        if sch["name"] not in cache:
            cache[sch["name"]] = ask_llm(texts[sch["name"]]); save_json("llm_cache_v2.json", cache)
        llm = cache[sch["name"]]
        rows.append({"id": sch["name"], **{r: "" for r in RULE_IDS}, "K1_ms": m["K1_ms"],
                     "K2_MB": m["K2_MB"], "K3_joins": m["K3_joins"], "K4": llm["overall"],
                     "valid": m["valid"], "grain_ok": not grain, "info_preserved": round(info, 3),
                     **{f"llm_{a}": llm.get(a) for a in ASPECTS[:-1]},
                     "justification": llm.get("justification", "")})
        print(f"    {sch['name']:10s} K1={m['K1_ms']:7.1f} K2={m['K2_MB']:6.1f} K3={m['K3_joins']:>2} "
              f"K4={llm['overall']:>2} grain_ok={not grain} info={info:.3f}"
              + (f"  grain problem: {grain}" if grain else ""))
    save_json("schema_texts.json", texts)
    write_csv("results_enum_sf1.csv", rows)
    write_csv("timings_enum_sf1.csv", log, ["schema", "cycle", "query", "run", "ms"])
    print("\nSaved results_enum_sf1.csv and timings_enum_sf1.csv")

def read_enum():
    rows = list(csv.DictReader(open("results_enum_sf1.csv", encoding="utf-8")))
    for r in rows:
        for k in ("K1_ms", "K2_MB"): r[k] = float(r[k])
        r["K3_joins"] = int(r["K3_joins"]); r["K4"] = int(float(r["K4"]))
        r["valid"] = r["valid"] == "True"; r["grain_ok"] = r["grain_ok"] == "True"
        r["info_preserved"] = float(r["info_preserved"])
        r["is_baseline"] = not r["id"].startswith("S")
        if not r["is_baseline"]:
            r["cfg"] = tuple(int(r[x]) for x in RULE_IDS)
        r["admissible"] = r["valid"] and r["grain_ok"] and r["info_preserved"] >= 0.999
    return rows

def cmd_analyse(args):
    rows = read_enum()
    cands = [r for r in rows if not r["is_baseline"]]
    base = [r for r in rows if r["is_baseline"]]
    adm = [r for r in cands if r["admissible"]]
    by_cfg = {r["cfg"]: r for r in cands}
    print(f"Candidates: {len(cands)}, admissible (valid + grain + information): {len(adm)}")
    print(f"K4 range of candidates: {min(r['K4'] for r in cands)}-{max(r['K4'] for r in cands)}")

    # (a) full enumeration: fronts with and without the LLM criterion
    f3 = pareto(adm, False); f4 = pareto(adm, True)
    s3 = {r["id"] for r in f3}; s4 = {r["id"] for r in f4}
    print(f"\n(a) Full enumeration - front K1-K3: {len(f3)}, front K1-K4: {len(f4)}")
    print(f"    only with LLM: {sorted(s4 - s3)}   only without LLM: {sorted(s3 - s4)}")

    # (b) neighbourhood search with and without the LLM (identical measurements)
    print(f"\n(b) Search with budget {args.budget} (replayed on identical measurements)")
    out = []
    for use_llm in (False, True):
        ev, fr = simulate_search(by_cfg, use_llm, args.budget)
        ids = {r["id"] for r in fr}
        cov = len(ids & s3) / len(s3); cov4 = len(ids & s4) / len(s4)
        best = min(fr, key=lambda r: r["K1_ms"])
        out.append({"variant": "with LLM" if use_llm else "without LLM", "evaluated": len(ev),
                    "front_size": len(fr), "coverage_K1K3_front": round(cov, 2),
                    "coverage_K1K4_front": round(cov4, 2),
                    "best_K1": best["K1_ms"], "best_id": best["id"],
                    "min_K2": min(r["K2_MB"] for r in fr), "min_K3": min(r["K3_joins"] for r in fr),
                    "max_K4": max(r["K4"] for r in fr), "front": " ".join(sorted(r["id"] for r in fr))})
        print(f"    {out[-1]['variant']:12s} evaluated={len(ev)} front={len(fr)} cover K1-K3={cov:.2f} K1-K4={cov4:.2f} "
              f"best={best['id']} {best['K1_ms']} ms  maxK4={out[-1]['max_K4']}")
    # the same comparison repeated from every possible start configuration
    print(f"    Repeated from all {len(by_cfg)} start configurations (mean values):")
    for use_llm in (False, True):
        c3, c4, bk = [], [], []
        for st in by_cfg:
            ev, fr = simulate_search(by_cfg, use_llm, args.budget, st)
            if not fr: continue
            ids = {r["id"] for r in fr}
            c3.append(len(ids & s3) / len(s3)); c4.append(len(ids & s4) / len(s4))
            bk.append(min(r["K1_ms"] for r in fr))
        name = "with LLM" if use_llm else "without LLM"
        out.append({"variant": name + " (all starts, mean)", "evaluated": args.budget, "front_size": "",
                    "coverage_K1K3_front": round(statistics.mean(c3), 2),
                    "coverage_K1K4_front": round(statistics.mean(c4), 2),
                    "best_K1": round(statistics.mean(bk), 1), "best_id": "", "min_K2": "", "min_K3": "",
                    "max_K4": "", "front": f"runs={len(bk)}"})
        print(f"    {name:12s} cover K1-K3={statistics.mean(c3):.2f} K1-K4={statistics.mean(c4):.2f} "
              f"mean best K1={statistics.mean(bk):.1f} ms")
    write_csv("results_search_comparison.csv", out)

    # (c) overall Pareto set including the baselines
    allr = adm + base
    ov = pareto(allr, True)
    print("\n(c) Overall Pareto set (admissible candidates + baselines, K1-K4):")
    for r in sorted(ov, key=lambda r: r["K1_ms"]):
        flag = "" if not r["is_baseline"] else f"  [baseline; grain_ok={r['grain_ok']}, info={r['info_preserved']}]"
        print(f"    {r['id']:10s} K1={r['K1_ms']:7.1f} K2={r['K2_MB']:6.1f} K3={r['K3_joins']:>2} K4={r['K4']:>2}{flag}")
    for r in rows:
        r["front_K1K3"] = r["id"] in s3; r["front_K1K4"] = r["id"] in s4
        r["overall_front"] = r["id"] in {x["id"] for x in ov}
    keep = ["id"] + RULE_IDS + ["K1_ms", "K2_MB", "K3_joins", "K4", "valid", "grain_ok",
                                 "info_preserved", "front_K1K3", "front_K1K4", "overall_front"]
    write_csv("results_fronts.csv", [{k: r.get(k, "") for k in keep} for r in rows])

    # (d) effect of each rule: average change when only that rule is switched on
    print("\n(d) Average effect of switching a single rule on (paired comparison):")
    eff = []
    for i, rid in enumerate(RULE_IDS):
        pairs = [(by_cfg[c], by_cfg[c[:i] + (1,) + c[i + 1:]]) for c in by_cfg
                 if c[i] == 0 and (c[:i] + (1,) + c[i + 1:]) in by_cfg]
        if not pairs: continue
        d = lambda k, rel: statistics.mean(((b[k] - a[k]) / a[k] * 100) if rel else (b[k] - a[k]) for a, b in pairs)
        eff.append({"rule": rid, "pairs": len(pairs), "dK1_%": round(d("K1_ms", 1), 1),
                    "dK2_%": round(d("K2_MB", 1), 1), "dK3": round(d("K3_joins", 0), 1),
                    "dK4": round(d("K4", 0), 2)})
        e = eff[-1]
        print(f"    {rid}: pairs={e['pairs']:>2}  K1 {e['dK1_%']:+6.1f} %  K2 {e['dK2_%']:+6.1f} %  "
              f"K3 {e['dK3']:+5.1f}  K4 {e['dK4']:+5.2f}")
    write_csv("results_rule_effects.csv", eff)
    plot_overall(adm, base, ov)
    print("\nSaved results_search_comparison.csv, results_fronts.csv, results_rule_effects.csv, overall_front.png")

def cmd_consistency(args):
    load_dotenv()
    rows = read_enum(); texts = load_json("schema_texts.json", {})
    fr = [r for r in rows if r.get("id") in texts]
    fr = pareto([r for r in fr if r["admissible"] or r["is_baseline"]], True)
    front = sorted(r["id"] for r in fr if not r["is_baseline"])
    chosen = ["original", "star", "heuristic"] + front[: max(0, args.max_schemas - 3)]
    res = load_json("llm_consistency.json", {})
    for sid in chosen:
        res.setdefault(sid, [])
        while len(res[sid]) < args.repeats:
            res[sid].append(ask_llm(texts[sid])); save_json("llm_consistency.json", res)
    out = []
    for sid in chosen:
        sc = [x["overall"] for x in res[sid]]
        out.append({"schema": sid, "scores": " ".join(map(str, sc)), "mean": round(statistics.mean(sc), 2),
                    "sd": round(statistics.stdev(sc), 2) if len(sc) > 1 else 0, "range": max(sc) - min(sc)})
        print(f"{sid:10s} scores={out[-1]['scores']:12s} mean={out[-1]['mean']:4} sd={out[-1]['sd']:4} range={out[-1]['range']}")
    write_csv("results_llm_consistency.csv", out)

def cmd_setup(args):
    con = duckdb.connect(src_db(args.sf))
    con.execute("INSTALL tpch; LOAD tpch;"); t = time.time()
    con.execute(f"CALL dbgen(sf={args.sf});"); con.close()
    print(f"Generated {src_db(args.sf)} in {time.time() - t:.1f} s")

def t975(n):
    table = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}
    return table.get(n, 1.96)

def cmd_remeasure(args):
    rows = read_enum()          # results_fronts.csv is written by the 'analyse' command
    fr = list(csv.DictReader(open("results_fronts.csv", encoding="utf-8")))
    ov_ids = [r["id"] for r in fr if r["overall_front"] == "True"]
    cand_ids = sorted([i for i in ov_ids if i.startswith("S")],
                      key=lambda i: next(r["K1_ms"] for r in rows if r["id"] == i))[: args.max_candidates]
    folder = f"remeasure_sf{args.sf}"; os.makedirs(folder, exist_ok=True)
    src = src_db(args.sf)
    print(f"Building {len(cand_ids)} candidates + 3 baselines on {src} ...")
    schemas = baselines(args.sf, folder)
    for cid in cand_ids:
        cfg = tuple(int(ch) for ch in cid[1:])
        schemas.append(build_schema(cfg, os.path.join(folder, cid + ".duckdb"), src))
    reference = run_workload(schemas[0], runs=1)["answers"]
    log, totals = [], {s["name"]: [] for s in schemas}
    for cyc in range(1, args.cycles + 1):
        order = schemas[:]; random.Random(cyc).shuffle(order)
        for s in order:
            m = run_workload(s, reference, runs=3, log=log, cycle=cyc)
            if not m["valid"]:
                print(f"WARNING: {s['name']} returned different answers")
            totals[s["name"]].append(m["K1_ms"])
        print(f"cycle {cyc}/{args.cycles} done, order: {' '.join(s['name'] for s in order)}")
    write_csv(f"timings_remeasure_sf{args.sf}.csv", log, ["schema", "cycle", "query", "run", "ms"])
    out = []
    for s in schemas:
        v = totals[s["name"]]; mu = statistics.mean(v); sd = statistics.stdev(v) if len(v) > 1 else 0
        hw = t975(len(v) - 1) * sd / math.sqrt(len(v))
        out.append({"schema": s["name"], "cycles": len(v), "mean_ms": round(mu, 1), "sd_ms": round(sd, 1),
                    "ci95_low": round(mu - hw, 1), "ci95_high": round(mu + hw, 1),
                    "size_MB": round(os.path.getsize(s["db"]) / 2**20, 1)})
    out.sort(key=lambda r: r["mean_ms"])
    print(f"\nTotal time of the 13 queries over {args.cycles} cycles (SF {args.sf}, {THREADS} threads):")
    for r in out:
        print(f"  {r['schema']:10s} mean={r['mean_ms']:8.1f} ms  sd={r['sd_ms']:6.1f}  "
              f"95% CI [{r['ci95_low']}, {r['ci95_high']}]  size={r['size_MB']} MB")
    write_csv(f"results_remeasure_sf{args.sf}.csv", out)
    plot_remeasure(out, args.sf)
    if not args.keep:
        for s in schemas[1:]:
            os.remove(s["db"])

# ======================================================================
# 8. Output helpers and charts
# ======================================================================
def write_csv(path, rows, header=None):
    with open(path, "w", newline="", encoding="utf-8") as f:
        if header:
            w = csv.writer(f); w.writerow(header); w.writerows(rows)
        else:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)

INK, MUTED, GRID, BLUE, ORANGE = "#0b0b0b", "#9a9893", "#e5e4e0", "#2a78d6", "#eb6834"

def _style(ax):
    ax.grid(color=GRID, linewidth=0.8)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    ax.tick_params(colors=INK, labelsize=8)

def plot_overall(adm, base, ov):
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    ids = {r["id"] for r in ov}
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), dpi=200)
    for ax, (k, lab) in zip(axes, [("K4", "K4: semantic score (LLM)"), ("K2_MB", "K2: storage, MB")]):
        d = [r for r in adm if r["id"] not in ids]; p = [r for r in adm if r["id"] in ids]
        ax.scatter([r["K1_ms"] for r in d], [r[k] for r in d], s=30, color=MUTED, edgecolor="white",
                   linewidth=1.2, label="Dominated candidate", zorder=2)
        ax.scatter([r["K1_ms"] for r in p], [r[k] for r in p], s=50, color=BLUE, edgecolor="white",
                   linewidth=1.2, label="Overall Pareto-optimal candidate", zorder=3)
        ax.scatter([r["K1_ms"] for r in base], [r[k] for r in base], s=60, marker="D", color=ORANGE,
                   edgecolor="white", linewidth=1.2, label="Baseline schema", zorder=4)
        for b in base:
            ax.annotate(b["id"], (b["K1_ms"], b[k]), textcoords="offset points", xytext=(5, 5), fontsize=7, color=INK)
        ax.set_xlabel("K1: total query time, ms", fontsize=8, color=INK); ax.set_ylabel(lab, fontsize=8, color=INK)
        _style(ax)
    axes[0].set_ylim(0, 10.5); axes[0].legend(frameon=False, fontsize=7, loc="lower right")
    fig.tight_layout(); fig.savefig("overall_front.png")

def plot_remeasure(out, sf):
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(7, 0.45 * len(out) + 1), dpi=200)
    names = [r["schema"] for r in out][::-1]; means = [r["mean_ms"] for r in out][::-1]
    err = [r["mean_ms"] - r["ci95_low"] for r in out][::-1]
    colors = [ORANGE if not n.startswith("S") else BLUE for n in names]
    ax.barh(names, means, xerr=err, color=colors, height=0.6, error_kw={"ecolor": INK, "capsize": 3, "lw": 1})
    ax.set_xlabel(f"Total time of 13 queries, ms (mean and 95% CI, SF {sf})", fontsize=8, color=INK)
    _style(ax); fig.tight_layout(); fig.savefig(f"remeasure_sf{sf}.png")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["enumerate", "analyse", "consistency", "setup", "remeasure"])
    ap.add_argument("--sf", type=int, default=1)
    ap.add_argument("--budget", type=int, default=25)
    ap.add_argument("--cycles", type=int, default=5)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--max-schemas", type=int, default=10)
    ap.add_argument("--max-candidates", type=int, default=6)
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()
    {"enumerate": cmd_enumerate, "analyse": cmd_analyse, "consistency": cmd_consistency,
     "setup": cmd_setup, "remeasure": cmd_remeasure}[a.command](a)
