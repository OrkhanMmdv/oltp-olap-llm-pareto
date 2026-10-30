"""
Additional verification for the second version of the prototype (supervisor's comments).

  python verify.py checks             reconstruction check (EXCEPT ALL in both directions)
                                      and data-based grain check for all schemas (SF 1)
  python verify.py remeasure --sf 1   repeated measurements incl. the full manual star schema
  python verify.py remeasure --sf 5   the same on the larger dataset

Requires prototype2.py, workload.py and tpch_sf<N>.duckdb in the same folder.
"""
import argparse, math, os, random, statistics
import duckdb
import prototype2 as P
from workload import STAR, to_sql, QUERIES

FOLDER = "verify_dbs"
FRONT6 = ["S1011101", "S1111111", "S1111101", "S1110101", "S1010101", "S1001001"]
SRC_TABLES = ["lineitem", "orders", "customer", "supplier", "part", "partsupp", "nation", "region"]
KEYS = P.KEYS

# ======================================================================
# 1. Full manual star schema: star design, all information, sorted by date
# ======================================================================
def build_star_full(src, dst):
    if os.path.exists(dst): os.remove(dst)
    con = duckdb.connect(dst); con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
    # limit memory and allow spilling to disk, so that the sorted fact table can be built at SF 5
    os.makedirs("duckdb_tmp", exist_ok=True)
    con.execute("SET memory_limit = '6GB'"); con.execute("SET threads = 4")
    con.execute("SET temp_directory = 'duckdb_tmp'")
    con.execute("""CREATE TABLE dim_date AS SELECT DISTINCT
        CAST(strftime(o_orderdate, '%Y%m%d') AS INTEGER) AS d_datekey, o_orderdate AS d_date,
        year(o_orderdate) AS d_year, quarter(o_orderdate) AS d_quarter, month(o_orderdate) AS d_month,
        year(o_orderdate) * 100 + month(o_orderdate) AS d_yearmonthnum,
        weekofyear(o_orderdate) AS d_weeknuminyear, dayname(o_orderdate) AS d_dayofweek
        FROM src.orders""")
    for t, a in (("customer", "c"), ("supplier", "s")):
        con.execute(f"""CREATE TABLE dim_{t} AS SELECT {a}.*, n.n_name AS {a}_nation, r.r_name AS {a}_region
            FROM src.{t} {a} JOIN src.nation n ON {a}.{a}_nationkey = n.n_nationkey
            JOIN src.region r ON n.n_regionkey = r.r_regionkey""")
    con.execute("CREATE TABLE dim_part AS SELECT * FROM src.part")
    con.execute("CREATE TABLE dim_nation AS SELECT * FROM src.nation")
    con.execute("CREATE TABLE dim_region AS SELECT * FROM src.region")
    con.execute("""CREATE TABLE fact_lineorder AS SELECT
        l.l_orderkey AS lo_orderkey, l.l_linenumber AS lo_linenumber, o.o_custkey AS lo_custkey,
        l.l_partkey AS lo_partkey, l.l_suppkey AS lo_suppkey,
        CAST(strftime(o.o_orderdate, '%Y%m%d') AS INTEGER) AS lo_orderdatekey,
        o.o_orderstatus AS lo_orderstatus, o.o_orderpriority AS lo_orderpriority,
        o.o_clerk AS lo_clerk, o.o_shippriority AS lo_shippriority,
        l.l_quantity AS lo_quantity, l.l_extendedprice AS lo_extendedprice,
        l.l_discount AS lo_discount, l.l_tax AS lo_tax,
        l.l_extendedprice * (1 - l.l_discount) AS lo_revenue,
        ps.ps_supplycost * l.l_quantity AS lo_supplycost,
        l.l_returnflag AS lo_returnflag, l.l_linestatus AS lo_linestatus,
        l.l_shipdate AS lo_shipdate, l.l_commitdate AS lo_commitdate, l.l_receiptdate AS lo_receiptdate,
        l.l_shipinstruct AS lo_shipinstruct, l.l_shipmode AS lo_shipmode, l.l_comment AS lo_comment
        FROM src.lineitem l JOIN src.orders o ON l.l_orderkey = o.o_orderkey
        JOIN src.partsupp ps ON l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey
        ORDER BY lo_orderdatekey""")
    con.execute("CREATE TABLE fact_orders AS SELECT o_orderkey, o_totalprice, o_comment FROM src.orders")
    con.execute("CREATE TABLE fact_partsupp AS SELECT * FROM src.partsupp")
    con.execute("CHECKPOINT"); con.execute("DETACH src"); con.close()
    return dict(STAR, name="star_full", db=dst)

# ======================================================================
# 2. Control schemas (to show that the checks detect errors)
# ======================================================================
def build_control_lossy(src, dst):
    """Candidate S0000000 with one customer row removed (information loss)."""
    P.build_schema((0,) * 7, dst, src)
    con = duckdb.connect(dst)
    con.execute("DELETE FROM dim_customer WHERE c_custkey = (SELECT MIN(c_custkey) FROM dim_customer)")
    con.close()

def build_control_v1(src, dst):
    """First-version rule R1: o_totalprice copied into the order-line fact table."""
    P.build_schema((1, 0, 0, 0, 0, 0, 0), dst, src)
    con = duckdb.connect(dst); con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
    con.execute("""CREATE TABLE f2 AS SELECT f.*, o.o_totalprice FROM fact_lineorder f
                   JOIN src.orders o ON f.l_orderkey = o.o_orderkey""")
    con.execute("DROP TABLE fact_lineorder"); con.execute("ALTER TABLE f2 RENAME TO fact_lineorder")
    con.execute("CHECKPOINT"); con.execute("DETACH src"); con.close()

# ======================================================================
# 3. Reconstruction of every source table from a schema
# ======================================================================
def cols_of(con, catalog, table):
    return [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_catalog = ? AND table_name = ? "
        "ORDER BY ordinal_position", [catalog, table]).fetchall()]

HOME = {"lineitem": ["fact_lineorder"], "orders": ["fact_orders", "fact_lineorder"],
        "customer": ["dim_customer"], "supplier": ["dim_supplier"], "part": ["dim_part"],
        "partsupp": ["fact_partsupp"], "nation": ["dim_nation"], "region": ["dim_region"]}

def rec_candidate(con, cat):
    """Generic reconstruction for candidates (they keep the source column names)."""
    tables = {t: cols_of(con, cat, t) for (t,) in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = ?", [cat]).fetchall()}
    out = {}
    for t in SRC_TABLES:
        src_cols = cols_of(con, "src", t)
        base = HOME[t][0]; key = KEYS[t]
        sel, joins = [], []
        side = f"{t}_comments"
        for c in src_cols:
            if c in tables.get(base, []):
                sel.append(f"b.{c}")
            elif t == "orders" and c in tables.get("fact_lineorder", []):
                sel.append(f"lo.{c}")
                if not any("lo" == j[0] for j in joins):
                    desc = [x for x in src_cols if x in tables["fact_lineorder"] and x != "o_orderkey"]
                    joins.append(("lo", f"LEFT JOIN (SELECT DISTINCT l_orderkey AS o_orderkey, "
                                        f"{', '.join(desc)} FROM {cat}.fact_lineorder) lo "
                                        f"ON b.o_orderkey = lo.o_orderkey"))
            elif c in tables.get(side, []):
                sel.append(f"cm.{c}")
                if not any("cm" == j[0] for j in joins):
                    cond = " AND ".join(f"b.{k} = cm.{k}" for k in key)
                    joins.append(("cm", f"LEFT JOIN {cat}.{side} cm ON {cond}"))
        cov = [c for c in src_cols if c in tables.get(base, []) or c in tables.get(side, [])
               or (t == "orders" and c in tables.get("fact_lineorder", []))]
        out[t] = (cov, f"SELECT {', '.join(sel)} FROM {cat}.{base} b " + " ".join(j[1] for j in joins))
    return out

def rec_heuristic(con, cat):
    def c(t): return cols_of(con, "src", t)
    def pick(t, frm, distinct=False):
        return (c(t), f"SELECT {'DISTINCT ' if distinct else ''}{', '.join(c(t))} FROM {frm}")
    nat = ", ".join(c("nation")); reg = ", ".join(c("region"))
    return {"lineitem": pick("lineitem", f"{cat}.fact_lineitem"),
            "orders": pick("orders", f"{cat}.dim_orders"),
            "customer": pick("customer", f"{cat}.dim_orders", True),
            "supplier": pick("supplier", f"{cat}.dim_supplier"),
            "part": pick("part", f"{cat}.dim_part"),
            "partsupp": pick("partsupp", f"{cat}.dim_partsupp"),
            "nation": (c("nation"), f"SELECT DISTINCT * FROM (SELECT {nat} FROM {cat}.dim_orders "
                                    f"UNION ALL SELECT {nat} FROM {cat}.dim_supplier)"),
            "region": (c("region"), f"SELECT DISTINCT * FROM (SELECT {reg} FROM {cat}.dim_orders "
                                    f"UNION ALL SELECT {reg} FROM {cat}.dim_supplier)")}

def rec_star(con, cat):
    """Manual star schema: stored columns plus columns that can be derived."""
    f = f"{cat}.fact_lineorder"
    return {
        "lineitem": (["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
                      "l_extendedprice", "l_discount", "l_tax", "l_shipmode"],
                     f"SELECT lo_orderkey, lo_partkey, lo_suppkey, lo_linenumber, lo_quantity, "
                     f"lo_extendedprice, lo_discount, lo_tax, lo_shipmode FROM {f}"),
        "orders": (["o_orderkey", "o_custkey", "o_orderdate", "o_orderpriority", "o_shippriority"],
                   f"SELECT DISTINCT f.lo_orderkey, f.lo_custkey, d.d_date, f.lo_orderpriority, "
                   f"f.lo_shippriority FROM {f} f JOIN {cat}.dim_date d ON f.lo_orderdatekey = d.d_datekey"),
        "customer": (["c_custkey", "c_name", "c_address", "c_phone", "c_mktsegment"],
                     f"SELECT c_custkey, c_name, c_address, c_phone, c_mktsegment FROM {cat}.dim_customer"),
        "supplier": (["s_suppkey", "s_name", "s_address", "s_phone"],
                     f"SELECT s_suppkey, s_name, s_address, s_phone FROM {cat}.dim_supplier"),
        "part": (["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container"],
                 f"SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container FROM {cat}.dim_part"),
        "partsupp": (["ps_partkey", "ps_suppkey", "ps_supplycost"],
                     f"SELECT DISTINCT lo_partkey, lo_suppkey, "
                     f"CAST(ROUND(lo_supplycost / lo_quantity, 2) AS DECIMAL(15,2)) FROM {f}"),
        "nation": (["n_name"], f"SELECT c_nation FROM {cat}.dim_customer UNION "
                               f"SELECT s_nation FROM {cat}.dim_supplier"),
        "region": (["r_name"], f"SELECT c_region FROM {cat}.dim_customer UNION "
                               f"SELECT s_region FROM {cat}.dim_supplier")}

def rec_star_full(con, cat):
    f = f"{cat}.fact_lineorder"
    li = ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice",
          "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
          "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"]
    def c(t): return cols_of(con, "src", t)
    def plain(t, tab): return (c(t), f"SELECT {', '.join(c(t))} FROM {cat}.{tab}")
    return {
        "lineitem": (li, "SELECT " + ", ".join("lo_" + x[2:] for x in li) + f" FROM {f}"),
        "orders": (c("orders"),
                   f"SELECT o.o_orderkey, x.lo_custkey, x.lo_orderstatus, o.o_totalprice, x.d_date, "
                   f"x.lo_orderpriority, x.lo_clerk, x.lo_shippriority, o.o_comment FROM {cat}.fact_orders o "
                   f"LEFT JOIN (SELECT DISTINCT f.lo_orderkey, f.lo_custkey, f.lo_orderstatus, d.d_date, "
                   f"f.lo_orderpriority, f.lo_clerk, f.lo_shippriority FROM {f} f JOIN {cat}.dim_date d "
                   f"ON f.lo_orderdatekey = d.d_datekey) x ON o.o_orderkey = x.lo_orderkey"),
        "customer": plain("customer", "dim_customer"), "supplier": plain("supplier", "dim_supplier"),
        "part": plain("part", "dim_part"), "partsupp": plain("partsupp", "fact_partsupp"),
        "nation": plain("nation", "dim_nation"), "region": plain("region", "dim_region")}

def rec_original(con, cat):
    return {t: (cols_of(con, "src", t), f"SELECT * FROM {cat}.{t}") for t in SRC_TABLES}

def reconstruction(db, src, kind):
    con = duckdb.connect(db, read_only=True)
    cat = con.execute("SELECT current_database()").fetchone()[0]
    con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
    recs = {"candidate": rec_candidate, "heuristic": rec_heuristic, "star": rec_star,
            "star_full": rec_star_full, "original": rec_original}[kind](con, cat)
    rows = []
    for t in SRC_TABLES:
        cov, sql = recs[t]
        src_all = cols_of(con, "src", t)
        n_src = con.execute(f"SELECT COUNT(*) FROM src.{t}").fetchone()[0]
        proj = f"SELECT {', '.join(cov)} FROM src.{t}"
        n_rec = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
        missing = con.execute(f"SELECT COUNT(*) FROM ({proj} EXCEPT ALL SELECT * FROM ({sql}))").fetchone()[0]
        extra = con.execute(f"SELECT COUNT(*) FROM (SELECT * FROM ({sql}) EXCEPT ALL {proj})").fetchone()[0]
        key = [k for k in KEYS[t] if k in cov]
        if key:
            keyexpr = ", ".join(f"c{i}" for i in range(len(key)))
            aliased = f"SELECT * FROM ({sql}) AS r({', '.join('c%d' % i for i in range(len(cov)))})"
            idx = [cov.index(k) for k in key]
            kx = ", ".join(f"c{i}" for i in idx)
            dup = con.execute(f"SELECT COUNT(*) - COUNT(DISTINCT ({kx})) FROM ({aliased})").fetchone()[0]
        else:
            dup = None
        rows.append({"table": t, "columns_source": len(src_all), "columns_recovered": len(cov),
                     "rows_source": n_src, "rows_reconstructed": n_rec,
                     "missing_rows": missing, "extra_rows": extra, "duplicate_keys": dup,
                     "exact": len(cov) == len(src_all) and missing == 0 and extra == 0 and not dup})
    con.close()
    return rows

# ======================================================================
# 4. Data-based grain check
# ======================================================================
MEASURES = ["l_quantity", "l_extendedprice", "l_discount", "l_tax", "lo_quantity", "lo_extendedprice",
            "lo_discount", "lo_tax", "lo_revenue", "lo_supplycost", "o_totalprice", "c_acctbal",
            "s_acctbal", "ps_availqty", "ps_supplycost", "p_retailprice"]
GRAIN_KEYS = [("order line", ["l_orderkey", "l_linenumber"]), ("order line", ["lo_orderkey", "lo_linenumber"]),
              ("part-supplier", ["ps_partkey", "ps_suppkey"]), ("order", ["o_orderkey"]),
              ("customer", ["c_custkey"]), ("supplier", ["s_suppkey"]), ("part", ["p_partkey"]),
              ("nation", ["n_nationkey"]), ("region", ["r_regionkey"]), ("date", ["d_datekey"])]
COARSER = [("order", ["l_orderkey"]), ("order", ["lo_orderkey"]), ("customer", ["o_custkey"]),
           ("customer", ["lo_custkey"]), ("customer", ["c_custkey"]), ("supplier", ["l_suppkey"]),
           ("supplier", ["lo_suppkey"]), ("part", ["l_partkey"]), ("part", ["lo_partkey"]),
           ("part-supplier", ["l_partkey", "l_suppkey"]), ("part-supplier", ["lo_partkey", "lo_suppkey"]),
           ("date", ["o_orderdate"]), ("date", ["lo_orderdatekey"]), ("nation", ["c_nationkey"]),
           ("nation", ["s_nationkey"])]

def grain_check2(db):
    """1) the declared grain key of every table must be unique;
       2) a measure must not be repeated: it is flagged if it is constant within every group of a
          coarser key that has more than one row (i.e. it belongs to a coarser grain)."""
    con = duckdb.connect(db, read_only=True)
    out = []
    for (t,) in con.execute("SELECT table_name FROM information_schema.tables ORDER BY 1").fetchall():
        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?", [t]).fetchall()]
        grain, key = next(((g, k) for g, k in GRAIN_KEYS if all(c in cols for c in k)), (None, None))
        if key is None:
            continue
        n, nd = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT ({', '.join(key)})) FROM {t}").fetchone()
        meas = [m for m in MEASURES if m in cols]
        repeated = []
        for g, ck in COARSER:
            if not all(c in cols for c in ck) or set(ck) >= set(key) or not meas:
                continue
            agg = ", ".join(f"COUNT(DISTINCT {m}) AS d{i}" for i, m in enumerate(meas))
            chk = ", ".join(f"SUM(CASE WHEN cnt > 1 AND d{i} > 1 THEN 1 ELSE 0 END)" for i in range(len(meas)))
            r = con.execute(f"SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END), {chk} FROM "
                            f"(SELECT COUNT(*) AS cnt, {agg} FROM {t} GROUP BY {', '.join(ck)})").fetchone()
            multi, viol = r[0] or 0, r[1:]
            for m, v in zip(meas, viol):
                msg = f"{m} repeated per {g}"
                if multi > 0 and (v or 0) == 0 and msg not in repeated:
                    repeated.append(msg)
        out.append({"table": t, "grain": grain, "key": "+".join(key), "rows": n,
                    "key_unique": n == nd, "repeated_measures": "; ".join(repeated)})
    con.close()
    return out

# ======================================================================
# 5. Commands
# ======================================================================
def cmd_checks(args):
    os.makedirs(FOLDER, exist_ok=True)
    src = P.src_db(1)
    targets = [("original", "original", src, None),
               ("star", "star", os.path.join(FOLDER, "star.duckdb"), P.build_star),
               ("heuristic", "heuristic", os.path.join(FOLDER, "heuristic.duckdb"), P.build_heuristic),
               ("star_full", "star_full", os.path.join(FOLDER, "star_full.duckdb"), build_star_full),
               ("control_lossy", "candidate", os.path.join(FOLDER, "control_lossy.duckdb"), build_control_lossy),
               ("control_v1_R1", "candidate", os.path.join(FOLDER, "control_v1.duckdb"), build_control_v1)]
    cfgs = P.all_configs() if args.all else [tuple(int(c) for c in s[1:]) for s in FRONT6]
    for cfg in cfgs:
        targets.append((P.cfg_id(cfg), "candidate", os.path.join(FOLDER, P.cfg_id(cfg) + ".duckdb"), cfg))
    detail, summary, grains = [], [], []
    for k, (name, kind, path, builder) in enumerate(targets, 1):
        if builder is not None:
            if isinstance(builder, tuple): P.build_schema(builder, path, src)
            else: builder(src, path)
        rec = reconstruction(path, src, kind)
        gr = grain_check2(path)
        for r in rec: detail.append({"schema": name, **r})
        for g in gr: grains.append({"schema": name, **g})
        cols = sum(r["columns_recovered"] for r in rec)
        s = {"schema": name, "tables_exact": sum(r["exact"] for r in rec),
             "columns_recovered": cols, "columns_share_%": round(100 * cols / 61, 1),
             "missing_rows": sum(r["missing_rows"] for r in rec), "extra_rows": sum(r["extra_rows"] for r in rec),
             "keys_unique": all(g["key_unique"] for g in gr),
             "repeated_measures": " | ".join(f"{g['table']}: {g['repeated_measures']}" for g in gr if g["repeated_measures"])}
        summary.append(s)
        print(f"{k:>3}/{len(targets)} {name:14s} exact tables {s['tables_exact']}/8, columns {cols}/61, "
              f"missing {s['missing_rows']}, extra {s['extra_rows']}, keys unique {s['keys_unique']}"
              + (f", REPEATED: {s['repeated_measures']}" if s["repeated_measures"] else ""))
        if builder is not None and kind == "candidate":
            os.remove(path)
    P.write_csv("results_reconstruction_detail.csv", detail)
    P.write_csv("results_reconstruction_summary.csv", summary)
    P.write_csv("results_grain_check2.csv", grains)
    print("\nSaved results_reconstruction_summary.csv, results_reconstruction_detail.csv, results_grain_check2.csv")

def cmd_remeasure(args):
    P.load_dotenv()
    folder = f"remeasure2_sf{args.sf}"; os.makedirs(folder, exist_ok=True)
    src = P.src_db(args.sf)
    print(f"Building 6 candidates + 4 baselines on {src} ...")
    schemas = P.baselines(args.sf, folder)
    schemas.append(build_star_full(src, os.path.join(folder, "star_full.duckdb")))
    for cid in FRONT6:
        schemas.append(P.build_schema(tuple(int(c) for c in cid[1:]), os.path.join(folder, cid + ".duckdb"), src))
    cache = P.load_json("llm_cache_v2.json", {})
    k3, k4 = {}, {}
    for s in schemas:
        k3[s["name"]] = sum(to_sql(s, q)[1] for q in QUERIES)
        if s["name"] not in cache:
            cache[s["name"]] = P.ask_llm(P.schema_text(s)); P.save_json("llm_cache_v2.json", cache)
        k4[s["name"]] = cache[s["name"]]["overall"]
    reference = P.run_workload(schemas[0], runs=1)["answers"]
    log, totals = [], {s["name"]: [] for s in schemas}
    for cyc in range(1, args.cycles + 1):
        order = schemas[:]; random.Random(100 + cyc).shuffle(order)
        for s in order:
            m = P.run_workload(s, reference, runs=3, log=log, cycle=cyc)
            if not m["valid"]: print(f"WARNING: {s['name']} returned different answers")
            totals[s["name"]].append(m["K1_ms"])
        print(f"cycle {cyc}/{args.cycles} done")
    P.write_csv(f"timings_remeasure2_sf{args.sf}.csv", log, ["schema", "cycle", "query", "run", "ms"])
    out = []
    for s in schemas:
        v = totals[s["name"]]; mu = statistics.mean(v); sd = statistics.stdev(v)
        hw = P.t975(len(v) - 1) * sd / math.sqrt(len(v))
        out.append({"schema": s["name"], "mean_ms": round(mu, 1), "sd_ms": round(sd, 1),
                    "ci95_low": round(mu - hw, 1), "ci95_high": round(mu + hw, 1),
                    "K2_MB": round(os.path.getsize(s["db"]) / 2**20, 1), "K3": k3[s["name"]], "K4": k4[s["name"]]})
    def dom(a, b):
        x = (a["mean_ms"], a["K2_MB"], a["K3"], -a["K4"]); y = (b["mean_ms"], b["K2_MB"], b["K3"], -b["K4"])
        return all(p <= q for p, q in zip(x, y)) and x != y
    for r in out:
        r["pareto_K1K4"] = not any(dom(o, r) for o in out if o is not r)
        r["dominated_by"] = " ".join(o["schema"] for o in out if dom(o, r))
    out.sort(key=lambda r: r["mean_ms"])
    print(f"\nSF {args.sf}: mean of {args.cycles} cycles, 95 % CI, storage, joins, semantic score")
    for r in out:
        print(f"  {r['schema']:10s} {r['mean_ms']:8.1f} ms [{r['ci95_low']}, {r['ci95_high']}]  "
              f"{r['K2_MB']:7.1f} MB  K3={r['K3']:>2}  K4={r['K4']}  Pareto={r['pareto_K1K4']}"
              + (f"  dominated by: {r['dominated_by']}" if r["dominated_by"] else ""))
    P.write_csv(f"results_remeasure2_sf{args.sf}.csv", out)
    P.plot_remeasure(out, args.sf)
    os.replace(f"remeasure_sf{args.sf}.png", f"remeasure2_sf{args.sf}.png")
    if not args.keep:
        for s in schemas[1:]: os.remove(s["db"])
    print(f"Saved results_remeasure2_sf{args.sf}.csv and remeasure2_sf{args.sf}.png")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["checks", "remeasure"])
    ap.add_argument("--sf", type=int, default=1)
    ap.add_argument("--cycles", type=int, default=5)
    ap.add_argument("--all", action="store_true", help="check all 96 candidates (default: 6 fastest)")
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()
    {"checks": cmd_checks, "remeasure": cmd_remeasure}[a.command](a)
