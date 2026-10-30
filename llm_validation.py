"""
Validation of the LLM assessment against independent expert evaluation (supervisor's comment).

  python llm_validation.py prepare    build 9 schemas, write basic and rich descriptions,
                                      blind expert form (schemas labelled A-I)
  python llm_validation.py llm        LLM scores with basic and rich descriptions (3 repeats each)
  python llm_validation.py compare    compare LLM scores with the expert scores

The expert fills expert_scores.csv (and optionally expert_scores_2.csv for a second expert).
"""
import argparse, csv, json, os, random, re, statistics
import duckdb
import prototype2 as P
import verify as V

FOLDER = "llmval_dbs"
SCHEMAS = ["original", "star", "star_full", "heuristic", "S0000000", "S0110000",
           "S1011100", "S1011101", "S1011111"]
ASPECTS = ["fact_dimension", "grain", "hierarchies_time", "naming", "redundancy", "overall"]
AGGREGATION = {
    "l_quantity": "additive", "lo_quantity": "additive", "l_extendedprice": "additive",
    "lo_extendedprice": "additive", "lo_revenue": "additive", "lo_supplycost": "additive",
    "o_totalprice": "additive only at the order grain",
    "ps_availqty": "additive over parts and suppliers",
    "c_acctbal": "semi-additive (must not be summed over time)",
    "s_acctbal": "semi-additive (must not be summed over time)",
    "l_discount": "non-additive (rate, use average)", "lo_discount": "non-additive (rate, use average)",
    "l_tax": "non-additive (rate, use average)", "lo_tax": "non-additive (rate, use average)",
    "ps_supplycost": "non-additive (unit cost)", "p_retailprice": "non-additive (unit price)"}
FK = {"l_orderkey": "o_orderkey", "lo_orderkey": "o_orderkey", "l_partkey": "p_partkey",
      "lo_partkey": "p_partkey", "l_suppkey": "s_suppkey", "lo_suppkey": "s_suppkey",
      "o_custkey": "c_custkey", "lo_custkey": "c_custkey", "c_nationkey": "n_nationkey",
      "s_nationkey": "n_nationkey", "n_regionkey": "r_regionkey", "ps_partkey": "p_partkey",
      "ps_suppkey": "s_suppkey", "o_orderdatekey": "d_datekey", "lo_orderdatekey": "d_datekey"}
COMPOSITE_FK = [(("l_partkey", "l_suppkey"), ("ps_partkey", "ps_suppkey")),
                (("lo_partkey", "lo_suppkey"), ("ps_partkey", "ps_suppkey"))]

def build_all():
    os.makedirs(FOLDER, exist_ok=True)
    src = P.src_db(1); out = {}
    for name in SCHEMAS:
        path = os.path.join(FOLDER, name + ".duckdb")
        if name == "original":
            out[name] = (dict(P.ORIGINAL, db=src), None)
        elif name == "star":
            out[name] = (P.build_star(src, path), None)
        elif name == "heuristic":
            out[name] = (P.build_heuristic(src, path), None)
        elif name == "star_full":
            out[name] = (V.build_star_full(src, path), "fact_lineorder sorted by lo_orderdatekey")
        else:
            cfg = tuple(int(c) for c in name[1:])
            out[name] = (P.build_schema(cfg, path, src), "fact_lineorder sorted by o_orderdate" if cfg[6] else None)
    return out

def rich_text(schema, sort_note):
    """Description with keys, grain, measures with aggregation, relationships and physical order."""
    con = duckdb.connect(schema["db"], read_only=True)
    tabs = [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables ORDER BY 1").fetchall()]
    info = {}
    for t in tabs:
        cols = con.execute("SELECT column_name, data_type FROM information_schema.columns "
                           f"WHERE table_name = '{t}' ORDER BY ordinal_position").fetchall()
        names = [c for c, _ in cols]
        grain, key = next(((g, k) for g, k in V.GRAIN_KEYS if all(c in names for c in k)), (None, None))
        if t.endswith("_comments"):
            grain = f"one row per row of the {t[:-9]} table (1:1 extension)"
        n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        uniq = None
        if key:
            uniq = con.execute(f"SELECT COUNT(*) = COUNT(DISTINCT ({', '.join(key)})) FROM {t}").fetchone()[0]
        info[t] = dict(cols=cols, names=names, grain=grain, key=key, rows=n, unique=uniq)
    con.close()
    owner = {}
    for t, i in info.items():
        if i["key"] and len(i["key"]) == 1 and not t.endswith("_comments"):
            owner.setdefault(i["key"][0], t)
        if i["key"] and len(i["key"]) == 2 and i["key"][0] == "ps_partkey" and not t.endswith("_comments"):
            owner.setdefault(("ps_partkey", "ps_suppkey"), t)
    lines = []
    for t, i in info.items():
        lines.append(f"TABLE {t} ({i['rows']:,} rows)")
        lines.append("  columns: " + ", ".join(f"{c} {d}" for c, d in i["cols"]))
        if i["key"]:
            lines.append(f"  primary key: {', '.join(i['key'])} (verified unique: {'yes' if i['unique'] else 'no'})")
        if i["grain"]:
            lines.append(f"  grain: {i['grain'] if 'row' in str(i['grain']) else 'one row per ' + i['grain']}")
        meas = [c for c in i["names"] if c in AGGREGATION]
        if meas:
            lines.append("  measures: " + "; ".join(f"{m} ({AGGREGATION[m]})" for m in meas))
        rels = []
        for c in i["names"]:
            tgt = FK.get(c)
            if tgt and tgt in owner and owner[tgt] != t and not (i["key"] == [c]):
                rels.append(f"{c} -> {owner[tgt]}.{tgt} (many-to-one)")
        for (a, b), tk in COMPOSITE_FK:
            if a in i["names"] and b in i["names"] and tk in owner and owner[tk] != t:
                rels.append(f"({a}, {b}) -> {owner[tk]}.({', '.join(tk)}) (many-to-one)")
        if t.endswith("_comments"):
            base = next((x for x in info if x.endswith(t[:-9]) and x != t), None)
            if base: rels.append(f"{', '.join(i['key'] or [])} -> {base} (one-to-one)")
        if rels:
            lines.append("  relationships: " + "; ".join(rels))
        if sort_note and sort_note.startswith(t):
            lines.append(f"  physical storage: rows are stored sorted by {sort_note.split(' by ')[1]}")
    return "\n".join(lines)

def cmd_prepare(args):
    built = build_all()
    labels = list("ABCDEFGHI"); random.Random(7).shuffle(labels)
    data = {}
    for (name, (sch, note)), lab in zip(built.items(), labels):
        data[name] = {"label": lab, "basic": P.schema_text(sch), "rich": rich_text(sch, note)}
        print(f"{name:10s} -> Schema {lab}")
    P.save_json("llm_validation_descriptions.json", data)
    with open("expert_descriptions.txt", "w", encoding="utf-8") as f:
        f.write("EXPERT EVALUATION – schemas derived from TPC-H (names hidden)\n"
                "Score each schema 1-10 for each aspect (see expert_rubric.txt).\n\n")
        for name, d in sorted(data.items(), key=lambda x: x[1]["label"]):
            f.write(f"=================== SCHEMA {d['label']} ===================\n{d['rich']}\n\n")
    with open("expert_scores.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["schema"] + ASPECTS + ["comment"])
        for lab in sorted(d["label"] for d in data.values()): w.writerow([lab] + [""] * 7)
    with open("expert_rubric.txt", "w", encoding="utf-8") as f:
        f.write(RUBRIC)
    print("\nSaved expert_descriptions.txt, expert_rubric.txt, expert_scores.csv, llm_validation_descriptions.json")

RUBRIC = """EXPERT RUBRIC (the same five aspects as in the LLM prompt)
Study condition: all source information must be preserved, so keeping source columns or tables
is NOT a weakness. Evaluate only semantic quality, not performance or storage size.

Scale: 1-2 wrong or misleading design, 3-4 major weaknesses, 5-6 usable with clear weaknesses,
       7-8 good with minor weaknesses, 9-10 textbook dimensional design.

1. fact_dimension   Are facts (measurable events) and dimensions (descriptive context) identified
                    correctly? Are measures in fact tables and descriptive attributes in dimensions?
2. grain            Is the grain of every fact table clear, and are measures of different grains kept
                    in different tables? (Descriptive attributes of a coarser level in a fact table,
                    e.g. order date in an order-line fact, do NOT mix grains; repeated MEASURES do.)
3. hierarchies_time Are meaningful hierarchies (customer -> nation -> region) and a usable time
                    dimension available?
4. naming           Are table and column names clear, consistent and unambiguous?
5. redundancy       Is the schema free of duplicated or ambiguous attributes and tables
                    (beyond what the study condition requires)?
overall             Your overall judgement (not necessarily the average).

Write your scores into expert_scores.csv (one row per schema, numbers 1-10) and a short comment.
Do not look at the LLM results before you have finished.
"""

def cmd_llm(args):
    P.load_dotenv()
    data = P.load_json("llm_validation_descriptions.json", {})
    res = P.load_json("llm_validation_results.json", {})
    for name, d in data.items():
        for variant in ("basic", "rich"):
            key = f"{name}|{variant}"; res.setdefault(key, [])
            while len(res[key]) < args.repeats:
                res[key].append(P.ask_llm(d[variant])); P.save_json("llm_validation_results.json", res)
            print(f"{name:10s} {variant:5s} overall={[r['overall'] for r in res[key]]} "
                  f"grain={[r.get('grain') for r in res[key]]}")
    print("Saved llm_validation_results.json")

GRAIN_CLAIM = re.compile(r"(mix|conflat|blend|confus|two grains|different grains|grain ambigu)", re.I)

def ranks(v):
    s = sorted(range(len(v)), key=lambda i: v[i]); r = [0.0] * len(v); i = 0
    while i < len(v):
        j = i
        while j + 1 < len(v) and v[s[j + 1]] == v[s[i]]: j += 1
        for k in range(i, j + 1): r[s[k]] = (i + j) / 2 + 1
        i = j + 1
    return r

def spearman(a, b):
    ra, rb = ranks(a), ranks(b)
    ma, mb = statistics.mean(ra), statistics.mean(rb)
    num = sum((x - ma) * (y - mb) for x, y in zip(ra, rb))
    den = (sum((x - ma) ** 2 for x in ra) * sum((y - mb) ** 2 for y in rb)) ** 0.5
    return round(num / den, 2) if den else float("nan")

def read_expert(path):
    if not os.path.exists(path): return None
    out = {}
    for r in csv.DictReader(open(path, encoding="utf-8-sig")):
        if r.get(ASPECTS[0], "").strip():
            out[r["schema"].strip()] = {a: float(r[a]) for a in ASPECTS}
    return out

def cmd_compare(args):
    data = P.load_json("llm_validation_descriptions.json", {})
    res = P.load_json("llm_validation_results.json", {})
    experts = [e for e in (read_expert("expert_scores.csv"), read_expert("expert_scores_2.csv")) if e]
    if not experts:
        print("expert_scores.csv is not filled in yet"); return
    gchk = {r["schema"]: r["repeated_measures"] for r in csv.DictReader(open("results_grain_check2.csv", encoding="utf-8"))
            } if os.path.exists("results_grain_check2.csv") else {}
    rows = []
    for name, d in data.items():
        lab = d["label"]
        exp = {a: statistics.mean(e[lab][a] for e in experts) for a in ASPECTS}
        row = {"schema": name, "label": lab}
        for a in ASPECTS: row[f"expert_{a}"] = round(exp[a], 2)
        for v in ("basic", "rich"):
            rr = res[f"{name}|{v}"]
            for a in ASPECTS: row[f"{v}_{a}"] = round(statistics.mean(r[a] for r in rr), 2)
            row[f"{v}_grain_mix_claims"] = sum(bool(GRAIN_CLAIM.search(r.get("justification", ""))) for r in rr)
        row["grain_check_ok"] = name not in ("heuristic",)
        cls = []
        for a in ASPECTS:
            if abs(row[f"basic_{a}"] - exp[a]) >= 2:
                cls.append(f"{a}: {'input' if abs(row[f'rich_{a}'] - exp[a]) <= 1 else 'model'}")
        row["disagreements_basic (cause)"] = "; ".join(cls)
        rows.append(row)
    P.write_csv("results_llm_validation.csv", rows)
    print(f"Experts: {len(experts)}   Schemas: {len(rows)}\n")
    print(f"{'schema':10s} {'exp':>4s} {'basic':>6s} {'rich':>6s}   grain: {'exp':>4s} {'basic':>6s} {'rich':>6s}  mix-claims b/r")
    for r in rows:
        print(f"{r['schema']:10s} {r['expert_overall']:4.1f} {r['basic_overall']:6.2f} {r['rich_overall']:6.2f}   "
              f"       {r['expert_grain']:4.1f} {r['basic_grain']:6.2f} {r['rich_grain']:6.2f}  "
              f"{r['basic_grain_mix_claims']}/{r['rich_grain_mix_claims']}   {r['disagreements_basic (cause)']}")
    summ = []
    for a in ASPECTS:
        e = [r[f"expert_{a}"] for r in rows]
        s = {"aspect": a}
        for v in ("basic", "rich"):
            l = [r[f"{v}_{a}"] for r in rows]
            s[f"MAE_{v}"] = round(statistics.mean(abs(x - y) for x, y in zip(e, l)), 2)
            s[f"spearman_{v}"] = spearman(e, l)
        summ.append(s)
    if len(experts) == 2:
        for s, a in zip(summ, ASPECTS):
            labs = list(experts[0])
            x = [experts[0][l][a] for l in labs]; y = [experts[1][l][a] for l in labs]
            s["MAE_expert1_vs_expert2"] = round(statistics.mean(abs(p - q) for p, q in zip(x, y)), 2)
            s["spearman_experts"] = spearman(x, y)
    print("\nAgreement with the expert (MAE = mean absolute difference in points, Spearman = rank correlation):")
    for s in summ: print("  " + "  ".join(f"{k}={v}" for k, v in s.items()))
    P.write_csv("results_llm_validation_summary.csv", summ)
    print("\nSaved results_llm_validation.csv and results_llm_validation_summary.csv")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["prepare", "llm", "compare"])
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    {"prepare": cmd_prepare, "llm": cmd_llm, "compare": cmd_compare}[a.command](a)
