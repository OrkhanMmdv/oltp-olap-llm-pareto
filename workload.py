"""
Workload definition: 13 analytical queries adapted from the Star Schema
Benchmark (O'Neil et al., 2009), written once at the LOGICAL level and
translated automatically into SQL for any physical schema.
"""
from collections import deque
import re

# ------------------------------------------------------------------
# 1. Physical schema descriptions
#    nodes : alias -> physical table name
#    edges : (alias1, alias2, join condition)
#    attrs : logical attribute -> (alias, SQL expression)
#    root  : alias of the central (fact) table
# ------------------------------------------------------------------
ORIGINAL = {
    "name": "original",
    "db": "tpch_sf1.duckdb",
    "root": "l",
    "nodes": {"l": "lineitem", "o": "orders", "c": "customer", "cn": "nation",
              "cr": "region", "s": "supplier", "sn": "nation", "sr": "region",
              "p": "part", "ps": "partsupp"},
    "edges": [
        ("l", "o",  "l.l_orderkey = o.o_orderkey"),
        ("o", "c",  "o.o_custkey = c.c_custkey"),
        ("c", "cn", "c.c_nationkey = cn.n_nationkey"),
        ("cn", "cr", "cn.n_regionkey = cr.r_regionkey"),
        ("l", "s",  "l.l_suppkey = s.s_suppkey"),
        ("s", "sn", "s.s_nationkey = sn.n_nationkey"),
        ("sn", "sr", "sn.n_regionkey = sr.r_regionkey"),
        ("l", "p",  "l.l_partkey = p.p_partkey"),
        ("l", "ps", "l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey"),
    ],
    "attrs": {
        "extendedprice": ("l", "l.l_extendedprice"),
        "discount":      ("l", "l.l_discount"),
        "quantity":      ("l", "l.l_quantity"),
        "revenue":       ("l", "l.l_extendedprice * (1 - l.l_discount)"),
        "supplycost":    ("ps", "ps.ps_supplycost * l.l_quantity"),
        "year":          ("o", "year(o.o_orderdate)"),
        "yearmonthnum":  ("o", "year(o.o_orderdate) * 100 + month(o.o_orderdate)"),
        "weeknuminyear": ("o", "weekofyear(o.o_orderdate)"),
        "c_nation":      ("cn", "cn.n_name"),
        "c_region":      ("cr", "cr.r_name"),
        "c_mktsegment":  ("c", "c.c_mktsegment"),
        "s_nation":      ("sn", "sn.n_name"),
        "s_region":      ("sr", "sr.r_name"),
        "p_mfgr":        ("p", "p.p_mfgr"),
        "p_brand":       ("p", "p.p_brand"),
        "p_type":        ("p", "p.p_type"),
    },
}

STAR = {
    "name": "star",
    "db": "star_sf1.duckdb",
    "root": "f",
    "nodes": {"f": "fact_lineorder", "d": "dim_date", "c": "dim_customer",
              "s": "dim_supplier", "p": "dim_part"},
    "edges": [
        ("f", "d", "f.lo_orderdatekey = d.d_datekey"),
        ("f", "c", "f.lo_custkey = c.c_custkey"),
        ("f", "s", "f.lo_suppkey = s.s_suppkey"),
        ("f", "p", "f.lo_partkey = p.p_partkey"),
    ],
    "attrs": {
        "extendedprice": ("f", "f.lo_extendedprice"),
        "discount":      ("f", "f.lo_discount"),
        "quantity":      ("f", "f.lo_quantity"),
        "revenue":       ("f", "f.lo_revenue"),
        "supplycost":    ("f", "f.lo_supplycost"),
        "year":          ("d", "d.d_year"),
        "yearmonthnum":  ("d", "d.d_yearmonthnum"),
        "weeknuminyear": ("d", "d.d_weeknuminyear"),
        "c_nation":      ("c", "c.c_nation"),
        "c_region":      ("c", "c.c_region"),
        "c_mktsegment":  ("c", "c.c_mktsegment"),
        "s_nation":      ("s", "s.s_nation"),
        "s_region":      ("s", "s.s_region"),
        "p_mfgr":        ("p", "p.p_mfgr"),
        "p_brand":       ("p", "p.p_brand"),
        "p_type":        ("p", "p.p_type"),
    },
}

# ------------------------------------------------------------------
# 2. Logical queries ({attr} = logical attribute)
# ------------------------------------------------------------------
Y92_97 = "{year} BETWEEN 1992 AND 1997"
QUERIES = {
    "Q1.1": {"measures": ["SUM({extendedprice} * {discount}) AS revenue"],
             "where": ["{year} = 1993", "{discount} BETWEEN 0.01 AND 0.03",
                       "{quantity} < 25"], "group": []},
    "Q1.2": {"measures": ["SUM({extendedprice} * {discount}) AS revenue"],
             "where": ["{yearmonthnum} = 199401", "{discount} BETWEEN 0.04 AND 0.06",
                       "{quantity} BETWEEN 26 AND 35"], "group": []},
    "Q1.3": {"measures": ["SUM({extendedprice} * {discount}) AS revenue"],
             "where": ["{weeknuminyear} = 6", "{year} = 1994",
                       "{discount} BETWEEN 0.05 AND 0.07",
                       "{quantity} BETWEEN 26 AND 35"], "group": []},
    "Q2.1": {"measures": ["SUM({revenue}) AS revenue"],
             "where": ["{p_brand} = 'Brand#12'", "{s_region} = 'AMERICA'"],
             "group": ["year", "p_type"]},
    "Q2.2": {"measures": ["SUM({revenue}) AS revenue"],
             "where": ["{p_brand} BETWEEN 'Brand#22' AND 'Brand#25'",
                       "{s_region} = 'ASIA'"],
             "group": ["year", "p_brand"]},
    "Q2.3": {"measures": ["SUM({revenue}) AS revenue"],
             "where": ["{p_type} = 'SMALL PLATED COPPER'", "{s_region} = 'EUROPE'"],
             "group": ["year", "p_brand"]},
    "Q3.1": {"measures": ["SUM({revenue}) AS revenue"],
             "where": ["{c_region} = 'ASIA'", "{s_region} = 'ASIA'", Y92_97],
             "group": ["c_nation", "s_nation", "year"]},
    "Q3.2": {"measures": ["SUM({revenue}) AS revenue"],
             "where": ["{c_nation} = 'UNITED STATES'",
                       "{s_nation} = 'UNITED STATES'", Y92_97],
             "group": ["c_mktsegment", "year"]},
    "Q3.3": {"measures": ["SUM({revenue}) AS revenue"],
             "where": ["{c_nation} IN ('UNITED KINGDOM', 'FRANCE')",
                       "{s_nation} IN ('UNITED KINGDOM', 'FRANCE')", Y92_97],
             "group": ["c_nation", "s_nation", "year"]},
    "Q3.4": {"measures": ["SUM({revenue}) AS revenue"],
             "where": ["{c_nation} IN ('UNITED KINGDOM', 'FRANCE')",
                       "{s_nation} IN ('UNITED KINGDOM', 'FRANCE')",
                       "{yearmonthnum} = 199712"],
             "group": ["c_nation", "s_nation", "year"]},
    "Q4.1": {"measures": ["SUM({revenue} - {supplycost}) AS profit"],
             "where": ["{c_region} = 'AMERICA'", "{s_region} = 'AMERICA'",
                       "{p_mfgr} IN ('Manufacturer#1', 'Manufacturer#2')"],
             "group": ["year", "c_nation"]},
    "Q4.2": {"measures": ["SUM({revenue} - {supplycost}) AS profit"],
             "where": ["{c_region} = 'AMERICA'", "{s_region} = 'AMERICA'",
                       "{year} IN (1997, 1998)",
                       "{p_mfgr} IN ('Manufacturer#1', 'Manufacturer#2')"],
             "group": ["year", "s_nation", "p_brand"]},
    "Q4.3": {"measures": ["SUM({revenue} - {supplycost}) AS profit"],
             "where": ["{c_region} = 'AMERICA'", "{s_nation} = 'UNITED STATES'",
                       "{year} IN (1997, 1998)", "{p_brand} = 'Brand#14'"],
             "group": ["year", "s_nation", "p_type"]},
}

# ------------------------------------------------------------------
# 3. Translator: logical query -> SQL for a given schema
# ------------------------------------------------------------------
def _attrs_used(q):
    text = " ".join(q["measures"] + q["where"]) + " " + " ".join(
        "{" + g + "}" for g in q["group"])
    return set(re.findall(r"\{(\w+)\}", text))


def to_sql(schema, qname):
    """Return (sql, number_of_joins) for query qname on the given schema."""
    q = QUERIES[qname]
    attrs = schema["attrs"]
    needed = {attrs[a][0] for a in _attrs_used(q)}

    # Adjacency list and BFS tree from the root (fact) table
    adj = {n: [] for n in schema["nodes"]}
    for a, b, cond in schema["edges"]:
        adj[a].append((b, cond))
        adj[b].append((a, cond))
    parent = {schema["root"]: None}
    dq = deque([schema["root"]])
    while dq:
        n = dq.popleft()
        for m, cond in adj[n]:
            if m not in parent:
                parent[m] = (n, cond)
                dq.append(m)

    # All tables on the paths root -> needed table
    include = set()
    for n in needed:
        while n is not None and n not in include:
            include.add(n)
            n = parent[n][0] if parent[n] else None
    include.add(schema["root"])

    # Join each table after its parent
    order, dq = [], deque([schema["root"]])
    seen = {schema["root"]}
    while dq:
        n = dq.popleft()
        for m, cond in adj[n]:
            if m in include and m not in seen and parent[m][0] == n:
                seen.add(m)
                order.append((m, cond))
                dq.append(m)

    sub = lambda s: s.format(**{k: v[1] for k, v in attrs.items()})
    group_cols = [f"{attrs[g][1]} AS {g}" for g in q["group"]]
    select = ", ".join(group_cols + [sub(m) for m in q["measures"]])
    frm = f"{schema['nodes'][schema['root']]} {schema['root']}"
    for m, cond in order:
        frm += f"\n  JOIN {schema['nodes'][m]} {m} ON {cond}"
    sql = f"SELECT {select}\nFROM {frm}\nWHERE " + "\n  AND ".join(
        sub(w) for w in q["where"])
    if q["group"]:
        g = ", ".join(attrs[x][1] for x in q["group"])
        sql += f"\nGROUP BY {g}\nORDER BY {g}"
    return sql, len(order)


if __name__ == "__main__":
    for sch in (ORIGINAL, STAR):
        sql, j = to_sql(sch, "Q4.1")
        print(f"--- {sch['name']} ({j} joins) ---\n{sql}\n")