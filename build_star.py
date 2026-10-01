import duckdb, os, time

SRC = "tpch_sf1.duckdb"
DST = "star_sf1.duckdb"

if os.path.exists(DST):
    os.remove(DST)                      # start from scratch every time

start = time.time()
con = duckdb.connect(DST)
con.execute(f"ATTACH '{SRC}' AS src (READ_ONLY)")

# ---------- Dimension: date (one row per order date) ----------
con.execute("""
CREATE TABLE dim_date AS
SELECT DISTINCT
    CAST(strftime(o_orderdate, '%Y%m%d') AS INTEGER) AS d_datekey,
    o_orderdate                                      AS d_date,
    year(o_orderdate)                                AS d_year,
    month(o_orderdate)                               AS d_month,
    year(o_orderdate) * 100 + month(o_orderdate)     AS d_yearmonthnum,
    weekofyear(o_orderdate)                          AS d_weeknuminyear,
    dayname(o_orderdate)                             AS d_dayofweek
FROM src.orders;
""")

# ---------- Dimension: customer (nation and region merged in) ----------
con.execute("""
CREATE TABLE dim_customer AS
SELECT c.c_custkey, c.c_name, c.c_address, c.c_phone, c.c_mktsegment,
       n.n_name AS c_nation, r.r_name AS c_region
FROM src.customer c
JOIN src.nation n ON c.c_nationkey = n.n_nationkey
JOIN src.region r ON n.n_regionkey = r.r_regionkey;
""")

# ---------- Dimension: supplier (nation and region merged in) ----------
con.execute("""
CREATE TABLE dim_supplier AS
SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone,
       n.n_name AS s_nation, r.r_name AS s_region
FROM src.supplier s
JOIN src.nation n ON s.s_nationkey = n.n_nationkey
JOIN src.region r ON n.n_regionkey = r.r_regionkey;
""")

# ---------- Dimension: part ----------
con.execute("""
CREATE TABLE dim_part AS
SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container
FROM src.part;
""")

# ---------- Fact: lineorder (LINEITEM + ORDERS + supply cost) ----------
con.execute("""
CREATE TABLE fact_lineorder AS
SELECT
    l.l_orderkey                                     AS lo_orderkey,
    l.l_linenumber                                   AS lo_linenumber,
    o.o_custkey                                      AS lo_custkey,
    l.l_partkey                                      AS lo_partkey,
    l.l_suppkey                                      AS lo_suppkey,
    CAST(strftime(o.o_orderdate, '%Y%m%d') AS INTEGER) AS lo_orderdatekey,
    o.o_orderpriority                                AS lo_orderpriority,
    o.o_shippriority                                 AS lo_shippriority,
    l.l_quantity                                     AS lo_quantity,
    l.l_extendedprice                                AS lo_extendedprice,
    l.l_discount                                     AS lo_discount,
    l.l_tax                                          AS lo_tax,
    l.l_extendedprice * (1 - l.l_discount)           AS lo_revenue,
    ps.ps_supplycost * l.l_quantity                  AS lo_supplycost,
    l.l_shipmode                                     AS lo_shipmode
FROM src.lineitem l
JOIN src.orders   o  ON l.l_orderkey = o.o_orderkey
JOIN src.partsupp ps ON l.l_partkey = ps.ps_partkey
                    AND l.l_suppkey = ps.ps_suppkey;
""")

con.execute("CHECKPOINT")
con.execute("DETACH src")

# ---------- Report ----------
for (name,) in con.execute(
        "SELECT table_name FROM information_schema.tables ORDER BY table_name").fetchall():
    rows = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    cols = con.execute(
        f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{name}'").fetchone()[0]
    print(f"{name:15s} {rows:>12,} rows  {cols:>3} columns")
con.close()

size_mb = os.path.getsize(DST) / 1024 / 1024
print(f"\nFile size: {size_mb:.1f} MB")
print(f"Built in {time.time() - start:.1f} seconds")