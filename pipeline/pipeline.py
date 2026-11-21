#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB implementation of rules.md.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import json
import os

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

MS_DAY = 86_400_000
MS_WEEK = 7 * MS_DAY
GAP_MS = 30 * 60 * 1000
# Delimiter that never occurs in the data. We read each line as one column
# and split on ',' ourselves, so DuckDB's own CSV rules never apply.
SEP = chr(31)
FIELD_OK = r'"[^"]*"|[^"]*'  # a whole quoted field, or a bare field with no quote

con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")
con.execute("SET TimeZone = 'UTC'")

# ---------------------------------------------------------------- timestamps
# F1 (Z), F2 (+HH:MM / -HH:MM) and F3 (digits = epoch ms). Returns NULL if bad.
# Every digit extraction uses TRY_CAST, so a non-digit can never raise an error.
con.execute(r"""
CREATE OR REPLACE MACRO ts_ms(s) AS (
  CASE
    WHEN regexp_full_match(s, '[0-9]+')
      THEN TRY_CAST(coalesce(nullif(ltrim(s, '0'), ''), '0') AS BIGINT)
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})')
      THEN CASE
        WHEN TRY_CAST(substr(s, 6, 2) AS INTEGER) BETWEEN 1 AND 12
         AND TRY_CAST(substr(s, 12, 2) AS INTEGER) BETWEEN 0 AND 23
         AND TRY_CAST(substr(s, 15, 2) AS INTEGER) BETWEEN 0 AND 59
         AND TRY_CAST(substr(s, 18, 2) AS INTEGER) BETWEEN 0 AND 59
         AND (substr(s, 20, 1) = 'Z'
              OR (TRY_CAST(substr(s, 21, 2) AS INTEGER) BETWEEN 0 AND 23
                  AND TRY_CAST(substr(s, 24, 2) AS INTEGER) BETWEEN 0 AND 59))
        THEN epoch_ms(TRY_CAST(substr(s, 1, 19) AS TIMESTAMP))
             - CASE WHEN substr(s, 20, 1) = 'Z' THEN 0
                    ELSE (CASE WHEN substr(s, 20, 1) = '+' THEN 1 ELSE -1 END)
                         * (TRY_CAST(substr(s, 21, 2) AS BIGINT) * 3600
                            + TRY_CAST(substr(s, 24, 2) AS BIGINT) * 60) * 1000
               END
      END
  END
);
""")

# ---------------------------------------------------------------- parsing
def raw_lines(path, skip):
    """One row per physical line: (line, n), where n is the 1-based line number in its file.
    The CSV reader is never used, so no CSV rules apply here."""
    return f"""(
        SELECT r.line, r.n FROM (
            SELECT unnest(list_transform(
                       range(1, len(arr) + 1),
                       i -> struct_pack(line := arr[i], n := i))) AS r
            FROM (SELECT string_split(content, chr(10)) AS arr
                  FROM read_text('{path}'))
        ) WHERE r.n > {skip}
    )"""


def parsed(path, nfields, skip=1):
    return f"""
        SELECT list_transform(p, x -> NULLIF(
                   CASE WHEN x LIKE '"%"' THEN substr(x, 2, length(x) - 2) ELSE x END, ''
               )) AS f
        FROM (SELECT string_split(line, ',') AS p FROM {raw_lines(path, skip)})
        WHERE len(p) = {nfields}
          AND len(list_filter(p, x -> NOT regexp_full_match(x, '{FIELD_OK}'))) = 0
    """


with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
WS = con.execute("SELECT ts_ms(?)", [manifest["window_start"]]).fetchone()[0]
WE = con.execute("SELECT ts_ms(?)", [manifest["window_end"]]).fetchone()[0]
print("window (ms):", WS, WE)

# ---------------------------------------------------------------- small tables
con.execute(f"""
CREATE TABLE products AS
SELECT f[1] AS product_id, f[2] AS category
FROM ({parsed(os.path.join(DATA, 'products.csv'), 2)})
WHERE f[1] IS NOT NULL AND f[2] IS NOT NULL
""")

con.execute(f"""
CREATE TABLE campaigns AS
SELECT f[1] AS campaign_id, f[2] AS channel
FROM ({parsed(os.path.join(DATA, 'campaigns.csv'), 2)})
WHERE f[1] IS NOT NULL AND f[2] IS NOT NULL
""")

# ---------------------------------------------------------------- identities
con.execute(f"""
CREATE TABLE lk AS
SELECT f[1] AS a, f[2] AS b
FROM ({parsed(os.path.join(DATA, 'links', '*.csv'), 2)})
WHERE f[1] IS NOT NULL AND f[2] IS NOT NULL
""")

con.execute("""
CREATE TABLE edges AS
SELECT a AS u, b AS v FROM lk
UNION ALL
SELECT b AS u, a AS v FROM lk
""")
con.execute("CREATE TABLE lab AS SELECT DISTINCT u AS id, u AS lab FROM edges")

# Connected components by min-label propagation with pointer jumping.
# Labels only decrease, and a fixed point assigns every component its byte-wise
# minimum ID. VARCHAR comparison is byte-wise in DuckDB's default collation.
while True:
    con.execute("""
        CREATE OR REPLACE TABLE lab_next AS
        WITH nb AS (
            SELECT e.u AS id, min(l.lab) AS nmin
            FROM edges e JOIN lab l ON l.id = e.v
            GROUP BY e.u
        ),
        m AS (
            SELECT l.id, least(l.lab, coalesce(nb.nmin, l.lab)) AS m
            FROM lab l LEFT JOIN nb ON nb.id = l.id
        )
        SELECT m.id, least(m.m, coalesce(l2.lab, m.m)) AS lab
        FROM m LEFT JOIN lab l2 ON l2.id = m.m
    """)
    changed = con.execute(
        "SELECT count(*) FROM lab_next n JOIN lab o USING (id) WHERE n.lab <> o.lab"
    ).fetchone()[0]
    con.execute("DROP TABLE lab")
    con.execute("ALTER TABLE lab_next RENAME TO lab")
    if changed == 0:
        break

# ---------------------------------------------------------------- events
con.execute(f"""
CREATE TABLE ev_raw AS
SELECT f[1] AS event_id, f[2] AS user_id,
       ts_ms(f[3]) AS ts, ts_ms(f[4]) AS ingest_ts,
       f[5] AS event_type, f[6] AS product_id, f[7] AS price_s
FROM ({parsed(os.path.join(DATA, 'events', '*.csv'), 7)})
WHERE f[1] IS NOT NULL
""")

# Dedup first (smallest ingest_ts), then validate the kept copy only.
con.execute(f"""
CREATE TABLE ev AS
WITH kept AS (
    SELECT * FROM ev_raw
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1
),
priced AS (
    SELECT *,
           CASE WHEN regexp_full_match(price_s, '[0-9]+')
                THEN TRY_CAST(coalesce(nullif(ltrim(price_s, '0'), ''), '0') AS BIGINT)
           END AS price_v
    FROM kept
)
SELECT event_id, user_id, ts, event_type, product_id,
       CASE WHEN event_type = 'purchase' THEN price_v END AS price
FROM priced
WHERE user_id IS NOT NULL
  AND ts IS NOT NULL AND ts >= {WS} AND ts < {WE}
  AND event_type IN ('view', 'cart', 'purchase')
  AND product_id IS NOT NULL
  AND (event_type <> 'purchase' OR (price_v BETWEEN 1 AND 10000000))
  AND ingest_ts - ts <= {MS_DAY}          -- late drop: over 24 h is dropped
""")

# Map to person name. Unlinked IDs keep their own name.
con.execute("""
CREATE TABLE evp AS
SELECT coalesce(l.lab, e.user_id) AS pid, e.ts, e.event_type, e.product_id, e.price
FROM ev e LEFT JOIN lab l ON l.id = e.user_id
""")

# ---------------------------------------------------------------- sessions
# Sessionize over DISTINCT timestamps per person, so equal-ts events always
# share a session regardless of read order.
con.execute(f"""
CREATE TABLE dts AS
WITH d AS (SELECT DISTINCT pid, ts FROM evp),
g AS (
    SELECT pid, ts,
           CASE WHEN lag(ts) OVER (PARTITION BY pid ORDER BY ts) IS NULL
                  OR ts - lag(ts) OVER (PARTITION BY pid ORDER BY ts) > {GAP_MS}
                THEN 1 ELSE 0 END AS newf
    FROM d
)
SELECT pid, ts,
       CAST(sum(newf) OVER (PARTITION BY pid ORDER BY ts ROWS UNBOUNDED PRECEDING) AS BIGINT) AS seq
FROM g
""")

con.execute("""
CREATE TABLE sess AS
SELECT pid, seq, min(ts) AS start_ts
FROM dts GROUP BY pid, seq
""")

# Each event with its session and category ('unknown' if the product is not in products.csv).
con.execute("""
CREATE TABLE evs AS
SELECT e.pid, d.seq, e.event_type, e.price,
       coalesce(p.category, 'unknown') AS category
FROM evp e
JOIN dts d ON d.pid = e.pid AND d.ts = e.ts
LEFT JOIN products p ON p.product_id = e.product_id
""")

# Session aggregates. A session's day is the UTC date of its start.
con.execute("""
CREATE TABLE sess_agg AS
SELECT s.pid, s.seq, s.start_ts,
       CAST(make_timestamp(s.start_ts * 1000) AS DATE) AS day,
       count(*) AS n_events,
       CAST(coalesce(sum(e.price), 0) AS BIGINT) AS revenue,
       count(e.price) AS n_purch
FROM sess s
JOIN evs e ON e.pid = s.pid AND e.seq = s.seq
GROUP BY s.pid, s.seq, s.start_ts
""")

# ---------------------------------------------------------------- user_daily
con.execute("""
CREATE TABLE cat_day AS
SELECT s.pid, s.day, e.category, sum(e.price) AS rev
FROM sess_agg s
JOIN evs e ON e.pid = s.pid AND e.seq = s.seq
WHERE e.price IS NOT NULL
GROUP BY s.pid, s.day, e.category
""")

con.execute("""
CREATE TABLE top_cat AS
SELECT pid, day, category AS top_category
FROM cat_day
QUALIFY row_number() OVER (PARTITION BY pid, day ORDER BY rev DESC, category ASC) = 1
""")

# ---------------------------------------------------------------- clicks
con.execute(f"""
CREATE TABLE cl_raw AS
SELECT f[1] AS click_id, f[2] AS user_id, f[3] AS campaign_id, ts_ms(f[4]) AS ts
FROM ({parsed(os.path.join(DATA, 'clicks', '*.csv'), 4)})
WHERE f[1] IS NOT NULL
""")

# Copies of a click are identical, so keep any one. Then validate.
con.execute(f"""
CREATE TABLE cl AS
WITH d AS (
    SELECT * FROM cl_raw
    QUALIFY row_number() OVER (PARTITION BY click_id ORDER BY ts) = 1
)
SELECT coalesce(l.lab, d.user_id) AS pid, d.click_id, d.ts, d.campaign_id
FROM d LEFT JOIN lab l ON l.id = d.user_id
WHERE d.user_id IS NOT NULL
  AND d.ts >= {WS - MS_WEEK} AND d.ts < {WE}
  AND d.campaign_id IN (SELECT campaign_id FROM campaigns)
""")

# One row per (person, ts). On a tie in ts, the smallest click_id wins.
con.execute("""
CREATE TABLE cl_pt AS
SELECT pid, ts, min(click_id) AS click_id, arg_min(campaign_id, click_id) AS campaign_id
FROM cl GROUP BY pid, ts
""")

# Credit: the latest click at or before session start (ASOF). If that click is
# more than 7 days old, nothing qualifies and the session goes to 'organic'.
con.execute(f"""
CREATE TABLE credit AS
SELECT s.pid, s.seq, s.day, s.revenue, s.n_purch,
       CASE WHEN c.ts IS NOT NULL AND s.start_ts - c.ts <= {MS_WEEK}
            THEN c.campaign_id ELSE 'organic' END AS campaign_id
FROM sess_agg s
ASOF LEFT JOIN cl_pt c ON s.pid = c.pid AND s.start_ts >= c.ts
""")

# Assists: a campaign assists a session if one of the person's clicks has
# click.ts in [start - 7d, start], i.e. start in [click.ts, click.ts + 7d].
# Per (person, campaign), merge those intervals into islands, then join once.
con.execute(f"""
CREATE TABLE iv_base AS
SELECT pid, campaign_id AS camp, ts AS a, ts + {MS_WEEK} AS b, click_id,
       max(ts + {MS_WEEK}) OVER (
           PARTITION BY pid, campaign_id ORDER BY ts, click_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_b
FROM cl
""")

con.execute("""
CREATE TABLE iv_id AS
SELECT pid, camp, a, b,
       CAST(sum(CASE WHEN prev_b IS NULL OR a > prev_b THEN 1 ELSE 0 END)
            OVER (PARTITION BY pid, camp ORDER BY a, click_id ROWS UNBOUNDED PRECEDING)
            AS BIGINT) AS isl
FROM iv_base
""")

con.execute("""
CREATE TABLE ivals AS
SELECT pid, camp, min(a) AS lo, max(b) AS hi
FROM iv_id GROUP BY pid, camp, isl
""")

con.execute("""
CREATE TABLE assist AS
SELECT DISTINCT s.pid, s.seq, s.day, iv.camp AS campaign_id
FROM sess_agg s
JOIN ivals iv ON iv.pid = s.pid AND s.start_ts BETWEEN iv.lo AND iv.hi
""")

# ---------------------------------------------------------------- campaign_daily
con.execute("""
CREATE TABLE camp_credit AS
SELECT campaign_id, day,
       count(*) AS sessions,
       count(*) FILTER (WHERE n_purch > 0) AS purchasing_sessions,
       CAST(sum(revenue) AS BIGINT) AS revenue_cents
FROM credit
GROUP BY campaign_id, day
""")

con.execute("""
CREATE TABLE camp_assist AS
SELECT campaign_id, day, count(*) AS assisted_sessions
FROM assist
GROUP BY campaign_id, day
""")

# ---------------------------------------------------------------- write outputs
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)

con.execute(f"""
COPY (
    SELECT s.pid AS user_id,
           s.day AS day,
           count(*)::BIGINT AS sessions,
           CAST(sum(s.n_events) AS BIGINT) AS events,
           CAST(sum(s.revenue) AS BIGINT) AS revenue_cents,
           any_value(t.top_category) AS top_category
    FROM sess_agg s
    LEFT JOIN top_cat t ON t.pid = s.pid AND t.day = s.day
    GROUP BY s.pid, s.day
) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")

con.execute(f"""
COPY (
    WITH merged AS (
        SELECT coalesce(c.campaign_id, a.campaign_id) AS campaign_id,
               coalesce(c.day, a.day) AS day,
               coalesce(c.sessions, 0) AS sessions,
               coalesce(c.purchasing_sessions, 0) AS purchasing_sessions,
               coalesce(c.revenue_cents, 0) AS revenue_cents,
               coalesce(a.assisted_sessions, 0) AS assisted_sessions
        FROM camp_credit c
        FULL OUTER JOIN camp_assist a
          ON c.campaign_id = a.campaign_id AND c.day = a.day
    )
    SELECT m.campaign_id,
           CASE WHEN m.campaign_id = 'organic' THEN 'organic' ELSE k.channel END AS channel,
           m.day,
           CAST(m.sessions AS BIGINT) AS sessions,
           CAST(m.purchasing_sessions AS BIGINT) AS purchasing_sessions,
           CAST(m.revenue_cents AS BIGINT) AS revenue_cents,
           CAST(m.assisted_sessions AS BIGINT) AS assisted_sessions
    FROM merged m
    LEFT JOIN campaigns k ON k.campaign_id = m.campaign_id
) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")

# ---------------------------------------------------------------- self-check
ud = con.execute(f"SELECT sum(sessions), sum(revenue_cents) FROM read_parquet('{OUT}/user_daily/*.parquet')").fetchone()
cd = con.execute(f"SELECT sum(sessions), sum(revenue_cents), sum(assisted_sessions) FROM read_parquet('{OUT}/campaign_daily/*.parquet')").fetchone()
print("user_daily sessions/revenue:", ud)
print("campaign_daily sessions/revenue/assisted:", cd)
if ud[0] != cd[0] or ud[1] != cd[1]:
    print("WARNING: sessions or revenue do not match across tables")
print("done")