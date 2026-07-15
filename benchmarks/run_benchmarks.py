"""
Benchmark: the same 4 demand planning queries against SQL Server and
ClickHouse, 10 runs each, reporting p50 and p95 wall clock latency.

Run from the host machine after both DAGs have completed:

    pip install pymssql clickhouse-connect
    MSSQL_SA_PASSWORD='<your password>' python benchmarks/run_benchmarks.py

Writes results to benchmarks/results.md.
"""

import os
import statistics
import time

import clickhouse_connect
import pymssql

RUNS = 10

MSSQL = dict(
    server=os.environ.get("MSSQL_HOST", "localhost"),
    port=int(os.environ.get("MSSQL_PORT", 1433)),
    user="sa",
    password=os.environ["MSSQL_SA_PASSWORD"],
    database="demand",
)

CH = dict(
    host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
    port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
    password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
)

# Each entry: (name, sqlserver_query, clickhouse_query)
QUERIES = [
    (
        "Monthly demand by region",
        """
        SELECT DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS month,
               region, SUM(quantity) AS units
        FROM dbo.orders
        WHERE status != 5
        GROUP BY DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1), region
        ORDER BY month, region
        """,
        """
        SELECT toStartOfMonth(order_date) AS month, region,
               sum(quantity) AS units
        FROM demand.orders
        WHERE status != 5
        GROUP BY month, region
        ORDER BY month, region
        """,
    ),
    (
        "Top models by revenue, last 4 quarters",
        """
        SELECT TOP 10 model, trim_level,
               SUM(unit_price * quantity) AS revenue
        FROM dbo.orders
        WHERE order_date >= DATEADD(quarter, -4, GETDATE()) AND status != 5
        GROUP BY model, trim_level
        ORDER BY revenue DESC
        """,
        """
        SELECT model, trim_level,
               sum(unit_price * quantity) AS revenue
        FROM demand.orders
        WHERE order_date >= subtractQuarters(today(), 4) AND status != 5
        GROUP BY model, trim_level
        ORDER BY revenue DESC
        LIMIT 10
        """,
    ),
    (
        "Rolling 7 day order rate, NA-West",
        """
        WITH daily AS (
            SELECT order_date, SUM(quantity) AS units
            FROM dbo.orders
            WHERE region = 'NA-West' AND status != 5
            GROUP BY order_date
        )
        SELECT order_date, units,
               AVG(CAST(units AS FLOAT)) OVER (
                   ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
               ) AS rolling_7d
        FROM daily
        ORDER BY order_date
        """,
        """
        SELECT order_date, sum(quantity) AS units,
               avg(units) OVER (
                   ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
               ) AS rolling_7d
        FROM demand.orders
        WHERE region = 'NA-West' AND status != 5
        GROUP BY order_date
        ORDER BY order_date
        """,
    ),
    (
        "Year over year units by model",
        """
        SELECT model, YEAR(order_date) AS yr, SUM(quantity) AS units
        FROM dbo.orders
        WHERE status != 5
        GROUP BY model, YEAR(order_date)
        ORDER BY model, yr
        """,
        """
        SELECT model, toYear(order_date) AS yr, sum(quantity) AS units
        FROM demand.orders
        WHERE status != 5
        GROUP BY model, yr
        ORDER BY model, yr
        """,
    ),
]


def bench(fn):
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    times.sort()
    p50 = statistics.median(times)
    p95 = times[max(0, int(round(0.95 * len(times))) - 1)]
    return p50, p95


def main():
    ms_conn = pymssql.connect(**MSSQL)
    ms_cur = ms_conn.cursor()
    ch = clickhouse_connect.get_client(**CH)

    lines = [
        "# Benchmark results",
        "",
        f"Same queries, {RUNS} runs each, wall clock seconds. "
        "Both databases running in Docker on the same machine.",
        "",
        "| Query | SQL Server p50 | SQL Server p95 | ClickHouse p50 | ClickHouse p95 | Speedup (p50) |",
        "|---|---|---|---|---|---|",
    ]

    for name, ms_q, ch_q in QUERIES:
        print(f"running: {name}")
        ms_p50, ms_p95 = bench(lambda: (ms_cur.execute(ms_q), ms_cur.fetchall()))
        ch_p50, ch_p95 = bench(lambda: ch.query(ch_q))
        speedup = ms_p50 / ch_p50 if ch_p50 > 0 else float("inf")
        lines.append(
            f"| {name} | {ms_p50:.3f}s | {ms_p95:.3f}s "
            f"| {ch_p50:.3f}s | {ch_p95:.3f}s | {speedup:.0f}x |"
        )
        print(
            f"  sqlserver p50={ms_p50:.3f}s  clickhouse p50={ch_p50:.3f}s  "
            f"speedup={speedup:.0f}x"
        )

    out = os.path.join(os.path.dirname(__file__), "results.md")
    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
