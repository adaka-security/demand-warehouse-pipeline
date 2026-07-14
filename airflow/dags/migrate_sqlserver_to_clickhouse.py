"""
DAG 2: migrate dbo.orders from SQL Server into ClickHouse.

Design decisions worth talking about in an interview:

- Extraction uses keyset pagination on order_id (WHERE order_id > last_id)
  instead of OFFSET/FETCH. OFFSET gets slower the deeper you page because
  SQL Server still has to scan and discard skipped rows. Keyset stays O(chunk).

- ClickHouse table is MergeTree, PARTITION BY month of order_date,
  ORDER BY (region, model, order_date). The ORDER BY matches how demand
  planners actually filter: by region first, then model, then a date range.
  That makes those queries hit a small sorted slice of each part instead of
  scanning everything.

- The final task validates the migration: exact row count match plus
  aggregate checksums (sum of quantity, sum of unit_price) on both sides.
  A migration without validation is just hope.
"""

import os
from datetime import datetime

import clickhouse_connect
import pymssql
from airflow.decorators import dag, task

MSSQL = dict(
    server=os.environ["MSSQL_HOST"],
    port=int(os.environ.get("MSSQL_PORT", 1433)),
    user=os.environ["MSSQL_USER"],
    password=os.environ["MSSQL_PASSWORD"],
    database=os.environ.get("MSSQL_DB", "demand"),
)

CH = dict(
    host=os.environ["CLICKHOUSE_HOST"],
    port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
    username=os.environ.get("CLICKHOUSE_USER", "default"),
    password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
)

CHUNK = 100_000

CH_COLUMNS = [
    "order_id",
    "order_date",
    "region",
    "model",
    "trim_level",
    "unit_price",
    "quantity",
    "status",
    "delivery_estimate_days",
]


@dag(
    dag_id="migrate_sqlserver_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demand", "migration"],
)
def migrate_sqlserver_to_clickhouse():

    @task
    def create_clickhouse_schema():
        client = clickhouse_connect.get_client(**CH)
        client.command("CREATE DATABASE IF NOT EXISTS demand")
        client.command("DROP TABLE IF EXISTS demand.orders")
        client.command(
            """
            CREATE TABLE demand.orders (
                order_id UInt64,
                order_date Date,
                region LowCardinality(String),
                model LowCardinality(String),
                trim_level LowCardinality(String),
                unit_price Decimal(10, 2),
                quantity UInt8,
                status UInt8,
                delivery_estimate_days UInt16
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(order_date)
            ORDER BY (region, model, order_date)
            """
        )

    @task
    def copy_data():
        src = pymssql.connect(**MSSQL)
        cur = src.cursor()
        client = clickhouse_connect.get_client(**CH)

        last_id = 0
        total = 0
        while True:
            cur.execute(
                """
                SELECT TOP %s
                    order_id, order_date, region, model, trim_level,
                    unit_price, quantity, status, delivery_estimate_days
                FROM dbo.orders
                WHERE order_id > %s
                ORDER BY order_id
                """,
                (CHUNK, last_id),
            )
            rows = cur.fetchall()
            if not rows:
                break
            client.insert("demand.orders", rows, column_names=CH_COLUMNS)
            last_id = rows[-1][0]
            total += len(rows)
            print(f"copied {total:,} rows, last order_id={last_id}")

        src.close()
        print(f"done, {total:,} rows total")

    @task
    def validate():
        src = pymssql.connect(**MSSQL)
        cur = src.cursor()
        cur.execute(
            """
            SELECT COUNT_BIG(*), SUM(CAST(quantity AS BIGINT)),
                   SUM(CAST(unit_price AS DECIMAL(18,2)))
            FROM dbo.orders
            """
        )
        src_count, src_qty, src_price = cur.fetchone()
        src.close()

        client = clickhouse_connect.get_client(**CH)
        res = client.query(
            "SELECT count(), sum(quantity), sum(unit_price) FROM demand.orders"
        ).result_rows[0]
        ch_count, ch_qty, ch_price = res

        print(f"rows       src={src_count:,}  ch={ch_count:,}")
        print(f"sum(qty)   src={src_qty:,}  ch={ch_qty:,}")
        print(f"sum(price) src={src_price}  ch={ch_price}")

        assert int(src_count) == int(ch_count), "row count mismatch"
        assert int(src_qty) == int(ch_qty), "quantity checksum mismatch"
        # Decimal vs float comparison, allow a tiny tolerance
        assert abs(float(src_price) - float(ch_price)) < 1.0, "price checksum mismatch"
        print("validation passed")

    create_clickhouse_schema() >> copy_data() >> validate()


migrate_sqlserver_to_clickhouse()
