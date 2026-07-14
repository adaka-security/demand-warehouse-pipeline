"""
DAG 1: generate synthetic vehicle demand data into SQL Server.

This stands in for the "legacy" OLTP source system. The data is skewed on
purpose (weekend dip, quarter end spike, uneven region mix) so that the
benchmark queries later actually mean something.

Row count is controlled by the DEMAND_ROW_COUNT env var. Start with 200000
to smoke test, then rerun with 3000000 for real benchmarks.
"""

import os
from datetime import datetime, timedelta

import numpy as np
import pymssql
from airflow.decorators import dag, task

MSSQL = dict(
    server=os.environ["MSSQL_HOST"],
    port=int(os.environ.get("MSSQL_PORT", 1433)),
    user=os.environ["MSSQL_USER"],
    password=os.environ["MSSQL_PASSWORD"],
)

DB_NAME = os.environ.get("MSSQL_DB", "demand")
ROW_COUNT = int(os.environ.get("DEMAND_ROW_COUNT", 3_000_000))
BATCH = 10_000

REGIONS = ["NA-West", "NA-East", "NA-Central", "EU-Central", "EU-North", "APAC-East"]
REGION_WEIGHTS = [0.28, 0.22, 0.10, 0.18, 0.08, 0.14]
MODELS = ["M3", "MY", "MS", "MX", "CT"]
MODEL_WEIGHTS = [0.34, 0.38, 0.09, 0.08, 0.11]
TRIMS = ["Standard", "Long Range", "Performance"]
TRIM_WEIGHTS = [0.45, 0.38, 0.17]
BASE_PRICE = {"M3": 39990, "MY": 44990, "MS": 79990, "MX": 84990, "CT": 61990}
TRIM_MULT = {"Standard": 1.0, "Long Range": 1.18, "Performance": 1.35}
# 1=placed 2=confirmed 3=in_production 4=delivered 5=cancelled
STATUS_WEIGHTS = [0.10, 0.15, 0.20, 0.48, 0.07]


@dag(
    dag_id="generate_demand_data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demand", "source"],
)
def generate_demand_data():

    @task
    def create_database_and_schema():
        # autocommit connection without a db first, so CREATE DATABASE works
        conn = pymssql.connect(**MSSQL, autocommit=True)
        cur = conn.cursor()
        cur.execute(
            f"IF DB_ID('{DB_NAME}') IS NULL CREATE DATABASE [{DB_NAME}]"
        )
        conn.close()

        conn = pymssql.connect(**MSSQL, database=DB_NAME, autocommit=True)
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.orders', 'U') IS NOT NULL DROP TABLE dbo.orders;
            CREATE TABLE dbo.orders (
                order_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                order_date DATE NOT NULL,
                region VARCHAR(20) NOT NULL,
                model VARCHAR(10) NOT NULL,
                trim_level VARCHAR(20) NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL,
                quantity SMALLINT NOT NULL,
                status TINYINT NOT NULL,
                delivery_estimate_days SMALLINT NOT NULL
            );
            """
        )
        conn.close()

    @task
    def load_rows():
        rng = np.random.default_rng(42)

        # Build a date distribution over the last 3 years with weekly and
        # quarterly seasonality baked in.
        start = datetime(2023, 7, 1)
        days = 1080
        all_dates = [start + timedelta(days=int(i)) for i in range(days)]
        weights = np.ones(days)
        for i, d in enumerate(all_dates):
            if d.weekday() >= 5:  # weekend dip
                weights[i] *= 0.55
            if d.month in (3, 6, 9, 12) and d.day >= 20:  # quarter end push
                weights[i] *= 1.9
        weights = weights / weights.sum()

        conn = pymssql.connect(**MSSQL, database=DB_NAME)
        cur = conn.cursor()

        inserted = 0
        while inserted < ROW_COUNT:
            n = min(BATCH, ROW_COUNT - inserted)
            date_idx = rng.choice(days, size=n, p=weights)
            regions = rng.choice(REGIONS, size=n, p=REGION_WEIGHTS)
            models = rng.choice(MODELS, size=n, p=MODEL_WEIGHTS)
            trims = rng.choice(TRIMS, size=n, p=TRIM_WEIGHTS)
            statuses = rng.choice([1, 2, 3, 4, 5], size=n, p=STATUS_WEIGHTS)
            qty = rng.choice([1, 1, 1, 1, 2, 3], size=n)
            noise = rng.normal(1.0, 0.03, size=n)
            est_days = rng.integers(7, 120, size=n)

            rows = []
            for i in range(n):
                price = round(
                    BASE_PRICE[models[i]] * TRIM_MULT[trims[i]] * float(noise[i]), 2
                )
                rows.append(
                    (
                        all_dates[int(date_idx[i])].date(),
                        str(regions[i]),
                        str(models[i]),
                        str(trims[i]),
                        price,
                        int(qty[i]),
                        int(statuses[i]),
                        int(est_days[i]),
                    )
                )

            cur.executemany(
                """
                INSERT INTO dbo.orders
                (order_date, region, model, trim_level, unit_price,
                 quantity, status, delivery_estimate_days)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
            conn.commit()
            inserted += n
            if inserted % 100_000 == 0:
                print(f"inserted {inserted:,} / {ROW_COUNT:,}")

        cur.execute("SELECT COUNT(*) FROM dbo.orders")
        print("final row count:", cur.fetchone()[0])
        conn.close()

    create_database_and_schema() >> load_rows()


generate_demand_data()
