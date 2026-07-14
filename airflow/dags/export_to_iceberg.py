"""
DAG 3: export the ClickHouse orders table into an Apache Iceberg table.

Uses PyIceberg with a SQLite catalog and local Parquet files. In production
this would be a REST catalog (Nessie, Glue, Polaris) over object storage,
but the table format mechanics are identical and this keeps the whole demo
runnable with docker compose alone.

Why @task.virtualenv here: pyiceberg's sql catalog needs SQLAlchemy 2.x,
while Airflow 2.10 itself runs on SQLAlchemy 1.4. Installing pyiceberg in
the main Airflow image crashes the scheduler on import. Running these tasks
in an isolated virtualenv keeps both worlds happy. First run of each task
is slower because the venv gets built.

The second task demonstrates the two things Iceberg gives you that a plain
warehouse table does not:
  1. schema evolution: add a column, old data stays readable
  2. time travel: read the table as of a previous snapshot
"""

from datetime import datetime

from airflow.decorators import dag, task

ICEBERG_REQUIREMENTS = [
    "pyiceberg[sql-sqlite,pyarrow]==0.8.1",
    "clickhouse-connect==0.8.11",
]


@dag(
    dag_id="export_to_iceberg",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demand", "iceberg"],
)
def export_to_iceberg():

    @task.virtualenv(requirements=ICEBERG_REQUIREMENTS, system_site_packages=False)
    def export_table():
        import os

        import clickhouse_connect
        from pyiceberg.catalog.sql import SqlCatalog

        warehouse = "/opt/airflow/iceberg_warehouse"
        os.makedirs(warehouse, exist_ok=True)

        client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        )

        # Arrow all the way through: ClickHouse -> Arrow -> Iceberg/Parquet.
        # Export a monthly aggregate rather than all raw rows so the demo
        # stays fast; swap the query for SELECT * to export everything.
        arrow_table = client.query_arrow(
            """
            SELECT
                toStartOfMonth(order_date) AS month,
                region,
                model,
                sum(quantity) AS units,
                round(sum(unit_price * quantity), 2) AS revenue
            FROM demand.orders
            WHERE status != 5
            GROUP BY month, region, model
            ORDER BY month, region, model
            """
        )

        catalog = SqlCatalog(
            "local",
            uri=f"sqlite:///{warehouse}/catalog.db",
            warehouse=f"file://{warehouse}",
        )
        try:
            catalog.create_namespace("demand")
        except Exception:
            pass  # namespace already exists

        try:
            catalog.drop_table("demand.monthly_demand")
        except Exception:
            pass

        table = catalog.create_table(
            "demand.monthly_demand", schema=arrow_table.schema
        )
        table.append(arrow_table)
        print(f"wrote {arrow_table.num_rows:,} rows to demand.monthly_demand")
        print("snapshot:", table.current_snapshot().snapshot_id)

    @task.virtualenv(requirements=ICEBERG_REQUIREMENTS, system_site_packages=False)
    def demonstrate_schema_evolution_and_time_travel():
        import pyarrow.compute as pc
        from pyiceberg.catalog.sql import SqlCatalog
        from pyiceberg.types import DoubleType

        warehouse = "/opt/airflow/iceberg_warehouse"
        catalog = SqlCatalog(
            "local",
            uri=f"sqlite:///{warehouse}/catalog.db",
            warehouse=f"file://{warehouse}",
        )
        table = catalog.load_table("demand.monthly_demand")

        first_snapshot = table.current_snapshot().snapshot_id
        print("snapshot before evolution:", first_snapshot)

        # Schema evolution: add a column. No rewrite of existing files.
        with table.update_schema() as update:
            update.add_column("avg_unit_price", DoubleType())

        table = catalog.load_table("demand.monthly_demand")
        print("schema after evolution:")
        print(table.schema())

        # Old data is still fully readable with the new schema; the new
        # column just comes back as null for existing rows.
        df = table.scan().to_arrow()
        nulls = pc.sum(pc.is_null(df["avg_unit_price"]).cast("int64")).as_py()
        print(f"rows={df.num_rows:,}, avg_unit_price nulls={nulls:,}")

        # Time travel: read as of the pre-evolution snapshot.
        old = table.scan(snapshot_id=first_snapshot).to_arrow()
        print(
            f"time travel to snapshot {first_snapshot}: "
            f"{old.num_rows:,} rows, columns={old.schema.names}"
        )

    export_table() >> demonstrate_schema_evolution_and_time_travel()


export_to_iceberg()
