# Demand Warehouse Pipeline

<!-- RENAME THIS FILE TO README.md AFTER FILLING IT IN.
     Everything marked TODO you write yourself, in your own words.
     Delete all these comments before publishing. -->

TODO: 2 or 3 plain sentences in your own words. What it is, why you built it.
Example shape (do not copy): "A demand planning data warehouse built end to
end: synthetic order data lands in SQL Server, an Airflow pipeline migrates
it to ClickHouse, exports it to Apache Iceberg, and a small agent answers
questions in plain English using a documented metadata layer."

## Architecture

TODO: insert your hand drawn Excalidraw diagram here as a screenshot.
Boxes: SQL Server -> Airflow (CeleryExecutor: scheduler, worker, Redis,
Postgres) -> ClickHouse -> Iceberg, plus the metadata layer + agent on the side.

![architecture](docs/architecture.png)

## What it does

- Generates ~3M synthetic vehicle orders with realistic skew (weekend dip, quarter end spike) into SQL Server
- Migrates them to ClickHouse with an Airflow DAG using keyset pagination and post migration validation (row counts + checksums)
- Benchmarks 4 real demand planning queries on both databases
- Exports a monthly aggregate to Apache Iceberg and demonstrates schema evolution and time travel
- Serves plain English questions through a text-to-SQL agent grounded in a documented metadata layer

## Benchmark results

TODO: paste the table from benchmarks/results.md after running it on your
machine. Add one sentence about your hardware (e.g. "MacBook Pro M2, 16GB,
both databases in Docker").

## Why ClickHouse is faster here

TODO: 3 or 4 sentences in your own words. The core points: columnar storage
reads only needed columns; ORDER BY (region, model, order_date) matches the
query access pattern; monthly partitions get pruned on date filters;
LowCardinality dictionary-encodes the region/model/status columns.

## Iceberg: when I'd use it and when I wouldn't

TODO: your honest take, 4 to 6 sentences. Points to consider: Iceberg buys
schema evolution, time travel, and engine interoperability (Spark, Trino,
ClickHouse can all read the same table), which matters when multiple teams
and engines share data. But for a single team serving fast dashboards,
native MergeTree is simpler and faster. So: ClickHouse as the serving
layer, Iceberg as the shared/lakehouse layer, not one replacing the other.

## The metadata layer and the agent

TODO: 3 sentences. Key insight to state: the agent is only as good as the
metadata. Without the business rule "status = 5 means cancelled" in
tables.yml, the generated SQL would be confidently wrong. AI ready data is
mostly a documentation problem.

## Quickstart

```bash
cp .env.example .env        # set a strong MSSQL_SA_PASSWORD
docker compose up -d --build
# wait for airflow-webserver to be healthy, then open http://localhost:8080
# login admin/admin, unpause and trigger in order:
#   1. generate_demand_data
#   2. migrate_sqlserver_to_clickhouse
#   3. export_to_iceberg

# benchmarks (from the host)
pip install pymssql clickhouse-connect
MSSQL_SA_PASSWORD='<your password>' python benchmarks/run_benchmarks.py

# ask the warehouse a question
pip install anthropic pyyaml
export ANTHROPIC_API_KEY=...
python agent/ask_warehouse.py "top 5 regions by units last quarter"
```

Needs Docker Desktop with at least 8GB RAM allocated (SQL Server alone wants ~2GB).

## Problems I hit

TODO: write 2 or 3 real problems you hit today and how you fixed them.
You will have some. This section matters more than you think.

## What I'd do differently with more time

TODO: rewrite these in your own words, keep the ones you agree with:
- Incremental CDC from SQL Server (change tracking) instead of full reloads
- A REST Iceberg catalog (Polaris/Nessie) over object storage instead of SQLite + local files
- Kubernetes with KubernetesExecutor instead of Docker Compose + Celery
- dbt for the aggregate transforms with tests on the business rules
- Great Expectations style data quality checks as a DAG task, not just count/checksum
