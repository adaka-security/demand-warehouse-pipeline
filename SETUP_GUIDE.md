# Setup guide: zero to submitted

DELETE THIS FILE BEFORE PUBLISHING THE REPO. It is your private checklist.

## Prerequisites (30 min)

1. Install Docker Desktop. Settings > Resources: give it at least 8GB RAM.
2. Install Python 3.11+ on your host machine.
3. Have a GitHub account (adaka-security) and an Anthropic API key.
4. Install Excalidraw is not needed, just use https://excalidraw.com in the browser.

## Phase 1: get the stack running (target: 1.5 hrs)

```bash
cd demand-warehouse-pipeline
git init
cp .env.example .env
# edit .env: set a strong MSSQL_SA_PASSWORD (SQL Server rejects weak ones,
# needs 8+ chars with upper, lower, digit, symbol)
# keep DEMAND_ROW_COUNT=200000 for now

docker compose up -d --build
```

First build takes 5 to 15 minutes (pulls SQL Server ~1.5GB, builds Airflow image).

Check everything is healthy:

```bash
docker compose ps
```

All services should show healthy or running. If sqlserver is unhealthy,
your password is probably too weak. If airflow-init failed, run
`docker compose logs airflow-init`.

Open http://localhost:8080, login admin / admin. You should see 3 DAGs.

FIRST COMMIT NOW: `git add -A && git commit -m "docker compose stack up"`
Then keep committing every time something works. 15 to 20 commits by end of
day. Normal messages: "fix clickhouse partition key", "validation task
passing", "benchmarks on 3M rows". Never commit .env.

## Phase 2: run the pipeline small (1 hr)

In the Airflow UI, unpause and trigger in order, waiting for each to go green:

1. `generate_demand_data` (200k rows, a few minutes)
2. `migrate_sqlserver_to_clickhouse` (check the validate task logs, you want
   to see "validation passed")
3. `export_to_iceberg` (check logs of the second task for the schema
   evolution and time travel output)

If a task fails, click it > Logs. Fix, commit the fix.

Verify ClickHouse directly:

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM demand.orders"
```

## Phase 3: scale up and benchmark (1.5 hrs)

1. Edit .env: DEMAND_ROW_COUNT=3000000
2. `docker compose up -d` (recreates airflow containers with the new env)
3. Re-trigger DAG 1 (this will take a while, ~20 to 40 min for 3M rows via
   executemany, go write your README sections while it runs), then DAG 2,
   then DAG 3.
4. From the host:

```bash
pip install pymssql clickhouse-connect
MSSQL_SA_PASSWORD='<your password>' python benchmarks/run_benchmarks.py
```

5. Commit benchmarks/results.md. Whatever the numbers are, they are yours.

## Phase 4: the agent (45 min)

```bash
pip install anthropic clickhouse-connect pyyaml
export ANTHROPIC_API_KEY=<your key>
python agent/ask_warehouse.py "top 5 regions by units last quarter"
python agent/ask_warehouse.py "monthly revenue for MY in EU regions in 2025"
python agent/ask_warehouse.py "how many orders were cancelled by region"
```

Screenshot or screen-record at least one of these for the demo.

## Phase 5: README and diagram (2 hrs, do not compress)

1. Draw the architecture in excalidraw.com by hand, export PNG to
   docs/architecture.png (create the docs folder).
2. Open README_TEMPLATE.md, fill every TODO in your own words, rename to
   README.md, delete the template comments and delete this SETUP_GUIDE.md.
3. Screenshot the Airflow UI with all three DAGs green, add it to docs/ and
   the README.
4. Read the whole repo top to bottom once. Every file. If he calls you, you
   walk through this live.

## Phase 6: publish and record (1 hr)

```bash
# create the repo on github.com under adaka-security first, then:
git remote add origin git@github.com:adaka-security/demand-warehouse-pipeline.git
git branch -M main
git push -u origin main
```

Loom video, 3 to 4 minutes, this exact flow:
1. 20 sec: what it is, in one breath
2. docker compose ps, the Airflow UI with green DAGs
3. the benchmark table, say the p50 numbers for one query out loud
4. run the agent live with one question
5. 20 sec: what you'd do next (CDC, REST catalog, k8s)

Do not script it word for word. One take with a small stumble beats five
polished takes.

## Phase 7: send it (30 min)

LinkedIn message to Anurag, 5 sentences max, project first. Shape (write it
in your own words, do not paste this):

- saw your Fall Data Engineering intern post
- I had already built an Airflow + ClickHouse demand pipeline as a portfolio
  project, and this weekend I extended it to cover your post: SQL Server to
  ClickHouse migration with validation, an Iceberg export with schema
  evolution, a CeleryExecutor Airflow cluster stood up from scratch, and an
  LLM-ready metadata layer with a working text-to-SQL agent
- repo link, demo video link
- enrolled full time, US based, available on site in Fremont for the fall term
- resume attached

Also apply through the official Tesla careers posting if one exists, and say
in the message that you did.

## If you fall behind, cut in this order

1. Iceberg DAG (keep the written trade-off section in the README, mark the
   DAG as work in progress)
2. Agent runs SQL generation only, without executing
3. Drop 3M rows, benchmark at 1M

Never cut: the README, the benchmark table, the validation task, the video.
