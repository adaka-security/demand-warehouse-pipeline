"""
Ask the warehouse a question in plain English.

Loads the metadata layer (metadata/tables.yml), hands it to Claude as
context, gets back a ClickHouse SQL query, runs it, prints the results.

The interesting part is not the agent, it is the metadata. Without the
business rules in tables.yml (like "status = 5 means cancelled, exclude it")
the generated SQL would be confidently wrong. AI-ready data is mostly a
documentation problem.

Usage:
    pip install anthropic clickhouse-connect pyyaml
    export ANTHROPIC_API_KEY=...
    python agent/ask_warehouse.py "top 5 regions by units last quarter"
"""

import os
import sys

import clickhouse_connect
import yaml
from anthropic import Anthropic

METADATA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "metadata", "tables.yml"
)

SYSTEM_PROMPT = """You translate business questions into ClickHouse SQL.

You are given the warehouse metadata below. Follow every business rule in it.
Respond with ONLY the SQL query. No markdown fences, no explanation, no
preamble. The query must be valid ClickHouse SQL and must be read only
(SELECT only, never INSERT/ALTER/DROP).

Warehouse metadata:

{metadata}
"""


def main():
    if len(sys.argv) < 2:
        print('usage: python agent/ask_warehouse.py "your question"')
        sys.exit(1)
    question = sys.argv[1]

    with open(METADATA_PATH) as f:
        metadata = f.read()

    client = Anthropic()  # reads ANTHROPIC_API_KEY from the environment
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        system=SYSTEM_PROMPT.format(metadata=metadata),
        messages=[{"role": "user", "content": question}],
    )
    sql = response.content[0].text.strip()
    # Strip fences if the model adds them anyway
    if sql.startswith("```"):
        sql = sql.strip("`")
        if sql.lower().startswith("sql"):
            sql = sql[3:]
        sql = sql.strip()

    # Cheap guardrail: read only
    if not sql.lstrip().lower().startswith(("select", "with")):
        print("refusing to run non-SELECT statement:\n", sql)
        sys.exit(1)

    print("\ngenerated SQL:\n")
    print(sql)

    ch = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    result = ch.query(sql)

    print("\nresults:\n")
    print(" | ".join(result.column_names))
    for row in result.result_rows[:25]:
        print(" | ".join(str(v) for v in row))
    if len(result.result_rows) > 25:
        print(f"... {len(result.result_rows) - 25} more rows")


if __name__ == "__main__":
    main()
