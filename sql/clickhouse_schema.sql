-- Reference copy of the ClickHouse warehouse schema.
-- The migrate_sqlserver_to_clickhouse DAG creates this automatically.
--
-- Why this ORDER BY: demand planners filter by region first, then model,
-- then a date range. Matching the sort key to the access pattern means
-- those queries read a small sorted slice of each part instead of the
-- whole table. PARTITION BY month keeps parts small and lets ClickHouse
-- prune whole months on date filters.

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
ORDER BY (region, model, order_date);
