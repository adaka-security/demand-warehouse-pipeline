-- Reference copy of the source (legacy) schema.
-- The generate_demand_data DAG creates this automatically.

CREATE TABLE dbo.orders (
    order_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    order_date DATE NOT NULL,
    region VARCHAR(20) NOT NULL,
    model VARCHAR(10) NOT NULL,
    trim_level VARCHAR(20) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    quantity SMALLINT NOT NULL,
    status TINYINT NOT NULL,          -- 1 placed, 2 confirmed, 3 in production, 4 delivered, 5 cancelled
    delivery_estimate_days SMALLINT NOT NULL
);
