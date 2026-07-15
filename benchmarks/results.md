# Benchmark results

Same queries, 10 runs each, wall clock seconds. Both databases running in Docker on the same machine.

| Query | SQL Server p50 | SQL Server p95 | ClickHouse p50 | ClickHouse p95 | Speedup (p50) |
|---|---|---|---|---|---|
| Monthly demand by region | 0.098s | 0.344s | 0.025s | 0.093s | 4x |
| Top models by revenue, last 4 quarters | 0.046s | 0.173s | 0.013s | 0.020s | 3x |
| Rolling 7 day order rate, NA-West | 0.040s | 0.091s | 0.013s | 0.021s | 3x |
| Year over year units by model | 0.070s | 0.074s | 0.017s | 0.018s | 4x |
