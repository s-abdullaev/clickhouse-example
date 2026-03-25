"""
Retail OLAP API — FastAPI + ClickHouse educational demo.

Based on the star schema from "Designing Data-Intensive Applications"
by Martin Kleppmann (Chapter 3, Figure 3-11).

Run:
    uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

Swagger UI:  http://localhost:8000/docs
ReDoc:       http://localhost:8000/redoc
"""
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.routers import dimensions, analytics

app = FastAPI(
    title="Retail OLAP API",
    description="""
## Real-time OLAP with FastAPI + ClickHouse

Educational demo of an analytical system built on the **star schema** from
_Designing Data-Intensive Applications_ (Kleppmann, Chapter 3).

### Star schema tables

| Table | Role |
|-------|------|
| `fact_sales` | Central fact table — one row per product line |
| `dim_product` | *What* was sold |
| `dim_store` | *Where* it was sold |
| `dim_date` | *When* it was sold |
| `dim_customer` | *Who* bought (loyalty card holders only) |
| `dim_promotion` | Active promotion at time of sale |

### OLAP operations demonstrated

| Operation | Endpoint |
|-----------|----------|
| **Roll-up** | `/analytics/sales/by-day`, `by-month`, `rollup-hierarchical` |
| **Drill-down** | `/analytics/sales/by-category`, `by-category/{cat}/products` |
| **Slice** | `/analytics/sales/by-state/{state}`, `holiday-vs-regular` |
| **Dice** | `/analytics/sales/dice` (multi-filter) |
| **Pivot** | `/analytics/sales/category-by-month` |

### ClickHouse features highlighted
- `MergeTree` engine with `PARTITION BY` and `ORDER BY` (sparse primary index)
- `LowCardinality(String)` — dictionary-encoded low-cardinality columns
- `GROUP BY ROLLUP` — hierarchical subtotals
- Window functions: `rank() OVER (...)`
- `quantile()` — approximate percentiles (t-digest algorithm)
- `countIf()` — conditional aggregation
- Parameterised queries via `clickhouse-connect` (`{name:Type}` syntax)
- `pandas` DataFrames as the result format (Arrow columnar transport)
""",
    version="0.1.0",
    contact={"name": "DDIA Demos"},
    license_info={"name": "MIT"},
)

app.include_router(
    dimensions.router,
    prefix="/dimensions",
    tags=["Dimensions"],
)

app.include_router(
    analytics.router,
    prefix="/analytics",
    tags=["OLAP Analytics"],
)


@app.get("/", include_in_schema=False)
def root():
    return JSONResponse({
        "message": "Retail OLAP API is running",
        "docs": "/docs",
        "redoc": "/redoc",
    })


@app.get("/health", tags=["Meta"], summary="ClickHouse connectivity check")
def health():
    """
    Pings ClickHouse and returns the server version.
    Useful for verifying the Docker container is up before running migrations.
    """
    from app.database import get_client
    client = get_client()
    version = client.server_version
    return {"status": "ok", "clickhouse_version": version}
