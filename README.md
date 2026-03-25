# Retail OLAP Demo — FastAPI + ClickHouse

Real-time OLAP system based on the **star schema** from
_Designing Data-Intensive Applications_ (Kleppmann, Chapter 3, Figure 3-11).

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────┐
│  Dev machine                                            │
│                                                         │
│   uv run uvicorn app.main:app --reload                  │
│       │                                                 │
│       │  HTTP / pandas DataFrame                        │
│       ▼                                                 │
│  ┌──────────────────────────────────────────────────┐   │
│  │  FastAPI (port 8000)                             │   │
│  │  ├── /dimensions/*   look-up dimension tables    │   │
│  │  └── /analytics/*   OLAP query endpoints         │   │
│  └────────────────────────┬─────────────────────────┘   │
│                           │  clickhouse-connect          │
│                           │  (HTTP, port 8123)           │
│                           ▼                             │
│  ┌──────────────────────────────────────────────────┐   │
│  │  ClickHouse  (Docker, port 8123 / 9000)          │   │
│  │  database: retail_dw                             │   │
│  │  ├── fact_sales        (central fact table)      │   │
│  │  ├── dim_product       what was sold             │   │
│  │  ├── dim_store         where it was sold         │   │
│  │  ├── dim_date          when it was sold          │   │
│  │  ├── dim_customer      who bought (loyalty only) │   │
│  │  └── dim_promotion     active promotion          │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Tool | Minimum version | Install |
|------|-----------------|---------|
| Docker Desktop | 24 | https://docs.docker.com/get-docker/ |
| `uv` | 0.4 | `pip install uv` or https://docs.astral.sh/uv/ |
| Python | 3.11 | managed automatically by `uv` |

---

## Running on a dev machine

### 1 — Start ClickHouse

```bash
docker-compose up -d
```

Wait until the health-check passes (≈10 s):

```bash
docker-compose ps          # STATUS should show "(healthy)"
```

Verify manually:

```bash
curl http://localhost:8123/ping   # → Ok.
```

### 2 — Create the virtual environment and install dependencies

```bash
uv sync
```

`uv` reads `pyproject.toml`, creates `.venv/`, and installs all packages.

### 3 — Run database migrations

```bash
uv run python scripts/migrate.py
```

This executes the two SQL files in `migrations/` in order:

```
001_create_schema.sql   — CREATE TABLE for all 5 dimension + 1 fact table
002_insert_data.sql     — INSERT sample data (Jan–Feb 2014, ~55 transactions)
```

Completed migrations are recorded in `retail_dw._migrations` so re-running is safe.

### 4 — Start the FastAPI app

```bash
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Open in browser:

| URL | What you see |
|-----|-------------|
| http://localhost:8000/docs | Swagger UI — try every endpoint interactively |
| http://localhost:8000/redoc | ReDoc — clean reference documentation |
| http://localhost:8000/health | ClickHouse connectivity + version |

---

## Project structure

```
clickhouse/
├── docker-compose.yml          ClickHouse container (only)
├── pyproject.toml              uv / PEP 517 project manifest
├── .python-version             pins Python 3.11 for uv
│
├── migrations/
│   ├── 001_create_schema.sql   DDL — all tables with ClickHouse annotations
│   └── 002_insert_data.sql     sample data (matches DDIA book + extras)
│
├── scripts/
│   └── migrate.py              migration runner (tracks applied migrations)
│
└── app/
    ├── config.py               pydantic-settings — reads env vars / .env
    ├── database.py             ClickHouse client + pandas helpers
    ├── main.py                 FastAPI app, router registration, /health
    └── routers/
        ├── dimensions.py       GET /dimensions/* — look-up tables
        └── analytics.py        GET /analytics/*  — all OLAP endpoints
```

---

## Key design decisions

### ClickHouse table engines

Every table uses `MergeTree` — ClickHouse's primary engine family.

```sql
-- Fact table: partitioned by month, primary index on (date, product, store)
CREATE TABLE fact_sales (...)
ENGINE = MergeTree()
PARTITION BY intDiv(date_key, 100)   -- 140102 → partition 1401 (Jan 2014)
ORDER BY (date_key, product_sk, store_sk);
```

`PARTITION BY` lets ClickHouse skip entire monthly data files for date-filtered queries.
`ORDER BY` builds a **sparse primary index** — queries filtering on `date_key + product_sk` read only a small fraction of the data on disk.

### LowCardinality(String)

Columns with few distinct values (state, category, weekday) use `LowCardinality(String)`.  ClickHouse stores them as integer dictionary IDs — `GROUP BY` on them compares integers, not strings.

```sql
category LowCardinality(String),   -- 4 distinct values: ~4× compression
state    LowCardinality(String),   -- 4 distinct values
weekday  LowCardinality(String),   -- 7 distinct values
```

### pandas DataFrames as the result layer

`clickhouse-connect`'s `query_df()` uses the **Arrow columnar format** for transport — much faster than row-by-row conversion for large result sets.

```python
# app/database.py
def query_df(sql, parameters=None) -> pd.DataFrame:
    client = get_client()
    return client.query_df(sql, parameters=parameters)  # Arrow → pandas
```

Endpoints return `df_to_records(df)` which handles pandas `NA`/`NaT`/numpy types that FastAPI's JSON encoder cannot serialise by itself.

### Safe parameterised queries

ClickHouse's own `{name:Type}` placeholder syntax is used — no string interpolation, no SQL injection risk:

```python
client.query_df(
    "SELECT * FROM dim_product WHERE category = {cat:String}",
    parameters={"cat": "Bakery"}
)
```

---

## OLAP operations — endpoint map

| OLAP operation | Endpoint | Description |
|----------------|----------|-------------|
| **Roll-up** | `GET /analytics/sales/by-day` | Daily totals |
| **Roll-up** | `GET /analytics/sales/by-month` | Roll day → month |
| **Roll-up** | `GET /analytics/sales/rollup-hierarchical` | `GROUP BY ROLLUP` subtotals |
| **Drill-down** | `GET /analytics/sales/by-category` | Category summaries |
| **Drill-down** | `GET /analytics/sales/by-category/{cat}/products` | Products within a category |
| **Slice** | `GET /analytics/sales/by-state/{state}` | One state's stores |
| **Slice** | `GET /analytics/sales/holiday-vs-regular` | Holiday vs regular days |
| **Dice** | `GET /analytics/sales/dice` | Multi-dimension filter |
| **Pivot** | `GET /analytics/sales/category-by-month` | Category × month cross-tab |
| Analytics | `GET /analytics/promotions/effectiveness` | Discount lift per promotion |
| Analytics | `GET /analytics/products/top-n` | Top-N by revenue + `RANK()` |
| Analytics | `GET /analytics/customers/spending` | Per-customer totals + `RANK()` |
| Analytics | `GET /analytics/stores/performance` | Store ranking + `countIf()` |
| Analytics | `GET /analytics/sales/discount-analysis` | Discount depth by category |
| Analytics | `GET /analytics/sales/price-quantiles` | P25/P50/P75/P95 via `quantile()` |
| Analytics | `GET /analytics/sales/by-brand` | Brand performance |
| Analytics | `GET /analytics/sales/by-weekday` | Day-of-week patterns |

---

## Configuration

All ClickHouse connection settings are read from environment variables (or a `.env` file at the project root).

```bash
# .env  (optional — defaults work for the Docker setup)
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=retail_dw
```

---

## Modifying the project

### Add a new dimension table

1. **Write the migration** — add a new numbered file in `migrations/`:

```sql
-- migrations/003_add_dim_supplier.sql
CREATE TABLE IF NOT EXISTS retail_dw.dim_supplier
(
    supplier_sk UInt32,
    name        String,
    country     LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY supplier_sk;

INSERT INTO retail_dw.dim_supplier VALUES
(1, 'Freshmax Corp', 'US'),
(2, 'Aquatech Ltd',  'UK');
```

2. **Run the migration runner** — it detects and applies only the new file:

```bash
uv run python scripts/migrate.py
```

3. **Add a look-up endpoint** in `app/routers/dimensions.py`:

```python
@router.get("/suppliers")
def list_suppliers():
    df = query_df("SELECT * FROM dim_supplier ORDER BY supplier_sk")
    return df_to_records(df)
```

---

### Add a new OLAP query

Add a function to `app/routers/analytics.py`:

```python
@router.get("/sales/by-country", summary="Sales by supplier country")
def sales_by_country():
    """
    Joins fact_sales → dim_product → dim_supplier to roll up by country.
    """
    sql = """
    SELECT
        ds.country,
        count()                                         AS num_transactions,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue
    FROM fact_sales fs
    JOIN dim_product  dp ON fs.product_sk  = dp.product_sk
    JOIN dim_supplier ds ON dp.supplier_sk = ds.supplier_sk
    GROUP BY ds.country
    ORDER BY total_revenue DESC
    """
    df = query_df(sql)
    return df_to_records(df)
```

FastAPI auto-registers the route and shows it in Swagger immediately (hot-reload is on).

---

### Work with the DataFrame before returning it

`query_df()` returns a plain pandas DataFrame — apply any transformation before serialising:

```python
@router.get("/sales/pivot-matrix")
def pivot_matrix():
    df = query_df("""
        SELECT dd.month, dp.category,
               round(sum(fs.discount_price * fs.quantity), 2) AS revenue
        FROM fact_sales fs
        JOIN dim_date    dd ON fs.date_key   = dd.date_key
        JOIN dim_product dp ON fs.product_sk = dp.product_sk
        GROUP BY dd.month, dp.category
    """)
    # Pivot: months as index, categories as columns
    pivot = df.pivot_table(index="month", columns="category",
                           values="revenue", fill_value=0)
    return pivot.reset_index().to_dict(orient="records")
```

---

### Point VS Code to the project interpreter

The IDE diagnostics showing "Package X not installed" mean VS Code is using the global Python rather than the project's `.venv`.

**Fix:** Press `Ctrl+Shift+P` → _Python: Select Interpreter_ → choose
`.venv\Scripts\python.exe` (Windows) or `.venv/bin/python` (Mac/Linux).

---

## Stopping and resetting

```bash
# Stop ClickHouse (keeps data)
docker-compose stop

# Stop and delete all data (volumes)
docker-compose down -v

# Re-run from scratch after docker-compose up -d
uv run python scripts/migrate.py
```
