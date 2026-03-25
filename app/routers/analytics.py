"""
OLAP Analytics endpoints.

Each endpoint demonstrates a classic OLAP operation or a ClickHouse-specific
analytical feature.  Results are returned as pandas DataFrames converted to
JSON records.

OLAP operations covered
-----------------------
  Roll-up      → aggregate from fine grain to coarser grain (day → month)
  Drill-down   → navigate from summary to detail (category → products)
  Slice        → restrict to a single dimension value (one state)
  Dice         → restrict to multiple dimension values simultaneously
  Pivot        → cross-tabulate two dimensions (category × month)

ClickHouse features highlighted
--------------------------------
  GROUP BY ROLLUP     — hierarchical subtotals in one query
  Window functions    — RANK() OVER (ORDER BY ...)
  quantile()          — approximate percentile in O(1) memory
  countIf()           — conditional aggregation
  isNull() / isNotNull() — NULL-aware filtering on Nullable columns
  LowCardinality      — dictionary-encoded strings (compression + speed)
  PARTITION BY        — partition pruning on the fact table
"""
from fastapi import APIRouter, Query

from app.database import query_df, df_to_records

router = APIRouter()


# ============================================================================
# ROLL-UP  — aggregate from fine grain to coarser grain
# ============================================================================

@router.get(
    "/sales/by-day",
    summary="[Roll-up] Daily sales totals",
    tags=["Roll-up"],
)
def sales_by_day():
    """
    **Roll-up — day level.**

    Aggregates every transaction into a daily summary.  This is the finest
    grain available in our star schema.

    ClickHouse scans only the `fact_sales` and `dim_date` columns it needs
    (columnar storage) and uses the `date_key` primary index to sort the output
    without a full sort step.
    """
    sql = """
    SELECT
        dd.date_key,
        dd.year,
        dd.month,
        dd.day,
        dd.weekday,
        dd.is_holiday,
        count()                                                 AS num_transactions,
        sum(fs.quantity)                                        AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2)         AS total_revenue,
        round(sum(fs.net_price     * fs.quantity), 2)          AS total_list_revenue,
        round(sum((fs.net_price - fs.discount_price) * fs.quantity), 2) AS total_discount_given
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dd.date_key, dd.year, dd.month, dd.day, dd.weekday, dd.is_holiday
    ORDER BY dd.date_key
    """
    df = query_df(sql)
    return df_to_records(df)


@router.get(
    "/sales/by-month",
    summary="[Roll-up] Monthly sales totals",
    tags=["Roll-up"],
)
def sales_by_month():
    """
    **Roll-up — month level.**

    Rolls daily transactions up to monthly summaries by grouping on
    `(year, month)`.  This discards the day grain — a classic OLAP roll-up.

    ClickHouse can satisfy this query by scanning only the monthly partitions
    touched (partition pruning) even when the table has years of data.
    """
    sql = """
    SELECT
        dd.year,
        dd.month,
        count()                                         AS num_transactions,
        sum(fs.quantity)                                AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue,
        round(avg(fs.discount_price * fs.quantity), 2) AS avg_basket_value
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dd.year, dd.month
    ORDER BY dd.year, dd.month
    """
    df = query_df(sql)
    return df_to_records(df)


@router.get(
    "/sales/rollup-hierarchical",
    summary="[Roll-up] GROUP BY ROLLUP: category → grand total",
    tags=["Roll-up"],
)
def sales_rollup_hierarchical():
    """
    **ClickHouse GROUP BY ROLLUP.**

    A single query returns three levels of aggregation simultaneously:
    1. `(category, state)` — every combination
    2. `(category, '')` — subtotal per category across all states
    3. `('', '')` — grand total across everything

    ClickHouse fills the missing dimension keys with their column default
    (empty string for `LowCardinality(String)`).  Rows where `state = ''`
    are subtotals; the row where both are `''` is the grand total.
    """
    sql = """
    SELECT
        dp.category,
        ds.state,
        count()                                         AS num_transactions,
        sum(fs.quantity)                                AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_sk = dp.product_sk
    JOIN dim_store   ds ON fs.store_sk   = ds.store_sk
    GROUP BY ROLLUP(dp.category, ds.state)
    ORDER BY dp.category, ds.state
    """
    df = query_df(sql)
    return df_to_records(df)


# ============================================================================
# DRILL-DOWN  — navigate from summary level to detail level
# ============================================================================

@router.get(
    "/sales/by-category",
    summary="[Drill-down] Sales totals by product category",
    tags=["Drill-down"],
)
def sales_by_category():
    """
    **Drill-down — category level summary.**

    Groups all sales by product category.  Use `GET /sales/by-category/{category}/products`
    to drill further down into the individual products within a category.
    """
    sql = """
    SELECT
        dp.category,
        count()                                         AS num_transactions,
        sum(fs.quantity)                                AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue,
        round(avg(fs.discount_price), 2)               AS avg_unit_price
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_sk = dp.product_sk
    GROUP BY dp.category
    ORDER BY total_revenue DESC
    """
    df = query_df(sql)
    return df_to_records(df)


@router.get(
    "/sales/by-category/{category}/products",
    summary="[Drill-down] Products within a category",
    tags=["Drill-down"],
)
def sales_by_category_products(category: str):
    """
    **Drill-down — product level within a category.**

    Drills from the category summary into individual products.
    `category` is case-sensitive (e.g. `Fresh fruit`, `Bakery`, `Pet supplies`).

    The parameterised query uses ClickHouse's `{name:Type}` syntax, handled
    safely by `clickhouse-connect` — no string interpolation, no SQL injection.
    """
    sql = """
    SELECT
        dp.product_sk,
        dp.sku,
        dp.description,
        dp.brand,
        count()                                         AS num_transactions,
        sum(fs.quantity)                                AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue,
        round(avg(fs.discount_price), 2)               AS avg_unit_price
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_sk = dp.product_sk
    WHERE dp.category = {category:String}
    GROUP BY dp.product_sk, dp.sku, dp.description, dp.brand
    ORDER BY total_revenue DESC
    """
    df = query_df(sql, parameters={"category": category})
    return df_to_records(df)


# ============================================================================
# SLICE  — restrict to a single dimension value
# ============================================================================

@router.get(
    "/sales/by-state/{state}",
    summary="[Slice] Sales for one US state",
    tags=["Slice"],
)
def sales_by_state(state: str):
    """
    **Slice — one state.**

    Restricts the fact table to stores in a given state (e.g. `CA`, `WA`, `NY`).
    This is the OLAP *slice* operation: we fix one dimension attribute and
    look at all the others.
    """
    sql = """
    SELECT
        ds.store_sk,
        ds.city,
        ds.state,
        count()                                         AS num_transactions,
        sum(fs.quantity)                                AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue,
        round(avg(fs.discount_price * fs.quantity), 2) AS avg_transaction_value
    FROM fact_sales fs
    JOIN dim_store ds ON fs.store_sk = ds.store_sk
    WHERE ds.state = {state:String}
    GROUP BY ds.store_sk, ds.city, ds.state
    ORDER BY total_revenue DESC
    """
    df = query_df(sql, parameters={"state": state})
    return df_to_records(df)


@router.get(
    "/sales/holiday-vs-regular",
    summary="[Slice] Holiday days vs regular days",
    tags=["Slice"],
)
def sales_holiday_vs_regular():
    """
    **Slice — holiday flag.**

    Slices the date dimension on `is_holiday` to compare buying patterns
    on public holidays versus regular days.

    Note how `avg_basket_value` (revenue per transaction) changes — holiday
    shoppers often buy more per visit.
    """
    sql = """
    SELECT
        if(dd.is_holiday = 1, 'Holiday', 'Regular') AS day_type,
        count()                                         AS num_transactions,
        sum(fs.quantity)                                AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue,
        round(avg(fs.discount_price * fs.quantity), 2) AS avg_basket_value
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dd.is_holiday
    ORDER BY dd.is_holiday DESC
    """
    df = query_df(sql)
    return df_to_records(df)


# ============================================================================
# DICE  — restrict to multiple dimension values simultaneously
# ============================================================================

@router.get(
    "/sales/dice",
    summary="[Dice] Multi-dimension filter",
    tags=["Dice"],
)
def sales_dice(
    state: str | None = Query(default=None, description="Filter by store state, e.g. CA"),
    category: str | None = Query(default=None, description="Filter by product category, e.g. Bakery"),
    from_date: int | None = Query(default=None, description="Start date_key (YYMMDD), e.g. 140101"),
    to_date: int | None = Query(default=None, description="End date_key (YYMMDD), e.g. 140131"),
    with_promotion: bool | None = Query(default=None, description="True = promoted sales only"),
    limit: int = Query(default=50, ge=1, le=500),
):
    """
    **Dice — arbitrary combination of filters.**

    The OLAP *dice* operation restricts the cube along multiple dimensions
    at once.  All parameters are optional — omit any to include all values
    for that dimension.

    Examples:
    - `?state=CA&category=Bakery` — CA bakery sales
    - `?with_promotion=true&from_date=140101&to_date=140107` — promoted sales in first week
    - `?state=WA` — all Washington state sales

    Dynamic WHERE clauses are built safely: each condition uses ClickHouse's
    `{name:Type}` parameterisation, never raw string interpolation.
    """
    # Build WHERE conditions and params dict dynamically
    conditions: list[str] = []
    params: dict = {"limit": limit}

    if state is not None:
        conditions.append("ds.state = {state:String}")
        params["state"] = state

    if category is not None:
        conditions.append("dp.category = {category:String}")
        params["category"] = category

    if from_date is not None:
        conditions.append("fs.date_key >= {from_date:UInt32}")
        params["from_date"] = from_date

    if to_date is not None:
        conditions.append("fs.date_key <= {to_date:UInt32}")
        params["to_date"] = to_date

    if with_promotion is True:
        conditions.append("isNotNull(fs.promotion_sk)")
    elif with_promotion is False:
        conditions.append("isNull(fs.promotion_sk)")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    sql = (
        "SELECT"
        "    fs.date_key,"
        "    dd.month,"
        "    dp.category,"
        "    dp.description  AS product,"
        "    ds.state,"
        "    ds.city,"
        "    if(isNull(fs.promotion_sk), 'No', dpr.name) AS promotion,"
        "    dc.name AS customer,"
        "    fs.quantity,"
        "    fs.net_price,"
        "    fs.discount_price,"
        "    round(fs.discount_price * fs.quantity, 2) AS line_total "
        "FROM fact_sales fs "
        "JOIN dim_date dd ON fs.date_key = dd.date_key "
        "JOIN dim_product dp ON fs.product_sk = dp.product_sk "
        "JOIN dim_store ds ON fs.store_sk = ds.store_sk "
        "LEFT JOIN dim_promotion dpr ON fs.promotion_sk = dpr.promotion_sk "
        "LEFT JOIN dim_customer  dc  ON fs.customer_sk  = dc.customer_sk "
        + where +
        " ORDER BY fs.date_key, line_total DESC"
        " LIMIT {limit:UInt32}"
    )

    df = query_df(sql, parameters=params)
    return df_to_records(df)


# ============================================================================
# PIVOT  — cross-tabulate two dimensions
# ============================================================================

@router.get(
    "/sales/category-by-month",
    summary="[Pivot] Revenue: product category × month",
    tags=["Pivot"],
)
def sales_category_by_month():
    """
    **Pivot — category × month.**

    Returns one row per `(year, month, category)` combination.  The client
    can pivot this into a matrix (category as rows, month as columns) to see
    how each category performs across time — the classic OLAP pivot view.

    ClickHouse does not have a built-in PIVOT syntax, but the raw cross-tab
    data is returned here for the client to reshape (e.g. with pandas
    `df.pivot_table()`).
    """
    sql = """
    SELECT
        dd.year,
        dd.month,
        dp.category,
        count()                                         AS num_transactions,
        sum(fs.quantity)                                AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue
    FROM fact_sales fs
    JOIN dim_date    dd ON fs.date_key   = dd.date_key
    JOIN dim_product dp ON fs.product_sk = dp.product_sk
    GROUP BY dd.year, dd.month, dp.category
    ORDER BY dd.year, dd.month, total_revenue DESC
    """
    df = query_df(sql)
    return df_to_records(df)


# ============================================================================
# PROMOTION ANALYSIS
# ============================================================================

@router.get(
    "/promotions/effectiveness",
    summary="Promotion effectiveness — discount lift & revenue impact",
)
def promotion_effectiveness():
    """
    Compares promoted vs non-promoted transactions.

    For each promotion (plus a "No promotion" group), shows:
    - How many transactions used it
    - Total revenue vs list revenue
    - The discount percentage granted

    ClickHouse's `isNull()` / `isNotNull()` functions work correctly on
    `Nullable(UInt32)` columns — plain `= NULL` would not.
    """
    sql = """
    SELECT
        if(isNull(fs.promotion_sk), 'No promotion', dpr.name)  AS promotion,
        if(isNull(fs.promotion_sk), 'None', dpr.ad_type)       AS ad_type,
        count()                                                 AS num_transactions,
        sum(fs.quantity)                                        AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2)         AS actual_revenue,
        round(sum(fs.net_price * fs.quantity), 2)              AS list_revenue,
        round(
            (1 - sum(fs.discount_price * fs.quantity)
                 / sum(fs.net_price * fs.quantity)) * 100,
        2)                                                      AS discount_pct
    FROM fact_sales fs
    LEFT JOIN dim_promotion dpr ON fs.promotion_sk = dpr.promotion_sk
    GROUP BY fs.promotion_sk, dpr.name, dpr.ad_type
    ORDER BY actual_revenue DESC
    """
    df = query_df(sql)
    return df_to_records(df)


# ============================================================================
# TOP-N PRODUCTS  (window functions)
# ============================================================================

@router.get(
    "/products/top-n",
    summary="Top-N products by revenue with rank",
)
def top_n_products(n: int = Query(default=5, ge=1, le=50)):
    """
    Returns the top *N* products by total revenue, with a `revenue_rank`
    column computed by a **window function**.

    ```sql
    rank() OVER (ORDER BY sum(...) DESC)
    ```

    ClickHouse supports standard SQL window functions.  The outer SELECT
    wraps the aggregation because window functions are evaluated after GROUP BY.
    """
    sql = """
    SELECT
        product_sk,
        sku,
        description,
        category,
        brand,
        num_transactions,
        total_units_sold,
        total_revenue,
        rank() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM (
        SELECT
            dp.product_sk,
            dp.sku,
            dp.description,
            dp.category,
            dp.brand,
            count()                                         AS num_transactions,
            sum(fs.quantity)                                AS total_units_sold,
            round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue
        FROM fact_sales fs
        JOIN dim_product dp ON fs.product_sk = dp.product_sk
        GROUP BY dp.product_sk, dp.sku, dp.description, dp.category, dp.brand
    )
    ORDER BY revenue_rank
    LIMIT {n:UInt32}
    """
    df = query_df(sql, parameters={"n": n})
    return df_to_records(df)


# ============================================================================
# CUSTOMER SPENDING
# ============================================================================

@router.get(
    "/customers/spending",
    summary="Customer spending summary (identified shoppers only)",
)
def customer_spending():
    """
    Summarises spending for each identified customer (loyalty card holders).

    Anonymous transactions (`customer_sk IS NULL`) are excluded — they have
    no entry in `dim_customer`.

    `age` is calculated inline using ClickHouse's `toYear()` date function.
    """
    sql = """
    SELECT
        dc.customer_sk,
        dc.name,
        dc.date_of_birth,
        toYear(today()) - toYear(dc.date_of_birth)      AS age,
        count()                                          AS num_purchases,
        sum(fs.quantity)                                 AS total_units_bought,
        round(sum(fs.discount_price * fs.quantity), 2)  AS total_spending,
        round(avg(fs.discount_price * fs.quantity), 2)  AS avg_basket_value,
        rank() OVER (ORDER BY sum(fs.discount_price * fs.quantity) DESC) AS spending_rank
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_sk = dc.customer_sk
    GROUP BY dc.customer_sk, dc.name, dc.date_of_birth
    ORDER BY spending_rank
    """
    df = query_df(sql)
    return df_to_records(df)


# ============================================================================
# STORE PERFORMANCE
# ============================================================================

@router.get(
    "/stores/performance",
    summary="Store performance ranking",
)
def store_performance():
    """
    Ranks every store by total revenue using a window function.

    Also shows what fraction of each store's transactions used a promotion —
    a useful signal for understanding promotional dependency.

    `countIf()` is a ClickHouse aggregate that counts only rows matching a
    predicate, equivalent to `COUNT(CASE WHEN ... THEN 1 END)` in standard SQL.
    """
    sql = """
    SELECT
        ds.store_sk,
        ds.city,
        ds.state,
        count()                                                 AS num_transactions,
        sum(fs.quantity)                                        AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2)         AS total_revenue,
        round(avg(fs.discount_price * fs.quantity), 2)         AS avg_transaction_value,
        countIf(isNotNull(fs.promotion_sk))                    AS promoted_transactions,
        round(countIf(isNotNull(fs.promotion_sk)) / count() * 100, 1) AS promo_pct,
        rank() OVER (ORDER BY sum(fs.discount_price * fs.quantity) DESC) AS revenue_rank
    FROM fact_sales fs
    JOIN dim_store ds ON fs.store_sk = ds.store_sk
    GROUP BY ds.store_sk, ds.city, ds.state
    ORDER BY revenue_rank
    """
    df = query_df(sql)
    return df_to_records(df)


# ============================================================================
# DISCOUNT ANALYSIS
# ============================================================================

@router.get(
    "/sales/discount-analysis",
    summary="Discount depth analysis by product category",
)
def discount_analysis():
    """
    Analyses how deeply each product category is discounted.

    `countIf(fs.net_price != fs.discount_price)` counts only discounted
    rows — a ClickHouse-idiomatic way to do conditional aggregation without
    a subquery or CASE expression.
    """
    sql = """
    SELECT
        dp.category,
        round(avg(fs.net_price), 2)           AS avg_list_price,
        round(avg(fs.discount_price), 2)      AS avg_sale_price,
        round(
            (1 - avg(fs.discount_price) / avg(fs.net_price)) * 100,
        2)                                    AS avg_discount_pct,
        count()                               AS total_transactions,
        countIf(fs.net_price != fs.discount_price) AS discounted_transactions,
        round(
            countIf(fs.net_price != fs.discount_price) / count() * 100,
        1)                                    AS pct_transactions_discounted
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_sk = dp.product_sk
    GROUP BY dp.category
    ORDER BY avg_discount_pct DESC
    """
    df = query_df(sql)
    return df_to_records(df)


# ============================================================================
# PRICE QUANTILES  (ClickHouse-specific analytical function)
# ============================================================================

@router.get(
    "/sales/price-quantiles",
    summary="Price distribution quantiles by category",
)
def price_quantiles():
    """
    Uses ClickHouse's **`quantile()`** aggregate to compute approximate
    percentiles in a single pass over the data.

    ClickHouse uses the *t-digest* algorithm — extremely memory-efficient
    even over billions of rows.  Standard SQL `PERCENTILE_CONT` requires
    sorting the entire dataset; ClickHouse's `quantile()` does not.

    Returns P25, median (P50), P75, and P95 of the actual sale price per
    product category.
    """
    sql = """
    SELECT
        dp.category,
        count()                                     AS num_transactions,
        round(min(fs.discount_price), 2)            AS min_price,
        round(quantile(0.25)(fs.discount_price), 2) AS p25,
        round(quantile(0.50)(fs.discount_price), 2) AS median,
        round(quantile(0.75)(fs.discount_price), 2) AS p75,
        round(quantile(0.95)(fs.discount_price), 2) AS p95,
        round(max(fs.discount_price), 2)            AS max_price
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_sk = dp.product_sk
    GROUP BY dp.category
    ORDER BY median DESC
    """
    df = query_df(sql)
    return df_to_records(df)


# ============================================================================
# BRAND PERFORMANCE
# ============================================================================

@router.get(
    "/sales/by-brand",
    summary="Sales totals by brand",
)
def sales_by_brand():
    """
    Groups sales by product brand across all categories.

    Demonstrates a simple GROUP BY on a `LowCardinality(String)` column.
    ClickHouse stores such columns in a dictionary format — GROUP BY on them
    works on integer dictionary IDs rather than comparing full strings.
    """
    sql = """
    SELECT
        dp.brand,
        groupArray(DISTINCT dp.category)                AS categories,
        count()                                         AS num_transactions,
        sum(fs.quantity)                                AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue,
        round(avg(fs.discount_price), 2)               AS avg_unit_price
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_sk = dp.product_sk
    GROUP BY dp.brand
    ORDER BY total_revenue DESC
    """
    df = query_df(sql)
    return df_to_records(df)


# ============================================================================
# WEEKDAY PATTERNS
# ============================================================================

@router.get(
    "/sales/by-weekday",
    summary="Sales patterns by day of week",
)
def sales_by_weekday():
    """
    Aggregates all transactions by day-of-week name.

    Because `dim_date.weekday` is `LowCardinality(String)` with only 7
    distinct values, this GROUP BY is extremely fast — ClickHouse scans
    the dictionary, not the raw string data.
    """
    sql = """
    SELECT
        dd.weekday,
        count()                                         AS num_transactions,
        sum(fs.quantity)                                AS total_units_sold,
        round(sum(fs.discount_price * fs.quantity), 2) AS total_revenue,
        round(avg(fs.discount_price * fs.quantity), 2) AS avg_basket_value
    FROM fact_sales fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dd.weekday
    ORDER BY total_revenue DESC
    """
    df = query_df(sql)
    return df_to_records(df)
