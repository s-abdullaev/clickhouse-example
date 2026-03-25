"""
Dimension table endpoints.

These are simple look-up endpoints that return the full contents of each
dimension table as a list of records (via pandas DataFrame).
"""
from fastapi import APIRouter, Query

from app.database import query_df, df_to_records

router = APIRouter()


@router.get("/products", summary="List all products")
def list_products():
    """
    Returns all rows from **dim_product**.

    The product dimension describes *what* was sold — SKU, description, brand,
    and the category used for OLAP drill-down / roll-up operations.
    """
    df = query_df("SELECT * FROM dim_product ORDER BY product_sk")
    return df_to_records(df)


@router.get("/stores", summary="List all stores")
def list_stores():
    """
    Returns all rows from **dim_store**.

    The store dimension describes *where* each transaction took place.
    State codes (WA, CA, NY, TX) are used for geographic slicing.
    """
    df = query_df("SELECT * FROM dim_store ORDER BY store_sk")
    return df_to_records(df)


@router.get("/customers", summary="List all customers")
def list_customers():
    """
    Returns all rows from **dim_customer**.

    Only loyalty-card holders appear here.  Anonymous purchases are stored
    in fact_sales with `customer_sk = NULL`.
    """
    df = query_df("SELECT * FROM dim_customer ORDER BY customer_sk")
    return df_to_records(df)


@router.get("/promotions", summary="List all promotions")
def list_promotions():
    """
    Returns all rows from **dim_promotion**.

    `coupon_type = null` means the promotion ran without a physical coupon
    (e.g. a poster-only campaign).
    """
    df = query_df("SELECT * FROM dim_promotion ORDER BY promotion_sk")
    return df_to_records(df)


@router.get("/dates", summary="List calendar dates")
def list_dates(limit: int = Query(default=36, ge=1, le=366)):
    """
    Returns rows from **dim_date**, ordered chronologically.

    The `date_key` is a 6-digit YYMMDD integer (e.g. 140102 = 2 Jan 2014).
    Storing date attributes (year, month, weekday, is_holiday) separately
    avoids runtime date-parsing during aggregation — a core dimensional
    modelling technique.
    """
    df = query_df(
        "SELECT * FROM dim_date ORDER BY date_key LIMIT {limit:UInt32}",
        parameters={"limit": limit},
    )
    return df_to_records(df)


@router.get("/sales/raw", summary="Raw fact_sales rows (paginated)")
def list_raw_sales(limit: int = Query(default=20, ge=1, le=500)):
    """
    Returns raw rows from **fact_sales** joined with all dimension labels —
    useful for inspecting the data without running a full OLAP query.

    `promotion_name` and `customer_name` are `null` when the FK is absent.
    """
    sql = """
    SELECT
        dd.date_key,
        dd.year,
        dd.month,
        dd.day,
        dd.weekday,
        dd.is_holiday,
        dp.sku,
        dp.description  AS product,
        dp.category,
        dp.brand,
        ds.city,
        ds.state,
        dpr.name        AS promotion_name,
        dc.name         AS customer_name,
        fs.quantity,
        fs.net_price,
        fs.discount_price,
        round(fs.discount_price * fs.quantity, 2) AS line_total
    FROM fact_sales fs
    JOIN dim_date      dd  ON fs.date_key     = dd.date_key
    JOIN dim_product   dp  ON fs.product_sk   = dp.product_sk
    JOIN dim_store     ds  ON fs.store_sk     = ds.store_sk
    LEFT JOIN dim_promotion dpr ON fs.promotion_sk = dpr.promotion_sk
    LEFT JOIN dim_customer  dc  ON fs.customer_sk  = dc.customer_sk
    ORDER BY fs.date_key, line_total DESC
    LIMIT {limit:UInt32}
    """
    df = query_df(sql, parameters={"limit": limit})
    return df_to_records(df)
