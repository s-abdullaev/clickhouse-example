"""
ClickHouse connection helpers.

We use clickhouse-connect (official Python client, HTTP transport).
Each request gets a fresh client — ClickHouse's HTTP interface is stateless
so there is no session state to preserve between calls.

pandas DataFrames are the primary result format:
    - query_df()   → returns a pandas DataFrame
    - query_rows() → returns list[dict] (useful for simple look-ups)
"""
import pandas as pd
import clickhouse_connect
from clickhouse_connect.driver.client import Client

from app.config import settings


def get_client() -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )


def query_df(sql: str, parameters: dict | None = None) -> pd.DataFrame:
    """
    Execute a SELECT and return a pandas DataFrame.

    clickhouse-connect's query_df() uses the Arrow columnar format under the
    hood — much faster than row-by-row conversion for large result sets.
    """
    client = get_client()
    return client.query_df(sql, parameters=parameters)


def query_rows(sql: str, parameters: dict | None = None) -> list[dict]:
    """
    Execute a SELECT and return a list of dicts.
    Convenient for small look-up queries where DataFrame overhead is unnecessary.
    """
    client = get_client()
    result = client.query(sql, parameters=parameters)
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def df_to_records(df: pd.DataFrame) -> list[dict]:
    """
    Convert a DataFrame to JSON-serialisable list[dict].

    Handles pandas NA / NaT / numpy types that FastAPI's default JSON encoder
    cannot serialise on its own.
    """
    return df.where(pd.notna(df), other=None).to_dict(orient="records")
