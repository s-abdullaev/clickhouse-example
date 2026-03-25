#!/usr/bin/env python3
"""
ClickHouse migration runner.

Reads numbered .sql files from the migrations/ directory, executes each
unapplied migration in order, and records completion in a tracking table.

Usage:
    uv run python scripts/migrate.py
"""
import os
import sys
from pathlib import Path

import clickhouse_connect

# ---------------------------------------------------------------------------
# Configuration — override via environment variables
# ---------------------------------------------------------------------------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
DATABASE = "retail_dw"

MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"

TRACKING_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {DATABASE}._migrations
(
    name    String,
    applied DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY name
"""


def get_client():
    """Connect to ClickHouse (default database — used before retail_dw exists)."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def split_statements(sql: str) -> list[str]:
    """
    Split a SQL file into individual statements on semicolons.
    Filters out empty strings and comment-only blocks.
    """
    statements = []
    for raw in sql.split(";"):
        stmt = raw.strip()
        if not stmt:
            continue
        # Skip blocks that contain only comments / blank lines
        content_lines = [
            line for line in stmt.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        if content_lines:
            statements.append(stmt)
    return statements


def run_migration(client, filepath: Path) -> None:
    sql = filepath.read_text(encoding="utf-8")
    statements = split_statements(sql)
    for i, stmt in enumerate(statements, 1):
        try:
            client.command(stmt)
        except Exception as exc:
            preview = stmt[:300].replace("\n", " ")
            print(f"  ✗ Statement {i} failed: {exc}")
            print(f"    SQL preview: {preview}")
            raise


def get_applied(client) -> set[str]:
    result = client.query(f"SELECT name FROM {DATABASE}._migrations")
    return {row[0] for row in result.result_rows}


def record_migration(client, name: str) -> None:
    client.command(
        f"INSERT INTO {DATABASE}._migrations (name) VALUES ('{name}')"
    )


def main() -> None:
    print("=" * 50)
    print("  ClickHouse Migration Runner")
    print("=" * 50)

    print(f"\nConnecting to {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} ...")
    try:
        client = get_client()
        client.ping()
    except Exception as exc:
        print(f"  ✗ Connection failed: {exc}")
        print("  Is ClickHouse running?  Try: docker-compose up -d")
        sys.exit(1)
    print("  ✓ Connected")

    # Ensure the target database exists (migration 001 also creates it,
    # but we need it before we can create the tracking table).
    client.command(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    print(f"  ✓ Database '{DATABASE}' ready")

    client.command(TRACKING_TABLE_DDL)
    print("  ✓ Migrations tracking table ready")

    applied = get_applied(client)
    print(f"\n  Already applied: {len(applied)} migration(s)")

    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        print("  No .sql files found in migrations/")
        return

    pending = [f for f in migration_files if f.name not in applied]
    if not pending:
        print("  All migrations are up to date. Nothing to do.")
        return

    print(f"  Pending: {len(pending)} migration(s)\n")

    for filepath in pending:
        print(f"  Running  {filepath.name} ...", end="", flush=True)
        try:
            run_migration(client, filepath)
            record_migration(client, filepath.name)
            print("  ✓")
        except Exception:
            print("  ✗ (see error above)")
            sys.exit(1)

    print(f"\n✓ Done — applied {len(pending)} migration(s) successfully.")


if __name__ == "__main__":
    main()
