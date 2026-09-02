#!/usr/bin/env python3
"""
Apply the paper-trade database schema and verify the required tables exist.

Usage:
    DATABASE_URL=postgresql://... python setup_database.py
"""
import os
import sys
from pathlib import Path

import psycopg2

REQUIRED_TABLES = {
    "options_orders",
    "instrument_keys",
    "upstox_accounts",
    "paper_trades",
}


def main():
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        return 1

    schema_path = Path(__file__).parent / "sql" / "paper_trade_schema.sql"
    if not schema_path.exists():
        print(f"ERROR: schema file not found: {schema_path}")
        return 1

    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(schema_path.read_text())
        print("Schema applied successfully.")
    finally:
        conn.close()

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            )
            existing = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    missing = REQUIRED_TABLES - existing
    print(f"Required tables present: {len(REQUIRED_TABLES) - len(missing)}/{len(REQUIRED_TABLES)}")
    if missing:
        print(f"MISSING: {sorted(missing)}")
        return 1

    print("All required tables verified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
