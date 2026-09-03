#!/usr/bin/env python3
"""
Fetch NFO instruments (NIFTY + BANKNIFTY options/futures) from the Upstox BOD
master-contract CSV file and populate the instrument_keys table.

Designed to run daily before 8:50 AM IST alongside the token cron.
"""

import sys
import gzip
import io
import requests
import pandas as pd
from datetime import datetime
from services.database import DatabaseService

db = DatabaseService()

URL_NSE = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
SYMBOLS = ["NIFTY", "BANKNIFTY"]
EXCHANGE = "NSE_FO"


def download_instruments():
    print(f"Downloading instruments from {URL_NSE}")
    resp = requests.get(URL_NSE, timeout=120)
    resp.raise_for_status()
    with gzip.open(io.BytesIO(resp.content), "rt") as f:
        df = pd.read_csv(f)
    print(f"Downloaded {len(df)} total instrument rows")
    return df


def normalize_symbol(val):
    return str(val or "").strip().upper()


def filter_instruments(df):
    # Keep only NSE_FO instruments whose symbol/name/tradingsymbol is NIFTY or BANKNIFTY
    mask = df["exchange"].astype(str).str.upper() == EXCHANGE
    df_fo = df[mask].copy()
    if df_fo.empty:
        return df_fo

    def is_target(row):
        vals = {normalize_symbol(row.get("symbol")),
                normalize_symbol(row.get("name")),
                normalize_symbol(row.get("tradingsymbol"))}
        for s in SYMBOLS:
            if s in vals:
                return True
        return False

    keep = df_fo.apply(is_target, axis=1)
    return df_fo[keep]


def parse_expiry(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return None


def upsert_instruments(df):
    required = {"instrument_key", "exchange", "tradingsymbol"}
    if not required.issubset(df.columns):
        print(f"Missing columns. Found: {list(df.columns)}")
        return 0

    inserted = 0
    errors = 0
    for _, row in df.iterrows():
        try:
            instrument_key = str(row.get("instrument_key", "")).strip()
            tradingsymbol = str(row.get("tradingsymbol", "")).strip()
            if not instrument_key or not tradingsymbol:
                continue

            symbol = normalize_symbol(row.get("symbol") or row.get("name"))
            lot_size = row.get("lot_size")
            lot_size = int(lot_size) if lot_size is not None and not pd.isna(lot_size) else None
            expiry_date = parse_expiry(row.get("expiry"))
            strike = row.get("strike")
            strike_price = float(strike) if strike is not None and not pd.isna(strike) else None
            option_type = str(row.get("option_type", "")).strip().upper() or None
            instrument_type = option_type if option_type else str(row.get("instrument_type", "")).strip().upper()
            prev_close = row.get("last_price")
            prev_close = float(prev_close) if prev_close is not None and not pd.isna(prev_close) else None

            db.upsert_instrument_key(
                symbol=symbol,
                instrument_key=instrument_key,
                exchange=EXCHANGE,
                tradingsymbol=tradingsymbol,
                lot_size=lot_size,
                instrument_type=instrument_type,
                expiry_date=expiry_date,
                strike_price=strike_price,
                option_type=option_type,
                prev_close=prev_close,
            )
            inserted += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"Error inserting {row.get('tradingsymbol', '?')}: {e}")

    print(f"Upserted {inserted} instruments, {errors} errors")
    return inserted


def main():
    df = download_instruments()
    filtered = filter_instruments(df)
    if filtered.empty:
        print("No matching NIFTY/BANKNIFTY instruments found")
        sys.exit(1)
    print(f"Filtered to {len(filtered)} NIFTY/BANKNIFTY instruments")
    upsert_instruments(filtered)
    print("Instrument fetch complete")


if __name__ == "__main__":
    main()
