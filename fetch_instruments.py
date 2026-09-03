#!/usr/bin/env python3
"""
Fetch NFO instruments (stock options, index options, stock/index futures, and
FNO equities) from the Upstox BOD master-contract CSV and populate the
instrument_keys table.

Ports the coverage of the original option_chain.fetch_instrument_keys() (app.py):
  - OPTSTK (stock options), OPTIDX (index options)
  - FUTSTK (stock futures), FUTIDX (index futures)
  - NSE_EQ equities for all F&O stocks
for the configured expiry month (config.INSTRUMENT_EXPIRIES / EXPIRY_DATE).

Designed to run daily before the market opens alongside the token cron.
"""

import sys
import re
import gzip
import io
import requests
import pandas as pd
from datetime import datetime
from config import Config
from services.database import DatabaseService

db = DatabaseService()

URL_COMPLETE = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
FUT_TYPES = {"FUTSTK", "FUTIDX"}
OPT_TYPES = {"OPTSTK", "OPTIDX"}


def get_configured_expiry():
    if Config.INSTRUMENT_EXPIRIES:
        first = Config.INSTRUMENT_EXPIRIES[0]
    else:
        first = Config.EXPIRY_DATE
    return datetime.strptime(first, "%Y-%m-%d")


def download_instruments():
    print(f"Downloading instruments from {URL_COMPLETE}")
    resp = requests.get(URL_COMPLETE, timeout=120)
    resp.raise_for_status()
    with gzip.open(io.BytesIO(resp.content), "rt") as f:
        df = pd.read_csv(f)
    print(f"Downloaded {len(df)} total instrument rows")
    return df


def base_symbol(tradingsymbol):
    m = re.match(r"[A-Z]+", str(tradingsymbol))
    return m.group(0) if m else None


def fetch_for_expiry(df, configured):
    target_month = f"{configured.year:04d}-{configured.month:02d}"
    fo = df[df["exchange"] == "NSE_FO"].copy()
    fo["_expiry_dt"] = pd.to_datetime(fo["expiry"], errors="coerce")
    fo["_expiry_month"] = fo["_expiry_dt"].dt.strftime("%Y-%m")
    month_rows = fo[fo["_expiry_month"] == target_month]
    print(f"NSE_FO instruments in {target_month}: {len(month_rows)}")

    if month_rows.empty:
        return pd.DataFrame()

    # FNO stock set = base symbols of all stock futures/options for the expiry month
    stock_symbols = set()
    for ts in month_rows["tradingsymbol"]:
        bs = base_symbol(ts)
        if bs:
            stock_symbols.add(bs)
    print(f"Derived {len(stock_symbols)} F&O symbols (stocks + indices)")

    equity_df = pd.DataFrame()
    eq_all = df[(df["exchange"] == "NSE_EQ")].copy()
    eq_all["_sym"] = eq_all["tradingsymbol"].astype(str)
    equity_df = eq_all[eq_all["_sym"].isin(stock_symbols)]
    print(f"NSE_EQ equities for F&O stocks: {len(equity_df)}")

    return month_rows, equity_df, stock_symbols


def upsert_instruments(rows):
    inserted = 0
    errors = 0
    for _, row in rows.iterrows():
        try:
            instrument_key = str(row.get("instrument_key", "")).strip()
            tradingsymbol = str(row.get("tradingsymbol", "")).strip()
            if not instrument_key or not tradingsymbol:
                continue

            symbol = base_symbol(tradingsymbol) or tradingsymbol
            lot_size = row.get("lot_size")
            lot_size = int(lot_size) if lot_size is not None and not pd.isna(lot_size) else None
            expiry_date = None
            ed = row.get("expiry")
            if ed is not None and not pd.isna(ed):
                expiry_date = pd.to_datetime(ed).date()
            strike = row.get("strike")
            strike_price = float(strike) if strike is not None and not pd.isna(strike) else None
            option_type = str(row.get("option_type", "")).strip().upper() or None
            instrument_type = str(row.get("instrument_type", "")).strip().upper()
            prev_close = row.get("last_price")
            prev_close = float(prev_close) if prev_close is not None and not pd.isna(prev_close) else None

            db.upsert_instrument_key(
                symbol=symbol,
                instrument_key=instrument_key,
                exchange=str(row.get("exchange", "")).strip(),
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
    configured = get_configured_expiry()
    print(f"Configured expiry: {configured.date()} (month {configured.year:04d}-{configured.month:02d})")

    df = download_instruments()
    month_rows, equity_df, _stock_symbols = fetch_for_expiry(df, configured)

    if month_rows.empty:
        print("No NSE_FO instruments found for configured expiry month")
        sys.exit(1)

    total = 0
    total += upsert_instruments(month_rows)
    if not equity_df.empty:
        total += upsert_instruments(equity_df)

    print(f"Total instrument rows upserted: {total}")
    print("Instrument fetch complete")


if __name__ == "__main__":
    main()
