#!/usr/bin/env python3
"""
Fetch NFO instruments into the instrument_keys table using the same universe
logic as the original option_chain._process_option_chain_data():
for each FNO stock, select the 3 strikes at or below spot (ATM) and the 3
strikes at or above spot, then keep the CE + PE for those selected strikes
(6 CE + 6 PE per stock approx).

Spot proxy: the previous close of the corresponding NSE_EQ equity (falling back
to the FUTSTK prev_close when the equity row is absent).
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
NUM_STRIKES = 3


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


def build_spot_map(df):
    spot = {}
    eq = df[df["exchange"] == "NSE_EQ"].copy()
    for _, row in eq.iterrows():
        ts = str(row.get("tradingsymbol", "")).strip()
        bs = base_symbol(ts)
        if not bs:
            continue
        pc = row.get("last_price")
        pc = float(pc) if pc is not None and not pd.isna(pc) else None
        if pc and bs not in spot:
            spot[bs] = pc
    fut = df[(df["exchange"] == "NSE_FO") & (df["instrument_type"] == "FUTSTK")].copy()
    for _, row in fut.iterrows():
        ts = str(row.get("tradingsymbol", "")).strip()
        bs = base_symbol(ts)
        if not bs or bs in spot:
            continue
        pc = row.get("last_price")
        pc = float(pc) if pc is not None and not pd.isna(pc) else None
        if pc:
            spot[bs] = pc
    return spot


def fetch_universe(df, configured):
    target_month = f"{configured.year:04d}-{configured.month:02d}"
    fo = df[df["exchange"] == "NSE_FO"].copy()
    fo["_expiry_dt"] = pd.to_datetime(fo["expiry"], errors="coerce")
    fo["_expiry_month"] = fo["_expiry_dt"].dt.strftime("%Y-%m")
    month_rows = fo[fo["_expiry_month"] == target_month].copy()
    if month_rows.empty:
        print(f"NSE_FO instruments in {target_month}: 0")
        return pd.DataFrame()

    stock_opts = month_rows[month_rows["instrument_type"] == "OPTSTK"].copy()
    print(f"NSE_FO OPTSTK in {target_month}: {len(stock_opts)}")
    if stock_opts.empty:
        return pd.DataFrame()

    spot_map = build_spot_map(df)
    print(f"Spot prices available for {len(spot_map)} stocks")

    selected = []
    stocks_count = 0
    for bs, grp in stock_opts.groupby(stock_opts["tradingsymbol"].map(base_symbol)):
        if not bs:
            continue
        spot = spot_map.get(bs)
        if not spot:
            print(f"Skipping {bs}: no spot price")
            continue
        grp = grp.copy()
        grp["_strike"] = grp["strike"].astype(float)
        strikes = sorted(grp["_strike"].unique())
        if not strikes:
            print(f"Skipping {bs}: no strikes")
            continue
        below = [s for s in strikes if s <= spot][-NUM_STRIKES:]
        above = [s for s in strikes if s >= spot][:NUM_STRIKES]
        closest_strikes = sorted(set(below + above))
        if len(closest_strikes) < min(NUM_STRIKES * 2, len(strikes)):
            ordered = sorted(strikes, key=lambda s: abs(s - spot))
            closest_strikes = sorted(ordered[:NUM_STRIKES * 2])
        sel = grp[grp["_strike"].isin(closest_strikes)]
        if not sel.empty:
            selected.append(sel)
            stocks_count += 1

    if not selected:
        print("No stocks selected")
        return pd.DataFrame()

    result = pd.concat(selected)
    print(f"Selected {len(result)} OPTSTK option rows across {stocks_count} stocks "
          f"({NUM_STRIKES} CE + {NUM_STRIKES} PE ATM strikes each)")
    return result


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
            ot = row.get("option_type")
            option_type = (str(ot).strip().upper() or None) if ot is not None and not pd.isna(ot) else None
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


def clear_instrument_keys():
    try:
        db.clear_old_data()
        print("Cleared existing instrument_keys")
    except Exception as e:
        print(f"Error clearing instrument_keys: {e}")


def main():
    configured = get_configured_expiry()
    print(f"Configured expiry: {configured.date()} (month {configured.year:04d}-{configured.month:02d})")
    df = download_instruments()
    rows = fetch_universe(df, configured)
    if rows.empty:
        print("No option instruments selected for configured expiry month")
        sys.exit(1)
    clear_instrument_keys()
    total = upsert_instruments(rows)
    print(f"Total instrument rows upserted: {total}")
    print("Instrument fetch complete")


if __name__ == "__main__":
    main()
