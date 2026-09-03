#!/usr/bin/env python3
"""
Fetch NFO instruments (NIFTY + BANKNIFTY options/futures) from Upstox API
and populate the instrument_keys table.

Designed to run daily before 8:50 AM IST alongside the token cron.
"""

import sys
import requests
from datetime import datetime
from config import Config
from services.database import DatabaseService

db = DatabaseService()

INSTRUMENTS_URL = "https://api.upstox.com/v2/market/instruments"
# NIFTY and BANKNIFTY symbols on NFO
SYMBOLS = ["NIFTY", "BANKNIFTY"]
EXCHANGE = "NSE_FO"


def get_access_token():
    accounts = db.get_upstox_accounts()
    for acc in accounts:
        t = acc.get("access_token")
        if t:
            return t
    return None


def fetch_instruments(access_token):
    """Fetch all NFO instruments from Upstox."""
    headers = {"Authorization": f"Bearer {access_token}"}
    print("Fetching instruments from Upstox API...")
    resp = requests.get(INSTRUMENTS_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    instruments = data.get("data", [])
    print(f"Received {len(instruments)} total instruments")
    return instruments


def filter_instruments(instruments):
    """Keep only NIFTY + BANKNIFTY options and futures on NFO."""
    filtered = []
    for inst in instruments:
        symbol = inst.get("symbol", "")
        exchange = inst.get("exchange", "")
        segment = inst.get("exchange_segment", "")
        name = inst.get("name", "")
        # Match on symbol or name containing NIFTY/BANKNIFTY, on NFO
        if segment != EXCHANGE and exchange != EXCHANGE:
            continue
        if symbol not in SYMBOLS and name not in SYMBOLS:
            continue
        filtered.append(inst)
    print(f"Filtered to {len(filtered)} NIFTY/BANKNIFTY instruments")
    return filtered


def parse_expiry(expiry_str):
    """Parse expiry string to date object."""
    if not expiry_str:
        return None
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(expiry_str, fmt).date()
        except ValueError:
            continue
    return None


def upsert_instruments(filtered):
    """Insert or update instrument_keys table."""
    inserted = 0
    updated = 0
    errors = 0
    for inst in filtered:
        try:
            instrument_key = f"{EXCHANGE}|{inst.get('instrument_token', inst.get('tradingsymbol', ''))}"
            symbol = inst.get("symbol", inst.get("name", ""))
            tradingsymbol = inst.get("tradingsymbol", "")
            lot_size = inst.get("lot_size", inst.get("minimum_lot", 1))
            expiry_date = parse_expiry(inst.get("expiry"))
            strike_price = inst.get("strike_price")
            option_type = inst.get("option_type", "").upper() if inst.get("option_type") else None
            instrument_type = option_type if option_type else ("FUT" if not strike_price else "OPT")
            prev_close = inst.get("close", 0) or 0

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
                print(f"Error inserting {inst.get('tradingsymbol', '?')}: {e}")

    print(f"Upserted {inserted} instruments, {errors} errors")
    return inserted


def main():
    access_token = get_access_token()
    if not access_token:
        print("No access token in DB — cannot fetch instruments")
        sys.exit(1)

    print(f"Using access token ending with ...{access_token[-6:]}")
    instruments = fetch_instruments(access_token)
    filtered = filter_instruments(instruments)

    if not filtered:
        print("No matching instruments found")
        sys.exit(1)

    upsert_instruments(filtered)
    print("Instrument fetch complete")


if __name__ == "__main__":
    main()
