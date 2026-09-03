"""
Standalone Recovery Trade Paper Trader
=======================================
Monitors the `options_orders` table (populated by the Upstox WebSocket feed)
for recovery trades, and paper-trades them in real time during market hours.

CONCEPT
-------
A "recovery trade" is any options order in `options_orders` that has a valid
stored LTP and whose LIVE price has recovered to the trade thresholds. The
paper trader computes recovery itself from stored vs live LTP (via Upstox REST
/market-quote/ltp) -- it does NOT depend on the worker or its recovery flags.

PAPER TRADING RULES (this file)
-------------------------------
  ENTRY : when a recovery trade hits 33% return from stored LTP
  EXIT  : book profit when return reaches 47% - 50% from stored LTP
          (first time it enters the 47-50% zone, exit at the live price)
  STOP  : exit at -90% from stored LTP (option lost 90% of value)

POSITION SIZING
---------------
  - Default quantity = 1 lot (lot_size from instrument_keys)
  - If 1 lot notional (lot_size * entry_ltp) <= 20,000 -> use 1 lot
  - If 1 lot notional > 20,000 -> still buy 1 lot (never restrict single lot)
  - If 1 lot notional is small, can add lots to reach ~20,000 notional, but
    NEVER exceed the single-lot rule.

Deploy as its own Railway worker:
    python paper_trader.py

Env vars:
    DATABASE_URL  (required)   Railway PostgreSQL connection string
    PORT          (optional)   unused, service runs headless
    POLL_INTERVAL_S (optional) seconds between checks (default 30)
"""

import os
import time
import json
from datetime import datetime, timedelta

import psycopg2
import requests
import pytz

try:
    from contextlib import contextmanager
except ImportError:  # pragma: no cover
    from contextlib import contextmanager

# =============================================================================
# CONFIGURATION
# =============================================================================
DATABASE_URL = os.getenv('DATABASE_URL', '').strip()

# MARKET HOURS (IST)
MARKET_OPEN = (9, 15)   # 09:15 AM
MARKET_CLOSE = (15, 30) # 03:30 PM

# POLL INTERVAL (seconds)
POLL_INTERVAL_S = int(os.getenv('POLL_INTERVAL_S', '30'))

# PAPER TRADING THRESHOLDS (percent from stored LTP)
ENTRY_PCT = 33.0
EXIT_PCT_MIN = 47.0
EXIT_PCT_MAX = 50.0
STOP_LOSS_PCT = -90.0

# POSITION SIZING
TARGET_NOTIONAL = 20000.0   # ~₹20,000 approx total amount for multiple lots
MAX_LOTS = 1                # only scale lots to reach target notional, never more than 1

# UPSTOX API (for live price fallback & instrument keys)
BASE_URL_V3 = "https://api.upstox.com/v3"
MAX_KEYS_PER_CALL = 500

IST = pytz.timezone('Asia/Kolkata')


# =============================================================================
# DATABASE HELPERS
# =============================================================================
@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def _fetch_one(query, params=None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            row = cur.fetchone()
            return row


def _fetch_all(query, params=None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()


def _execute(query, params=None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())


def _fetch_all_dicts(query, params=None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


# =============================================================================
# MARKET HOURS
# =============================================================================
def is_market_open(now=None):
    """Mon-Fri, 09:15 - 15:30 IST"""
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return False
    cur_time = (now.hour, now.minute)
    return MARKET_OPEN <= cur_time <= MARKET_CLOSE


def wait_until_market_open():
    """Sleep until next market open."""
    now = datetime.now(IST)
    while not is_market_open(now):
        # Find next open
        if now.weekday() >= 5 or (now.hour, now.minute) > MARKET_CLOSE:
            delta = 1
            while (now + timedelta(days=delta)).weekday() >= 5:
                delta += 1
            next_open = (now + timedelta(days=delta)).replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
        else:
            next_open = now.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
        wait_s = max(1, (next_open - now).total_seconds())
        print(f"[{now.strftime('%H:%M:%S')}] Market closed. Sleeping {int(wait_s/60)}m until {next_open.strftime('%Y-%m-%d %H:%M')}")
        time.sleep(min(wait_s, 300))
        now = datetime.now(IST)


# =============================================================================
# UPSTOX LIVE PRICE FETCH
# =============================================================================
def get_access_token():
    """Fetch a valid access token from upstox_accounts table.
    Paper trader is allocated account 6 (feed uses 1-4, worker uses 5)."""
    try:
        row = _fetch_one("""
            SELECT access_token FROM upstox_accounts
            WHERE id = 6 AND access_token IS NOT NULL AND access_token != ''
            ORDER BY id
            LIMIT 1
        """)
        return row[0] if row else None
    except Exception as e:
        print(f"[ERROR] get_access_token: {e}")
        return None


def fetch_live_ltp(instrument_keys):
    """
    Fetch live last traded price for instrument keys via Upstox /ltp.
    instrument_keys: list of 'NSE_FO|token' style keys.
    Returns {instrument_key: ltp}.
    """
    if not instrument_keys:
        return {}
    # refresh token if missing
    token = get_access_token()
    if not token:
        print("[WARN] No access token available for live prices")
        return {}

    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
    ltp_map = {}
    for i in range(0, len(instrument_keys), MAX_KEYS_PER_CALL):
        chunk = instrument_keys[i:i + MAX_KEYS_PER_CALL]
        try:
            params = {'instrument_key': ",".join(chunk)}
            resp = requests.get(f"{BASE_URL_V3}/market-quote/ltp", headers=headers, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('status') == 'success':
                    for key, val in data['data'].items():
                        ltp_map[key] = float(val.get('last_price', 0))
        except Exception as e:
            print(f"[ERROR] fetch_live_ltp chunk: {e}")
            time.sleep(1)
    return ltp_map


# =============================================================================
# RECOVERY TRADE CANDIDATES
# =============================================================================
def fetch_recovery_candidates():
    """
    Fetch all options_orders that have a valid stored LTP and are not Done.
    The paper trader evaluates their LIVE percent_change from stored LTP
    to decide entry (>=33%), exit (47-50%), or stop-loss (-90%).
    A recovery trade is any order whose live price recovered to the thresholds.
    """
    try:
        rows = _fetch_all_dicts("""
            SELECT
                o.symbol,
                o.strike_price::float AS strike_price,
                o.option_type,
                o.ltp::float                AS stored_ltp,
                o.lot_size,
                o.status,
                o.is_hit,
                o.is_less_than_25pct,
                o.is_less_than_50pct,
                i.instrument_key
            FROM options_orders o
            LEFT JOIN instrument_keys i
                ON o.symbol = i.symbol
               AND o.strike_price = i.strike_price
               AND o.option_type = i.option_type
            WHERE o.status != 'Done'
              AND o.ltp IS NOT NULL
              AND o.ltp > 0
            ORDER BY o.timestamp DESC
        """)
        return rows
    except Exception as e:
        print(f"[ERROR] fetch_recovery_candidates: {e}")
        return []


def fetch_open_paper_trades():
    """Fetch all OPEN paper trades."""
    try:
        return _fetch_all_dicts("""
            SELECT * FROM paper_trades WHERE status = 'OPEN' ORDER BY entry_time
        """)
    except Exception as e:
        print(f"[ERROR] fetch_open_paper_trades: {e}")
        return []


def fetch_existing_open_keys():
    """Fetch set of (symbol|strike|option_type|stored_ltp) for OPEN paper trades."""
    try:
        rows = _fetch_all("""
            SELECT source_symbol, source_strike_price, source_option_type, source_stored_ltp
            FROM paper_trades WHERE status = 'OPEN'
        """)
        return {(r[0], float(r[1]), r[2], float(r[3])) for r in rows}
    except Exception as e:
        print(f"[ERROR] fetch_existing_open_keys: {e}")
        return set()


# =============================================================================
# POSITION SIZING
# =============================================================================
def compute_quantity(entry_ltp, lot_size, instrument_key=None):
    """
    Determine paper quantity:
      - default lot_size 1 if unknown
      - single lot notional = lot_size * entry_ltp
      - if single lot notional > TARGET_NOTIONAL: buy exactly 1 lot (never restrict)
      - else: lots = floor(TARGET_NOTIONAL / notional), capped at MAX_LOTS
    Returns (quantity, lot_size_to_use, lots_used)
    """
    lot_size = int(lot_size) if lot_size else 1
    notional_per_lot = lot_size * entry_ltp

    if lot_size <= 0:
        lot_size = 1
        notional_per_lot = entry_ltp

    # Single lot alone exceeds target -> always buy 1 lot, no restriction
    if notional_per_lot > TARGET_NOTIONAL:
        lots = 1
    else:
        # Scale to reach ~20k, but never exceed MAX_LOTS
        max_lots_by_notional = int(TARGET_NOTIONAL // notional_per_lot) if notional_per_lot > 0 else 1
        max_lots_by_notional = max(1, max_lots_by_notional)
        lots = min(max_lots_by_notional, MAX_LOTS)

    quantity = lots * lot_size
    return quantity, lot_size, lots


# =============================================================================
# PAPER TRADE ACTIONS
# =============================================================================
def insert_paper_trade(candidate, entry_ltp):
    """Create a new OPEN paper trade when recovery >= 33%."""
    symbol = candidate['symbol']
    strike = candidate['strike_price']
    opt = candidate['option_type']
    stored_ltp = candidate['stored_ltp']

    # Prevent duplicates for OPEN trades
    existing = _fetch_one("""
        SELECT id FROM paper_trades
        WHERE source_symbol=%s AND source_strike_price=%s AND source_option_type=%s
          AND source_stored_ltp=%s
    """, (symbol, strike, opt, stored_ltp))
    if existing:
        return False

    lot_size = candidate.get('lot_size') or 1
    quantity, lot_size_use, lots = compute_quantity(entry_ltp, lot_size, candidate.get('instrument_key'))
    notional = quantity * entry_ltp
    entry_pct = ((entry_ltp - stored_ltp) / stored_ltp * 100) if stored_ltp else 0.0

    try:
        _execute("""
            INSERT INTO paper_trades (
                source_symbol, source_strike_price, source_option_type, source_stored_ltp,
                lot_size, quantity, notional_value,
                entry_ltp, entry_time, entry_pct_change,
                status, entry_threshold, exit_threshold_min, exit_threshold_max, stop_loss_threshold
            ) VALUES (%s,%s,%s,%s, %s,%s,%s, %s,NOW(),%s, 'OPEN', %s,%s,%s,%s)
        """, (
            symbol, strike, opt, stored_ltp,
            lot_size_use, quantity, notional,
            entry_ltp, entry_pct,
            ENTRY_PCT, EXIT_PCT_MIN, EXIT_PCT_MAX, STOP_LOSS_PCT
        ))
        print(f"[ENTRY] {symbol} {strike} {opt} | stored={stored_ltp:.2f} entry={entry_ltp:.2f} "
              f"({entry_pct:+.2f}%) qty={quantity} notional={notional:.2f}")
        return True
    except Exception as e:
        print(f"[ERROR] insert_paper_trade {symbol} {strike} {opt}: {e}")
        return False


def exit_paper_trade(trade_id, exit_ltp, reason, stored_ltp, current_pct):
    """Close a paper trade with final status."""
    pnl_pct = current_pct  # percent from stored LTP
    try:
        _execute("""
            UPDATE paper_trades
            SET exit_ltp=%s, exit_time=NOW(), status=%s, exit_reason=%s,
                exit_pct_change=%s, pnl_pct=%s, last_updated=NOW()
            WHERE id=%s AND status='OPEN'
        """, (exit_ltp, reason, reason, current_pct, pnl_pct, trade_id))
        print(f"[EXIT] trade_id={trade_id} reason={reason} exit={exit_ltp:.2f} ({current_pct:+.2f}%)")
        return True
    except Exception as e:
        print(f"[ERROR] exit_paper_trade {trade_id}: {e}")
        return False


def update_live_price(trade_id, current_ltp, current_pct, peak_pct):
    try:
        _execute("""
            UPDATE paper_trades
            SET current_ltp=%s, current_pct_change=%s, peak_pct=%s, last_updated=NOW()
            WHERE id=%s AND status='OPEN'
        """, (current_ltp, current_pct, peak_pct, trade_id))
    except Exception as e:
        print(f"[ERROR] update_live_price {trade_id}: {e}")


# =============================================================================
# MAIN PROCESSING LOOP
# =============================================================================
def process_once():
    """One full pass: evaluate entries and exits against live prices."""
    candidates = fetch_recovery_candidates()
    open_trades = fetch_open_paper_trades()

    if not candidates and not open_trades:
        return

    # Build instrument_key -> ltp for everything we care about (single REST call)
    # Map open trade -> stored_ltp and instrument_key needed for exit eval
    # Candidates need live price to detect >=33% entry.
    live_lookup = {}

    # For candidates: we need live price; stored_ltp + is flags we already have
    # Fetch live prices for candidate instrument keys
    cand_keys = {c['instrument_key']: c for c in candidates if c.get('instrument_key')}
    keys_to_fetch = list(cand_keys.keys())

    # Open trades may not have instrument_key stored; fetch them via symbol/strike/opt
    open_map = {}  # (symbol|strike|opt) -> trade
    for t in open_trades:
        open_map[(t['source_symbol'], float(t['source_strike_price']), t['source_option_type'])] = t

    if keys_to_fetch:
        live_lookup.update(fetch_live_ltp(keys_to_fetch))
        time.sleep(0.2)

    # ---- ENTRY CHECKS ----
    existing_open = fetch_existing_open_keys()
    for c in candidates:
        note = (c['symbol'], float(c['strike_price']), c['option_type'])
        key = (c['symbol'], float(c['strike_price']), c['option_type'], float(c['stored_ltp']))
        if key in existing_open:
            continue
        ik = c.get('instrument_key')
        current_ltp = live_lookup.get(ik) if ik else None
        if not current_ltp or current_ltp <= 0:
            continue
        stored = float(c['stored_ltp'])
        if stored <= 0:
            continue
        pct = (current_ltp - stored) / stored * 100
        if pct >= ENTRY_PCT:
            insert_paper_trade(c, current_ltp)

    # ---- EXIT CHECKS (for open paper trades) ----
    if open_trades:
        # Resolve instrument_key for every open trade in ONE query, so we only
        # make a single live-price REST call for the whole open set.
        open_key_rows = _fetch_all_dicts("""
            SELECT symbol, strike_price::float AS strike_price, option_type, instrument_key
            FROM instrument_keys
            WHERE (symbol, strike_price, option_type) IN (
                SELECT DISTINCT source_symbol, source_strike_price, source_option_type
                FROM paper_trades WHERE status = 'OPEN'
            )
        """)
        open_ik = {
            (r['symbol'], r['strike_price'], r['option_type']): r['instrument_key']
            for r in open_key_rows if r.get('instrument_key')
        }
        open_keys_to_fetch = list(open_ik.values())
        open_ltp_map = fetch_live_ltp(open_keys_to_fetch) if open_keys_to_fetch else {}

        for t in open_trades:
            sym, strike, opt = t['source_symbol'], float(t['source_strike_price']), t['source_option_type']
            ik_val = open_ik.get((sym, strike, opt))
            current_ltp = open_ltp_map.get(ik_val) if ik_val else None
            if not current_ltp or current_ltp <= 0:
                continue
            stored = float(t['source_stored_ltp'])
            if stored <= 0:
                continue
            pct = (current_ltp - stored) / stored * 100
            peak = max(float(t.get('peak_pct') or 0), pct)
            update_live_price(t['id'], current_ltp, pct, peak)

            # EXIT: profit at 47-50%
            if EXIT_PCT_MIN <= pct <= EXIT_PCT_MAX:
                exit_paper_trade(t['id'], current_ltp, 'PROFIT', stored, pct)
            # EXIT: stop loss at -90%
            elif pct <= STOP_LOSS_PCT:
                exit_paper_trade(t['id'], current_ltp, 'LOSS', stored, pct)
            # EXIT: if it spiked past 50% (caught on subsequent poll)
            elif pct > EXIT_PCT_MAX:
                exit_paper_trade(t['id'], current_ltp, 'PROFIT', stored, pct)


def main():
    print("=" * 60)
    print("RECOVERY TRADE PAPER TRADER")
    print(f"  ENTRY: >= {ENTRY_PCT}% | EXIT: {EXIT_PCT_MIN}-{EXIT_PCT_MAX}% | SL: {STOP_LOSS_PCT}%")
    print(f"  Poll interval: {POLL_INTERVAL_S}s | Target notional: ~{TARGET_NOTIONAL:,.0f}")
    print("=" * 60)

    if not DATABASE_URL:
        print("[FATAL] DATABASE_URL environment variable not set.")
        return

    # Verify connection
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        print("[OK] Database connection established")
    except Exception as e:
        print(f"[FATAL] Cannot connect to database: {e}")
        return

    while True:
        try:
            if not is_market_open():
                wait_until_market_open()
            else:
                now = datetime.now(IST)
                print(f"[{now.strftime('%H:%M:%S')}] Processing recovery paper trades...")
                process_once()
                time.sleep(POLL_INTERVAL_S)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as e:
            print(f"[ERROR] main loop: {e}")
            time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    main()
