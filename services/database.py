import json
import os
import time
from functools import lru_cache

import pandas as pd
import psycopg2
from services.custom_ta import ta
from psycopg2._psycopg import OperationalError
from psycopg2.extras import execute_batch
from decimal import Decimal
from contextlib import contextmanager
import numpy as np  # Add this import

from datetime import datetime, date
import pytz
from curl_cffi import requests
import yfinance as yf
import asyncio

# Memory cache for pivot point calculation tracking

class DatabaseService:

    pivot_calculation_cache = {}

    def __init__(self, max_retries=3, retry_delay=2):
        self.conn_params = self._build_conn_params()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session(impersonate="chrome110")
        print("databse init done")

    def _build_conn_params(self):
        """Build psycopg2 connection params.

        Prefer the individual DB_* environment variables, otherwise fall back
        to parsing the DATABASE_URL (postgresql://user:pass@host:port/dbname).
        """
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        if db_name and db_user and db_password and db_host and db_port:
            return {
                'dbname': db_name,
                'user': db_user,
                'password': db_password,
                'host': db_host,
                'port': db_port,
            }
        dsn = os.getenv('DATABASE_URL', '').strip()
        if dsn:
            from urllib.parse import urlparse, unquote
            parsed = urlparse(dsn)
            port = parsed.port or 5432
            try:
                dbname = parsed.path.lstrip('/')
            except Exception:
                dbname = 'railway'
            return {
                'dbname': dbname or 'railway',
                'user': unquote(parsed.username) if parsed.username else 'postgres',
                'password': unquote(parsed.password) if parsed.password else '',
                'host': parsed.hostname or 'localhost',
                'port': str(port),
            }
        return {
            'dbname': 'your_db_name',
            'user': 'your_db_user',
            'password': 'your_db_password',
            'host': 'localhost',
            'port': '5432',
        }

    def test_connection(self):
        """Test database connection"""
        try:
            with self._get_cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone()[0] == 1
        except Exception as e:
            print(f"Database connection failed: {str(e)}")
            return False

    def _init_db_with_retry(self):
        """Initialize connection pool with retry logic"""
        for attempt in range(self.max_retries):
            try:
                self._init_db()
                return
            except OperationalError as e:
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"Failed to initialize database after {self.max_retries} attempts: {str(e)}")
                print(f"Database connection failed (attempt {attempt + 1}/{self.max_retries}), retrying...")
                time.sleep(self.retry_delay)

    def _init_db(self):
        """Initialize connection pool"""
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=20,
            dsn=os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/mydb')
        )

    def _verify_connection(self):
        """Verify that the connection pool works"""
        try:
            with self._get_cursor() as cur:
                cur.execute("SELECT 1")
                if cur.fetchone()[0] != 1:
                    raise RuntimeError("Database connection test failed")
        except Exception as e:
            self.connection_pool.closeall()
            raise RuntimeError(f"Database connection verification failed: {str(e)}")

    @contextmanager
    def _get_cursor(self):
        """Get a database cursor with automatic cleanup"""
        conn = None
        for attempt in range(self.max_retries):
            try:
                conn = psycopg2.connect(**self.conn_params)
                with conn.cursor() as cur:
                    yield cur
                conn.commit()
                break
            except OperationalError as e:
                if conn:
                    conn.close()
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"Database connection failed after {self.max_retries} attempts: {str(e)}")
                print(f"Database connection failed (attempt {attempt + 1}/{self.max_retries}), retrying...")
                time.sleep(self.retry_delay)
            except Exception as e:
                if conn:
                    conn.rollback()
                raise
            finally:
                if conn:
                    conn.close()

    def get_instrument_details_by_key(self, instrument_key):
        """Get instrument details by instrument key."""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT symbol, instrument_key, exchange, tradingsymbol, lot_size,
                       instrument_type, expiry_date, strike_price, option_type
                FROM instrument_keys
                WHERE instrument_key = %s
            """, (instrument_key,))
            result = cur.fetchone()
            if result:
                return {
                    'symbol': result[0],
                    'instrument_key': result[1],
                    'exchange': result[2],
                    'tradingsymbol': result[3],
                    'lot_size': result[4],
                    'instrument_type': result[5],
                    'expiry_date': result[6],
                    'strike_price': result[7],
                    'option_type': result[8]
                }
            return None

    def get_instruments_by_keys(self, instrument_keys):
        """Get multiple instrument details by instrument keys."""
        if not instrument_keys:
            return []

        with self._get_cursor() as cur:
            placeholders = ','.join(['%s'] * len(instrument_keys))
            query = f"""
                SELECT symbol, instrument_key, exchange, tradingsymbol, lot_size,
                       instrument_type, expiry_date, strike_price, option_type
                FROM instrument_keys
                WHERE instrument_key IN ({placeholders})
            """
            cur.execute(query, instrument_keys)
            results = cur.fetchall()

            return [
                {
                    'symbol': row[0],
                    'instrument_key': row[1],
                    'exchange': row[2],
                    'tradingsymbol': row[3],
                    'lot_size': row[4],
                    'instrument_type': row[5],
                    'expiry_date': row[6].isoformat() if row[6] else None,
                    'strike_price': float(row[7]) if row[7] else None,
                    'option_type': row[8]
                }
                for row in results
            ]

    async def get_instrument_keys_async(self, symbol=None, instrument_type=None, limit=6000):
        """Get instrument keys from database (async version)"""
        loop = asyncio.get_event_loop()

        def _run_query():
            with self._get_cursor() as cur:
                query = """
                    SELECT symbol, instrument_key, exchange, tradingsymbol, lot_size, 
                           instrument_type, expiry_date, strike_price, option_type, prev_close
                    FROM instrument_keys
                    WHERE exchange IN ('NSE_EQ', 'NSE_INDEX', 'BSE_INDEX', 'NSE_FO', 'BSE_FO')
                """
                params = []
                conditions = []

                if symbol:
                    conditions.append("symbol = %s")
                    params.append(symbol)

                if instrument_type:
                    conditions.append("instrument_type = %s")
                    params.append(instrument_type)

                if conditions:
                    query += " WHERE " + " AND ".join(conditions)

                query += " ORDER BY tradingsymbol LIMIT %s"
                params.append(limit)

                cur.execute(query, params)
                results = cur.fetchall()

                return [
                    {
                        'symbol': row[0],
                        'instrument_key': row[1],
                        'exchange': row[2],
                        'tradingsymbol': row[3],
                        'lot_size': row[4],
                        'instrument_type': row[5],
                        'expiry_date': row[6].isoformat() if row[6] else None,
                        'strike_price': float(row[7]) if row[7] else None,
                        'option_type': row[8],
                        'prev_close': float(row[9]) if row[9] is not None else None
                    }
                    for row in results
                ]

        return await loop.run_in_executor(None, _run_query)

    async def get_instruments_by_keys_async(self, instrument_keys):
        """Get multiple instrument details by instrument keys (async version)."""
        if not instrument_keys:
            return []

        loop = asyncio.get_event_loop()

        def _run_query():
            with self._get_cursor() as cur:
                placeholders = ','.join(['%s'] * len(instrument_keys))
                query = f"""
                    SELECT symbol, instrument_key, exchange, tradingsymbol, lot_size,
                           instrument_type, expiry_date, strike_price, option_type, prev_close
                    FROM instrument_keys
                    WHERE instrument_key IN ({placeholders})
                """
                cur.execute(query, instrument_keys)
                results = cur.fetchall()

                return [
                    {
                        'symbol': row[0],
                        'instrument_key': row[1],
                        'exchange': row[2],
                        'tradingsymbol': row[3],
                        'lot_size': row[4],
                        'instrument_type': row[5],
                        'expiry_date': row[6].isoformat() if row[6] else None,
                        'strike_price': float(row[7]) if row[7] else None,
                        'option_type': row[8],
                        'prev_close': float(row[9]) if row[9] is not None else None
                    }
                    for row in results
                ]

        return await loop.run_in_executor(None, _run_query)

    def batch_get_instrument_keys(self, symbols):
        """
        Get instrument keys for multiple trading symbols in a single database query.

        Args:
            symbols (list): List of trading symbols to look up

        Returns:
            dict: Dictionary with symbol as key and instrument details as value
        """
        if not symbols:
            return {}

        result = {}
        try:
            with self._get_cursor() as cur:
                # Format the symbols for the SQL query
                symbol_placeholders = ','.join(['%s'] * len(symbols))

                # Create a query that fetches all symbols in one go
                query = f"""
                    SELECT 
                        tradingsymbol, 
                        instrument_key, 
                        exchange, 
                        symbol as underlying, 
                        lot_size, 
                        prev_close as last_close
                    FROM 
                        instrument_keys 
                    WHERE 
                        tradingsymbol IN ({symbol_placeholders})
                """

                # Execute the query with the symbols as parameters
                cur.execute(query, symbols)
                rows = cur.fetchall()

                # Process the results
                for row in rows:
                    symbol = row[0]  # tradingsymbol
                    result[symbol] = {
                        'instrument_key': row[1],
                        'exchange': row[2],
                        'symbol': row[3],
                        'tradingsymbol': symbol,
                        'lot_size': float(row[4]) if row[4] is not None else 1,  # Convert to float
                        'last_close': float(row[5]) if row[5] is not None else 0  # Convert to float
                    }

                # For any symbols not found in the first query, try looking them up by underlying
                missing_symbols = [s for s in symbols if s not in result]
                if missing_symbols:
                    # Try looking up by underlying and exchange
                    symbol_placeholders = ','.join(['%s'] * len(missing_symbols))
                    query = f"""
                        SELECT 
                            tradingsymbol, 
                            instrument_key, 
                            exchange, 
                            symbol as underlying, 
                            lot_size, 
                            prev_close as last_close
                        FROM 
                            instrument_keys 
                        WHERE 
                            symbol IN ({symbol_placeholders})
                        AND
                            exchange = 'NSE_EQ'
                    """

                    cur.execute(query, missing_symbols)
                    rows = cur.fetchall()

                    for row in rows:
                        symbol = row[3]  # Use underlying as the key
                        if symbol in missing_symbols:
                            result[symbol] = {
                                'instrument_key': row[1],
                                'exchange': row[2],
                                'symbol': symbol,
                                'tradingsymbol': row[0],
                                'lot_size': float(row[4]) if row[4] is not None else 1,  # Convert to float
                                'last_close': float(row[5]) if row[5] is not None else 0  # Convert to float
                            }

            return result
        except Exception as e:
            print(f"Error in batch_get_instrument_keys: {e}")
            return {}

    def get_options_orders(self):
        """Get all options orders from today only"""
        try:
            with self._get_cursor() as cur:
                # Convert current datetime to UTC for comparison with database timestamps
                today_utc = datetime.now(pytz.UTC).date()

                cur.execute("""
                    SELECT symbol, strike_price, option_type, ltp, bid_qty, ask_qty, lot_size, timestamp
                    FROM options_orders
                    WHERE DATE(timestamp AT TIME ZONE 'UTC') = %s
                """, (today_utc,))

                results = cur.fetchall()
                return [{
                    'stock': r[0], 'strike_price': r[1], 'type': r[2],
                    'ltp': r[3], 'bid_qty': r[4], 'ask_qty': r[5],
                    'lot_size': r[6], 'timestamp': r[7].isoformat()
                } for r in results]
        except Exception as e:
            print(f"Error fetching options orders: {str(e)}")
            return []

    def get_full_options_orders(self):
        """Get all options orders with instrument keys"""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    WITH nearest_expiry AS (
                        SELECT 
                            symbol,
                            MIN(expiry_date) as nearest_expiry
                        FROM instrument_keys
                        WHERE symbol IN ('NIFTY', 'SENSEX')
                          AND expiry_date >= CURRENT_DATE
                          AND option_type IN ('CE', 'PE')
                        GROUP BY symbol
                    )
                    SELECT 
                        o.symbol, 
                        o.strike_price, 
                        o.option_type, 
                        o.ltp, 
                        o.bid_qty, 
                        o.ask_qty, 
                        o.lot_size, 
                        o.timestamp,
                        i.instrument_key,
                        o.oi,
                        o.volume,
                        o.vega,
                        o.theta,
                        o.gamma,
                        o.delta,
                        o.iv,
                        o.pop,
                        o.status,
                        o.is_less_than_25pct,
                        o.is_less_than_50pct,
                        o.is_greater_than_25pct,
                        o.is_greater_than_50pct,
                        o.is_greater_than_75pct,
                         o.is_less_than_75pct,
                         o.pcr,
                         o.is_hit,
                         o.hit_peak_pct,
                         o.todays_max_ltp,
                         o.todays_min_ltp,
                         i.prev_close
                     FROM options_orders o
                     LEFT JOIN instrument_keys i ON 
                         o.symbol = i.symbol AND 
                         o.strike_price = i.strike_price AND 
                         o.option_type = i.option_type
                     WHERE (o.symbol NOT IN ('NIFTY', 'SENSEX')) OR
                           (o.symbol IN ('NIFTY', 'SENSEX') AND 
                            i.expiry_date IN (SELECT nearest_expiry FROM nearest_expiry WHERE nearest_expiry.symbol = o.symbol))
                 """)
                results = cur.fetchall()
                return [{
                    'symbol': r[0],
                    'strike_price': r[1],
                    'option_type': r[2],
                    'ltp': r[3],
                    'bid_qty': r[4],
                    'ask_qty': r[5],
                    'lot_size': r[6],
                    'timestamp': r[7].isoformat(),
                    'instrument_key': r[8],
                    'oi': r[9],
                    'volume': r[10],
                    'vega': r[11],
                    'theta': r[12],
                    'gamma': r[13],
                    'delta': r[14],
                    'iv': r[15],
                    'pop': r[16],
                    'status': r[17] if r[17] else 'Open',  # Default to 'Open' if NULL
                    'is_less_than_25pct': r[18] if r[18] is not None else False,
                    'is_less_than_50pct': r[19] if r[19] is not None else False,
                    'is_greater_than_25pct': r[20] if r[20] is not None else False,
                    'is_greater_than_50pct': r[21] if r[21] is not None else False,
                    'is_greater_than_75pct': r[22] if r[22] is not None else False,
                    'is_less_than_75pct': r[23] if r[23] is not None else False,
                    'pcr': r[24] if r[24] is not None else 0.0,
                    'is_hit': r[25] if r[25] is not None else False,
                    'hit_peak_pct': float(r[26]) if r[26] is not None else 0.0,
                    'todays_max_ltp': float(r[27]) if r[27] is not None else None,
                    'todays_min_ltp': float(r[28]) if r[28] is not None else None,
                    'prev_close': float(r[29]) if r[29] is not None else None
                } for r in results]
        except Exception as e:
            print(f"Error fetching options orders: {str(e)}")
            return []

    def save_options_data(self, symbol, orders):
        """Bulk insert options orders"""

        if not orders:
            print("No orders to save")
            return
        with self._get_cursor() as cur:
            data = [(order['stock'], order['strike_price'], order['type'],
                     order['ltp'], order['bid_qty'], order['ask_qty'],
                     order['lot_size'], order['timestamp'], order['oi'], order['volume'],
                     order['vega'], order['theta'], order['gamma'], order['delta'],
                     order['iv'], order['pop'], 'Open') for order in orders]  # Set initial status to 'Open'

            execute_batch(cur, """
                INSERT INTO options_orders 
                (symbol, strike_price, option_type, ltp, bid_qty, ask_qty, lot_size, timestamp, 
                 oi, volume, vega, theta, gamma, delta, iv, pop, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, strike_price, option_type) DO NOTHING
            """, data, page_size=100)

    def save_worker_options_data(self, symbol, orders):
        """Bulk insert options orders"""
        if not orders:
            return
        with self._get_cursor() as cur:
            data = [(order['stock'], order['strike_price'], order['type'],
                     order['ltp'], order['bid_qty'], order['ask_qty'],
                     order['lot_size'], order['timestamp'], order['oi'], order['volume'],
                     order['vega'], order['theta'], order['gamma'], order['delta'],
                     order['iv'], order['pop'], 'Open', False, False, False, False, False, False, False, 0.0, None, None) for order in orders]

            execute_batch(cur, """
                INSERT INTO options_orders 
                (symbol, strike_price, option_type, ltp, bid_qty, ask_qty, lot_size, timestamp, 
                 oi, volume, vega, theta, gamma, delta, iv, pop, status, is_less_than_25pct, is_less_than_50pct, is_greater_than_25pct, is_greater_than_50pct, is_greater_than_75pct, is_less_than_75pct, is_hit, hit_peak_pct, todays_max_ltp, todays_min_ltp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, strike_price, option_type) DO NOTHING
            """, data, page_size=100)


    def update_options_orders_status(self, orders_to_update):
        """Update status for options orders"""
        if not orders_to_update:
            return

        with self._get_cursor() as cur:
            data = [(order['new_status'],
                     order.get('is_less_than_25pct', False),
                     order.get('is_less_than_50pct', False),
                     order.get('is_greater_than_25pct', False),
                     order.get('is_greater_than_50pct', False),
                     order.get('is_greater_than_75pct', False),
                     order.get('is_less_than_75pct', False),
                     order.get('is_hit', False),
                     order.get('hit_peak_pct', 0.0),
                     order.get('todays_max_ltp'),
                     order.get('todays_min_ltp'),
                     order['symbol'], order['strike_price'], order['option_type'])
                    for order in orders_to_update]

            execute_batch(cur, """
                UPDATE options_orders
                SET status = %s,
                    is_less_than_25pct = %s,
                    is_less_than_50pct = %s,
                    is_greater_than_25pct = %s,
                    is_greater_than_50pct = %s,
                    is_greater_than_75pct = %s,
                    is_less_than_75pct = %s,
                    is_hit = %s,
                    hit_peak_pct = %s,
                    todays_max_ltp = %s,
                    todays_min_ltp = %s
                WHERE symbol = %s AND strike_price = %s AND option_type = %s
            """, data, page_size=100)

    def get_futures_orders(self):
        """Get all futures orders"""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT symbol, ltp, bid_qty, ask_qty, lot_size, timestamp
                    FROM futures_orders
                """)
                results = cur.fetchall()
                return [{
                    'stock': r[0], 'ltp': r[1], 'bid_qty': r[2],
                    'ask_qty': r[3], 'lot_size': r[4], 'timestamp': r[5].isoformat()
                } for r in results]
        except Exception as e:
            print(f"Error fetching futures orders: {str(e)}")

    def get_fno_data(self, stock, expiry, strike, option_type):
        """Get F&O data for specific option"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT display_time, oi, volume, price, strike_price, option_type
                FROM oi_volume_history
                WHERE symbol = %s AND expiry_date = %s
                  AND strike_price = %s AND option_type = %s
                ORDER BY display_time
            """, (stock, expiry, strike, option_type))

            data = [{
                'time': r[0], 'oi': float(r[1]) if r[1] else 0,
                'volume': float(r[2]) if r[2] else 0, 'price': float(r[3]) if r[3] else 0,
                'strike': str(r[4]), 'optionType': r[5]
            } for r in cur.fetchall()]

            return {"data": data}

    def get_option_data(self, stock):
        """Get all option data for a stock"""
        with self._get_cursor() as cur:
            # Get expiries
            cur.execute("""
                SELECT DISTINCT expiry_date 
                FROM oi_volume_history
                WHERE symbol = %s
                ORDER BY expiry_date
            """, (stock,))
            expiries = [r[0].strftime('%Y-%m-%d') for r in cur.fetchall()]

            # Get strikes
            cur.execute("""
                SELECT DISTINCT strike_price
                FROM oi_volume_history
                WHERE symbol = %s
                ORDER BY strike_price
            """, (stock,))
            strikes = [float(r[0]) for r in cur.fetchall()]

            # Get option chain data
            cur.execute("""
                SELECT 
                    expiry_date, strike_price, option_type,
                    json_agg(
                        json_build_object(
                            'time', display_time,
                            'oi', oi,
                            'volume', volume,
                            'price', price
                        )
                    ) as history
                FROM oi_volume_history
                WHERE symbol = %s
                GROUP BY expiry_date, strike_price, option_type
            """, (stock,))

            option_data = {}
            for expiry_date, strike_price, option_type, history in cur.fetchall():
                key = f"oi_volume_data:{stock}:{expiry_date}:{strike_price}:{option_type}"
                option_data[key] = history

            return {
                "expiries": expiries,
                "strikes": strikes,
                "data": option_data
            }


    def save_futures_data(self, symbol, orders):
        """Bulk insert futures orders"""
        if not orders:
            return

        with self._get_cursor() as cur:
            data = [(order['stock'], order['ltp'], order['bid_qty'],
                     order['ask_qty'], order['lot_size'], order['timestamp']) for order in orders]

            execute_batch(cur, """
                INSERT INTO futures_orders 
                (symbol, ltp, bid_qty, ask_qty, lot_size, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING
            """, data, page_size=100)

    def save_oi_volume_batch_feed(self, records):
        """Save OI volume data with option greeks as individual columns"""
        if not records:
            return

        # Filter and process records
        filtered_records = [
            r for r in records
            if not (isinstance(r['strike'], (int, float)) and r['strike'] < 10)
        ]

        if not filtered_records:
            return

        # Convert NumPy types to native Python types and handle None values
        processed_records = []
        for r in filtered_records:
            try:
                # Safely convert values or use defaults if None
                processed_record = (
                    str(r['symbol']),
                    str(r['expiry']) if r['expiry'] is not None else None,
                    float(r['strike']) if r['strike'] is not None else 0.0,
                    str(r['option_type']) if r['option_type'] is not None else None,
                    float(r['oi']) if r['oi'] is not None else 0.0,
                    float(r['volume']) if r['volume'] is not None else 0.0,
                    float(r['price']) if r['price'] is not None else 0.0,
                    str(r['timestamp']) if r['timestamp'] is not None else None,
                    float(r['pct_change']) if r['pct_change'] is not None else 0.0,
                    float(r['vega']) if r['vega'] is not None else 0.0,
                    float(r['theta']) if r['theta'] is not None else 0.0,
                    float(r['gamma']) if r['gamma'] is not None else 0.0,
                    float(r['delta']) if r['delta'] is not None else 0.0,
                    float(r['iv']) if r['iv'] is not None else 0.0,
                    float(r['pop']) if r['pop'] is not None else 0.0
                )
                processed_records.append(processed_record)
            except (TypeError, ValueError) as e:
                print(f"Error processing record: {r}")
                print(f"Error details: {e}")
                # Skip this record to avoid breaking the batch
                continue

        # If we have no valid records after processing, exit early
        if not processed_records:
            print("No valid records to insert after processing")
            return

        try:
            with self._get_cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO oi_volume_history (
                        symbol, expiry_date, strike_price, option_type,
                        oi, volume, price, display_time, pct_change,
                        vega, theta, gamma, delta, iv, pop
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, expiry_date, strike_price, option_type, display_time) 
                    DO NOTHING
                """, processed_records, page_size=100)
                print(f"Successfully inserted {len(processed_records)} records into oi_volume_history")
        except Exception as e:
            print(f"Database error in save_oi_volume_batch: {e}")
            import traceback
            traceback.print_exc()

    def save_total_oi_data(self, records):
        """Save total call/put OI data for each symbol.

        Args:
            records: List of dictionaries containing aggregated OI data
                    with keys: symbol, display_time, call_oi, put_oi, call_put_ratio, timestamp
        """
        if not records:
            return

        try:
            with self._get_cursor() as cur:
                for record in records:
                    # Check if a record already exists for this symbol and time
                    cur.execute("""
                        SELECT call_oi, put_oi 
                        FROM total_oi_history 
                        WHERE symbol = %s AND display_time = %s
                    """, (record['symbol'], record['display_time']))

                    existing = cur.fetchone()

                    if existing:
                        # Only update if OI values have changed by at least 1%
                        # When fetching data from the database
                        existing_call_oi = float(existing[0]) if existing[0] is not None else 0
                        existing_put_oi = float(existing[1]) if existing[1] is not None else 0

                        call_oi_change_pct = abs((record['call_oi'] - existing_call_oi) / existing_call_oi * 100) if existing_call_oi > 0 else 100
                        put_oi_change_pct = abs((record['put_oi'] - existing_put_oi) / existing_put_oi * 100) if existing_put_oi > 0 else 100

                        # Update if significant change in either call or put OI
                        if call_oi_change_pct >= 1 or put_oi_change_pct >= 1:
                            cur.execute("""
                                UPDATE total_oi_history
                                SET call_oi = %s, put_oi = %s, call_put_ratio = %s, timestamp = %s, last_updated = NOW()
                                WHERE symbol = %s AND display_time = %s
                            """, (
                                record['call_oi'],
                                record['put_oi'],
                                record['call_put_ratio'],
                                record['timestamp'],
                                record['symbol'],
                                record['display_time']
                            ))
                    else:
                        # Insert new record
                        cur.execute("""
                            INSERT INTO total_oi_history
                            (symbol, display_time, call_oi, put_oi, call_put_ratio, timestamp)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            record['symbol'],
                            record['display_time'],
                            record['call_oi'],
                            record['put_oi'],
                            record['call_put_ratio'],
                            record['timestamp']
                        ))

            return True
        except Exception as e:
            print(f"Error saving total OI data: {e}")
            import traceback
            traceback.print_exc()
            return False


    def save_oi_volume_batch(self, records):
        """Save OI volume data with option greeks as individual columns"""
        if not records:
            return

        # Filter and process records
        filtered_records = [
            r for r in records
            if not (isinstance(r['strike'], (int, float)) and r['strike'] < 10)
        ]

        if not filtered_records:
            return

        # Convert NumPy types to native Python types
        processed_records = [
            (
                str(r['symbol']),
                str(r['expiry']),
                float(r['strike']),  # Convert np.float64 to float
                str(r['option_type']),
                float(r['oi']),  # Convert np.float64 to float
                float(r['volume']),  # Convert np.float64 to float
                float(r['price']),  # Convert np.float64 to float
                str(r['timestamp']),
                float(r['pct_change']),
                float(r['vega']),
                float(r['theta']),
                float(r['gamma']),
                float(r['delta']),
                float(r['iv']),
                float(r['pop'])
            )
            for r in filtered_records
        ]



        with self._get_cursor() as cur:
            execute_batch(cur, """
                INSERT INTO oi_volume_history (
                    symbol, expiry_date, strike_price, option_type,
                    oi, volume, price, display_time, pct_change,
                    vega, theta, gamma, delta, iv, pop
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, expiry_date, strike_price, option_type, display_time) 
                DO NOTHING
            """, processed_records, page_size=100)

    def clear_old_data(self):
        """Delete previous day's data"""
        print("inside clear_old_data")
        with self._get_cursor() as cur:
            print("cur", cur)
            cur.execute("DELETE FROM instrument_keys")


    def save_market_data(self, data_type, data):
        """Save market data to cache by converting dict to JSON"""
        with self._get_cursor() as cur:
            cur.execute("""
                INSERT INTO market_data_cache (data_type, data)
                VALUES (%s, %s)
                ON CONFLICT (data_type) 
                DO UPDATE SET data = EXCLUDED.data, last_updated = NOW()
            """, (data_type, json.dumps(data)))  # Convert dict to JSON string

    def _convert_numpy_types(self, value):
        """Convert numpy types to native Python types"""
        if isinstance(value, (np.floating, np.float32, np.float64)):
            return float(value)
        elif isinstance(value, (np.integer, np.int32, np.int64)):
            return int(value)
        elif isinstance(value, str):
            # Attempt to convert strings to float if possible
            return float(value) if value.replace('.', '', 1).isdigit() else value
        elif isinstance(value, np.ndarray):
            return value.tolist()
        elif isinstance(value, (list, tuple)):
            return [self._convert_numpy_types(x) for x in value]
        elif isinstance(value, dict):
            return {k: self._convert_numpy_types(v) for k, v in value.items()}
        elif isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        elif isinstance(value, Decimal):
            return float(value)
        return value

    def get_access_token(self, account_id=1):
        """Get access token from the database for a specific account ID"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute("""
                    SELECT access_token 
                    FROM upstox_accounts 
                    WHERE id = %s
                """, (account_id,))

                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
                return None
        except Exception as e:
            print(f"Error getting access token from database: {str(e)}")
            return None

    def update_stock_data(self, symbol, interval, data, info_data=None):
        """Update stock data with daily recalculation of pivot points and indicators"""
        try:
            # Convert to DataFrame if it's not already
            if not isinstance(data, pd.DataFrame):
                if isinstance(data, dict):
                    data = pd.DataFrame(data)
                else:
                    raise TypeError(f"Expected DataFrame or dict, got {type(data)}")

            # Make copy to avoid modifying original
            df = data.copy()

            # Ensure the index is datetime
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # Sort by date
            df = df.sort_index()

            # Calculate Heikin-Ashi (HA) values
            if len(df) > 1:
                df['HA_Close'] = (df['Open'] + df['High'] + df['Low'] + df['Close']) / 4
                df['HA_Open'] = (df['Open'].shift(1) + df['Close'].shift(1)) / 2
                df['HA_High'] = df[['High', 'HA_Open', 'HA_Close']].max(axis=1)
                df['HA_Low'] = df[['Low', 'HA_Open', 'HA_Close']].min(axis=1)

            # Ensure HA columns exist in the DataFrame
            for col in ['HA_Open', 'HA_Close', 'HA_High', 'HA_Low']:
                if col not in df.columns:
                    df[col] = None  # Add the column with default None values

            if 'Volume' in df.columns and len(df) > 0:
                # Calculate VWAP for each row
                df['VP'] = df['Close'] * df['Volume']  # Value * Price
                df['CV'] = df['Volume'].cumsum()  # Cumulative Volume
                df['CVP'] = df['VP'].cumsum()  # Cumulative Value * Price
                df['VWAP'] = df['CVP'] / df['CV'].replace(0, np.nan)  # Avoid division by zero
            else:
                df['VWAP'] = df['Close']  # Fallback if no volume

            if len(df) > 1:
                df['macd_line'], df['macd_signal'], df['macd_histogram'] = ta.MACD(df['Close'])

                # Fix for ADX calculation - ensure enough data and better error handling
                df['adx'] = None  # Initialize with None
                df['adx_di_positive'] = None  # Initialize with None
                df['adx_di_negative'] = None  # Initialize with None

                if len(df) >= 14:  # ADX needs at least 14 periods
                    try:
                        # Calculate ADX with explicit error handling
                        adx_values = ta.ADX(df['High'], df['Low'], df['Close'], timeperiod=14)
                        # Only assign if calculation was successful and the result length matches
                        if adx_values is not None and len(adx_values) == len(df):
                            df['adx'] = adx_values

                        plus_di = ta.PLUS_DI(df['High'], df['Low'], df['Close'], timeperiod=14)
                        minus_di = ta.MINUS_DI(df['High'], df['Low'], df['Close'], timeperiod=14)

                        # Assign to DataFrame if calculations were successful
                        if plus_di is not None:
                            df['adx_di_positive'] = plus_di.iloc[-1]

                        if minus_di is not None:
                            df['adx_di_negative'] = minus_di.iloc[-1]

                    except Exception as e:
                        print(f"Error calculating ADX for {symbol}: {str(e)}")
                        # Leave as None - already initialized

                # Calculate DI indicators with similar safeguards

                # Continue with other indicators
                df['parabolic_sar'] = ta.SAR(df['High'], df['Low'])
                df['rsi'] = ta.RSI(df['Close'])
                stoch_rsi_k, stoch_rsi_d = ta.STOCHRSI(df['Close'])
                df['stoch_rsi'] = stoch_rsi_k  # Use only the first output (fastk)
                df['ichimoku_base'] = (df['High'].rolling(window=26).max() + df['Low'].rolling(window=26).min()) / 2
                df['ichimoku_conversion'] = (df['High'].rolling(window=9).max() + df['Low'].rolling(window=9).min()) / 2
                df['ichimoku_span_a'] = ((df['ichimoku_base'] + df['ichimoku_conversion']) / 2).shift(26)
                df['ichimoku_span_b'] = ((df['High'].rolling(window=52).max() + df['Low'].rolling(window=52).min()) / 2).shift(26)
                df['ichimoku_cloud_top'] = df[['ichimoku_span_a', 'ichimoku_span_b']].max(axis=1)
                df['ichimoku_cloud_bottom'] = df[['ichimoku_span_a', 'ichimoku_span_b']].min(axis=1)



            # Calculate Supertrend
            if len(df) > 1:
                atr = ta.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)
                hl2 = (df['High'] + df['Low']) / 2
                df['Supertrend'] = hl2 - (2 * atr)

            # Calculate Accumulation/Distribution (Acc/Dist)
            if len(df) > 1:
                clv = ((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / (df['High'] - df['Low']).replace(0, np.nan)
                df['Acc_Dist'] = clv * df['Volume']

            # Calculate Commodity Channel Index (CCI)
            if len(df) > 1:
                df['CCI'] = ta.CCI(df['High'], df['Low'], df['Close'], timeperiod=20)

            # Calculate Chaikin Money Flow (CMF)
            if len(df) > 1:
                df['CMF'] = ta.CMF(df['High'], df['Low'], df['Close'], df['Volume'], timeperiod=20)
            # Calculate Money Flow Index (MFI)
            if len(df) > 1:
                df['MFI'] = ta.MFI(df['High'], df['Low'], df['Close'], df['Volume'], timeperiod=14)

            # Calculate On Balance Volume (OBV)
            if len(df) > 1:
                df['On_Balance_Volume'] = ta.OBV(df['Close'], df['Volume'])

            # Calculate Williams %R
            if len(df) > 1:
                df['Williams_R'] = ta.WILLR(df['High'], df['Low'], df['Close'], timeperiod=14)

            # Calculate Bollinger Band %B
            # Replace the problematic Bollinger Bands calculation with this code:
            # Enhanced Bollinger Bands calculation to match TradingView implementation
            if len(df) > 1:
                length = 20  # Default length
                mult = 2.0   # Default multiplier for standard deviation

                # Calculate basis (middle band) using SMA
                basis = ta.SMA(df['Close'], timeperiod=length)

                # Calculate standard deviation
                dev = ta.STDDEV(df['Close'], timeperiod=length)

                # Calculate upper and lower bands
                df['upper_bollinger'] = basis + (mult * dev)
                df['middle_bollinger'] = basis  # Store middle band
                df['lower_bollinger'] = basis - (mult * dev)

                # Now calculate Bollinger %B correctly
                denominator = df['upper_bollinger'] - df['lower_bollinger']
                df['Bollinger_B'] = np.where(
                    denominator != 0,
                    (df['Close'] - df['lower_bollinger']) / denominator,
                    0.5  # Default value when upper == lower
                )

            # Calculate Intraday Intensity
            if len(df) > 1:
                df['Intraday_Intensity'] = ((2 * df['Close'] - df['High'] - df['Low']) / (df['High'] - df['Low']).replace(0, np.nan)) * df['Volume']

            # Calculate Force Index
            if len(df) > 1:
                df['Force_Index'] = df['Close'].diff() * df['Volume']

            # Calculate Schaff Trend Cycle (STC)
            if len(df) > 1:
                macd, macd_signal, _ = ta.MACD(df['Close'])
                df['STC'] = ta.STOCH(macd, macd_signal, macd_signal, fastk_period=14, slowk_period=3, slowd_period=3)[0]

            # Calculate new indicators
            if len(df) > 1:
                for period in [10, 20, 50, 100, 200]:
                    df[f'sma{period}'] = df['Close'].rolling(window=period).mean()
                    df[f'ema{period}'] = ta.EMA(df['Close'], timeperiod=period)
                    df[f'wma{period}'] = ta.WMA(df['Close'], timeperiod=period)
                    df[f'tma{period}'] = df['Close'].rolling(window=period).mean().rolling(window=period).mean()
                    df[f'rma{period}'] = df['Close'].ewm(span=period, adjust=False).mean()
                    df[f'tema{period}'] = ta.TEMA(df['Close'], timeperiod=period)
                    df[f'hma{period}'] = ta.WMA(2 * ta.WMA(df['Close'], timeperiod=period // 2) - ta.WMA(df['Close'], timeperiod=period), timeperiod=int(period**0.5))
                    df[f'vwma{period}'] = (df['Close'] * df['Volume']).rolling(window=period).sum() / df['Volume'].rolling(window=period).sum()
                    df[f'std{period}'] = df['Close'].rolling(window=period).std()

            if len(df) > 1:
                # ATR (Average True Range)
                df['ATR'] = ta.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)

                # TRIX (Triple Exponential Average)
                df['TRIX'] = ta.TRIX(df['Close'], timeperiod=14)

                # ROC (Rate of Change)
                df['ROC'] = ta.ROC(df['Close'], timeperiod=10)

                # Keltner Channels
                df['Keltner_Middle'] = df['Close'].rolling(window=20).mean()
                df['Keltner_Upper'] = df['Keltner_Middle'] + (2 * df['ATR'])
                df['Keltner_Lower'] = df['Keltner_Middle'] - (2 * df['ATR'])

                # Donchian Channels
                df['Donchian_High'] = df['High'].rolling(window=20).max()
                df['Donchian_Low'] = df['Low'].rolling(window=20).min()

                # Chaikin Oscillator
                ad_line = ta.AD(df['High'], df['Low'], df['Close'], df['Volume'])
                df['Chaikin_Oscillator'] = ta.ADOSC(df['High'], df['Low'], df['Close'], df['Volume'], fastperiod=3, slowperiod=10)

            if len(df) > 1:
                # Calculate TRUE_RANGE
                df['true_range'] = ta.TRUE_RANGE(df['High'], df['Low'], df['Close'])

                # Calculate ATR
                df['ATR'] = ta.ATR(df['High'], df['Low'], df['Close'], timeperiod=14)

                # Calculate AROON indicators
                df['aroon_up'] = ta.AROON_UP(df['High'], timeperiod=14)
                df['aroon_down'] = ta.AROON_DOWN(df['Low'], timeperiod=14)
                df['aroon_osc'] = ta.AROON_OSC(df['High'], df['Low'], timeperiod=14)

            if len(df) > 1:
                # Calculate buyer/seller initiated trades and related metrics
                buyer_seller_metrics = ta.buyer_seller_initiated_trades(df['Close'], df['Volume'])
                # Add all new columns to the DataFrame at once using pd.concat
                df = pd.concat([df, pd.DataFrame(buyer_seller_metrics, index=df.index)], axis=1)

            if len(df) > 1:
                # Calculate Wave Trend indicators
                df['wave_trend'], df['wave_trend_trigger'], df['wave_trend_momentum'] = ta.WAVE_TREND(
                    df['High'], df['Low'], df['Close']
                )

            # Ensure all required columns exist in the DataFrame
            # Filter out rows with NaN in critical indicators like Supertrend
            df = df.dropna(subset=['Supertrend'])

            # Ensure the index is datetime and sort by date
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            df = df.sort_index()

            # Take only the most recent valid data point
            latest_data = df.iloc[-1] if not df.empty else None
            if latest_data is None:
                print(f"No valid data available for {symbol} at interval {interval}.")
                return False

            latest_data = latest_data.apply(lambda x: self._convert_numpy_types(x))

            # Extract values for current data
            timestamp = latest_data.name.to_pydatetime()
            high = float(latest_data['High'])
            low = float(latest_data['Low'])
            close = float(latest_data['Close'])
            Open = float(latest_data['Open'])
            volume = float(latest_data['Volume']) if 'Volume' in latest_data else 0
            vwap = float(latest_data['VWAP']) if not pd.isna(latest_data['VWAP']) else close  # Use close as fallback


            if len(df) > 1:
                prev_close = float(df.iloc[-2]['Close'])
                price_change = close - prev_close
                percent_change = (price_change / prev_close) * 100 if prev_close != 0 else 0
            else:
                price_change = None
                percent_change = None


            # Check if we need to recalculate pivot points
            current_date = datetime.now().date()

            # Create a cache key for this symbol/interval
            cache_key = f"{symbol}_{interval}"

            # Check if we've already calculated pivots today
            pivot_needs_update = True
            pivot = r1 = r2 = r3 = s1 = s2 = s3 = None

            if cache_key in self.pivot_calculation_cache:
                last_calc_date = self.pivot_calculation_cache[cache_key]['date']
                if last_calc_date == current_date:
                    # We already calculated pivots today, reuse them from cache
                    pivot_data = self.pivot_calculation_cache[cache_key]
                    pivot, r1, r2, r3, s1, s2, s3 = (
                        pivot_data['pivot'], pivot_data['r1'], pivot_data['r2'],
                        pivot_data['r3'], pivot_data['s1'], pivot_data['s2'], pivot_data['s3']
                    )
                    pivot_needs_update = False
            if len(df) > 1:
                df['rsi'] = ta.RSI(df['Close'])
            # Calculate new pivot points if needed
            if pivot_needs_update:
                # First check if we can get them from the database
                with self._get_cursor() as cur:
                    cur.execute("""
                        SELECT pivot, r1, r2, r3, s1, s2, s3, DATE(last_updated)
                        FROM stock_data_cache
                        WHERE symbol = %s AND interval = %s
                        ORDER BY last_updated DESC
                        LIMIT 1
                    """, (symbol, interval))

                    existing_data = cur.fetchone()

                    if existing_data and existing_data[7] == current_date:
                        # Already calculated today, reuse from database
                        pivot, r1, r2, r3, s1, s2, s3 = existing_data[0:7]
                        pivot_needs_update = False
                    else:
                        # Need to calculate new pivot points
                        try:
                            # Get anchor data efficiently using cached function
                            anchor_data = self.get_anchored_pivot_data(symbol, interval)

                            if anchor_data is not None and len(anchor_data) >= 2:
                                prev_data = anchor_data.iloc[-2]
                                prev_high = float(prev_data['High'])
                                prev_low = float(prev_data['Low'])
                                prev_close = float(prev_data['Close'])

                                # Calculate pivot points
                                pivot = (prev_high + prev_low + prev_close) / 3
                                r1 = (2 * pivot) - prev_low
                                r2 = pivot + (prev_high - prev_low)
                                r3 = prev_high + 2 * (pivot - prev_low)
                                s1 = (2 * pivot) - prev_high
                                s2 = pivot - (prev_high - prev_low)
                                s3 = prev_low - 2 * (prev_high - pivot)
                            else:
                                # Fallback: use previous data point if available
                                if len(df) >= 2:
                                    prev_data = df.iloc[-2]
                                    prev_high = float(prev_data['High'])
                                    prev_low = float(prev_data['Low'])
                                    prev_close = float(prev_data['Close'])

                                    pivot = (prev_high + prev_low + prev_close) / 3
                                    r1 = (2 * pivot) - prev_low
                                    r2 = pivot + (prev_high - prev_low)
                                    r3 = prev_high + 2 * (pivot - prev_low)
                                    s1 = (2 * pivot) - prev_high
                                    s2 = pivot - (prev_high - prev_low)
                                    s3 = prev_low - 2 * (prev_high - pivot)
                        except Exception as e:
                            print(f"Error calculating pivot points for {symbol}: {str(e)}")
                            # Try to use existing data if available
                            if existing_data:
                                pivot, r1, r2, r3, s1, s2, s3 = existing_data[0:7]
                                pivot_needs_update = False

                # If we've calculated new pivot points, update the cache
                if pivot_needs_update and pivot is not None:
                    self.pivot_calculation_cache[cache_key] = {
                        'date': current_date,
                        'pivot': pivot,
                        'r1': r1, 'r2': r2, 'r3': r3,
                        's1': s1, 's2': s2, 's3': s3
                    }

            # Convert NumPy types to native Python types for the latest data
            latest_data = df.iloc[-1] if not df.empty else None
            if latest_data is None:
                return False

            # Ensure all extracted values are native Python types
            pivot = float(pivot) if pivot is not None else None
            r1 = float(r1) if r1 is not None else None
            r2 = float(r2) if r2 is not None else None
            r3 = float(r3) if r3 is not None else None
            s1 = float(s1) if s1 is not None else None
            s2 = float(s2) if s2 is not None else None
            s3 = float(s3) if s3 is not None else None

            # Safely convert all columns in `latest_data` to native Python types
            if isinstance(latest_data, pd.Series):
                latest_data = latest_data.map(
                    lambda x: float(x) if isinstance(x, (np.float64, np.float32, np.int64, np.int32)) else x
                )
            else:
                raise TypeError("Expected `latest_data` to be a pandas Series, but got: {}".format(type(latest_data)))

            # Extract fundamental data from info_data if provided
            pe_ratio = info_data.get('trailingPE') if info_data else None
            beta = info_data.get('beta') if info_data else None

            book_value = info_data.get('bookValue')
            debt_to_equity = info_data.get('debtToEquity')
            dividend_rate = info_data.get('dividendRate')
            dividend_yield = info_data.get('dividendYield')
            earnings_growth = info_data.get('earningsGrowth')
            earnings_quarterly_growth = info_data.get('earningsQuarterlyGrowth')
            ebitda_margins = info_data.get('ebitdaMargins')
            enterprise_to_ebitda = info_data.get('enterpriseToEbitda')
            enterprise_to_revenue = info_data.get('enterpriseToRevenue')
            enterprise_value = info_data.get('enterpriseValue')
            eps_trailing_twelve_months = info_data.get('epsTrailingTwelveMonths')
            five_year_avg_dividend_yield = info_data.get('fiveYearAvgDividendYield')
            float_shares = info_data.get('floatShares')
            forward_eps = info_data.get('forwardEps')
            forward_pe = info_data.get('forwardPE')
            free_cashflow = info_data.get('freeCashflow')
            gross_margins = info_data.get('grossMargins')
            gross_profits = info_data.get('grossProfits')
            industry = info_data.get('industry')
            market_cap = info_data.get('marketCap')
            operating_cashflow = info_data.get('operatingCashflow')
            operating_margins = info_data.get('operatingMargins')
            payout_ratio = info_data.get('payoutRatio')
            price_to_book = info_data.get('priceToBook')
            profit_margins = info_data.get('profitMargins')
            return_on_assets = info_data.get('returnOnAssets')
            return_on_equity = info_data.get('returnOnEquity')
            revenue_growth = info_data.get('revenueGrowth')
            revenue_per_share = info_data.get('revenuePerShare')
            sector = info_data.get('sector')
            total_cash = info_data.get('totalCash')
            total_cash_per_share = info_data.get('totalCashPerShare')
            total_debt = info_data.get('totalDebt')
            total_revenue = info_data.get('totalRevenue')
            trailing_eps = info_data.get('trailingEps')
            trailing_pe = info_data.get('trailingPE')
            trailing_peg_ratio = info_data.get('trailingPegRatio')

            # Calculate 52-week high/low data if we have enough historical data
            week52_high = week52_low = None
            pct_from_high = pct_from_low = None
            days_since_high = days_since_low = None
            w52_status = None

            if len(df) > 30:  # Ensure we have enough data
                try:
                    # Use up to 252 trading days (approximately 1 year)
                    lookback = min(252, len(df))
                    year_data = df.iloc[-lookback:]

                    # Calculate 52-week high and low
                    week52_high = year_data['High'].max()
                    week52_low = year_data['Low'].min()

                    # Get the dates of the high and low
                    high_date = year_data['High'].idxmax()
                    low_date = year_data['Low'].idxmax()

                    # Calculate percentage from high and low
                    pct_from_high = ((week52_high - latest_data['Close']) / week52_high) * 100
                    pct_from_low = ((latest_data['Close'] - week52_low) / week52_low) * 100

                    # Calculate days since high and low
                    current_date = timestamp.date() if hasattr(timestamp, 'date') else datetime.now().date()
                    high_date = high_date.date() if hasattr(high_date, 'date') else high_date
                    low_date = low_date.date() if hasattr(low_date, 'date') else low_date

                    days_since_high = (current_date - high_date).days
                    days_since_low = (current_date - low_date).days

                    # Determine status (near high, near low, or neutral)
                    if pct_from_high <= 5.0:
                        w52_status = 'near_high'
                    elif pct_from_low <= 5.0:
                        w52_status = 'near_low'
                    else:
                        w52_status = 'neutral'

                except Exception as e:
                    print(f"Error calculating 52-week data for {symbol}: {e}")

            # Prepare data for insertion
            required_columns = [
                'HA_Open', 'HA_Close', 'HA_High', 'HA_Low', 'Supertrend',
                'upper_bollinger', 'lower_bollinger', 'middle_bollinger',
                'ichimoku_base', 'ichimoku_conversion', 'ichimoku_span_a', 'ichimoku_span_b',
                'ichimoku_cloud_top', 'ichimoku_cloud_bottom',
                'stoch_rsi', 'parabolic_sar', 'macd_line', 'macd_signal', 'macd_histogram',
                'adx', 'adx_di_positive', 'adx_di_negative',
                'Acc_Dist', 'CCI', 'CMF', 'MFI', 'On_Balance_Volume', 'Williams_R',
                'Bollinger_B', 'Intraday_Intensity', 'Force_Index', 'STC',  # Added bollinger_b
                'sma10', 'sma20', 'sma50', 'sma100', 'sma200',
                'ema10', 'ema50', 'ema100', 'ema200',
                'wma10', 'wma50', 'wma100', 'wma200',
                'tma10', 'tma50', 'tma100', 'tma200',
                'rma10', 'rma50', 'rma100', 'rma200',
                'tema10', 'tema50', 'tema100', 'tema200',
                'hma10', 'hma50', 'hma100', 'hma200',
                'vwma10', 'vwma50', 'vwma100', 'vwma200',
                'std10', 'std50', 'std100', 'std200',
                'ATR', 'TRIX', 'ROC', 'rsi', 'VWAP',  # Added rsi, VWAP
                'Keltner_Middle', 'Keltner_Upper', 'Keltner_Lower',
                'Donchian_High', 'Donchian_Low', 'Chaikin_Oscillator',
                'true_range', 'aroon_up', 'aroon_down', 'aroon_osc',
                'buyer_initiated_trades', 'buyer_initiated_quantity', 'buyer_initiated_avg_qty',
                'seller_initiated_trades', 'seller_initiated_quantity', 'seller_initiated_avg_qty',
                'buyer_seller_ratio', 'buyer_seller_quantity_ratio', 'buyer_initiated_vwap', 'seller_initiated_vwap',
                'wave_trend', 'wave_trend_trigger', 'wave_trend_momentum'
            ]

            # Identify missing columns
            missing_columns = {col: None for col in required_columns if col not in df.columns}

            # Add missing columns in one operation
            if missing_columns:
                print("missing columns", missing_columns)

            # Update database using a single query with upsert
            with self._get_cursor() as cur:
                upsert_sql = """
                    INSERT INTO stock_data_cache (
                        symbol, interval, timestamp, open, high, low, close, volume,
                        pivot, r1, r2, r3, s1, s2, s3, price_change, percent_change, vwap,
                        pe_ratio, beta,
                        ichimoku_base, ichimoku_conversion, ichimoku_span_a, ichimoku_span_b,
                        ichimoku_cloud_top, ichimoku_cloud_bottom, stoch_rsi, parabolic_sar,
                        upper_bollinger, lower_bollinger, middle_bollinger, macd_line, macd_signal, macd_histogram,
                        adx, adx_di_positive, adx_di_negative, HA_Open, HA_Close, HA_High, HA_Low,
                        Supertrend, Acc_Dist, CCI, CMF, MFI, On_Balance_Volume, Williams_R,
                        Bollinger_B, Intraday_Intensity, Force_Index, STC, sma10, sma20, sma50, sma100,
                        sma200, ema10, ema50, ema100, ema200, wma10, wma50, wma100, wma200,
                        tma10, tma50, tma100, tma200, rma10, rma50, rma100, rma200, tema10,
                        tema50, tema100, tema200, hma10, hma50, hma100, hma200, vwma10, vwma50,
                        vwma100, vwma200, std10, std50, std100, std200, ATR, TRIX, ROC,
                        Keltner_Middle, Keltner_Upper, Keltner_Lower, Donchian_High, Donchian_Low,
                        Chaikin_Oscillator, rsi, true_range, aroon_up, aroon_down, aroon_osc,
                        buyer_initiated_trades, buyer_initiated_quantity, buyer_initiated_avg_qty,
                        seller_initiated_trades, seller_initiated_quantity, seller_initiated_avg_qty,
                        buyer_seller_ratio, buyer_seller_quantity_ratio, buyer_initiated_vwap, seller_initiated_vwap,
                        wave_trend, wave_trend_trigger, wave_trend_momentum,
                        book_value, debt_to_equity, dividend_rate, dividend_yield,
                        earnings_growth, earnings_quarterly_growth, ebitda_margins,
                        enterprise_to_ebitda, enterprise_to_revenue, enterprise_value,
                        eps_trailing_twelve_months, five_year_avg_dividend_yield, float_shares,
                        forward_eps, forward_pe, free_cashflow, gross_margins, gross_profits,
                        industry, market_cap, operating_cashflow, operating_margins,
                        payout_ratio, price_to_book, profit_margins, return_on_assets,
                        return_on_equity, revenue_growth, revenue_per_share, sector,
                        total_cash, total_cash_per_share, total_debt, total_revenue,
                        trailing_eps, trailing_pe, trailing_peg_ratio,
                        week52_high, week52_low, pct_from_week52_high, pct_from_week52_low, 
                        days_since_week52_high, days_since_week52_low, week52_status
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (symbol, interval)
                    DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
                        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                        close = EXCLUDED.close, volume = EXCLUDED.volume, pivot = EXCLUDED.pivot,
                        r1 = EXCLUDED.r1, r2 = EXCLUDED.r2, r3 = EXCLUDED.r3, s1 = EXCLUDED.s1,
                        s2 = EXCLUDED.s2, s3 = EXCLUDED.s3, price_change = EXCLUDED.price_change,
                        percent_change = EXCLUDED.percent_change, vwap = EXCLUDED.vwap,
                        pe_ratio = EXCLUDED.pe_ratio, market_cap = EXCLUDED.market_cap,
                        dividend_yield = EXCLUDED.dividend_yield, price_to_book = EXCLUDED.price_to_book,
                        beta = EXCLUDED.beta,
                        ichimoku_base = EXCLUDED.ichimoku_base, ichimoku_conversion = EXCLUDED.ichimoku_conversion,
                        ichimoku_span_a = EXCLUDED.ichimoku_span_a, ichimoku_span_b = EXCLUDED.ichimoku_span_b,
                        ichimoku_cloud_top = EXCLUDED.ichimoku_cloud_top, ichimoku_cloud_bottom = EXCLUDED.ichimoku_cloud_bottom,
                        stoch_rsi = EXCLUDED.stoch_rsi, parabolic_sar = EXCLUDED.parabolic_sar,
                        upper_bollinger = EXCLUDED.upper_bollinger, lower_bollinger = EXCLUDED.lower_bollinger,
                        middle_bollinger = EXCLUDED.middle_bollinger, macd_line = EXCLUDED.macd_line, 
                        macd_signal = EXCLUDED.macd_signal, macd_histogram = EXCLUDED.macd_histogram, 
                        adx = EXCLUDED.adx, adx_di_positive = EXCLUDED.adx_di_positive, 
                        adx_di_negative = EXCLUDED.adx_di_negative, HA_Open = EXCLUDED.HA_Open, 
                        HA_Close = EXCLUDED.HA_Close, ha_high = EXCLUDED.HA_High, HA_Low = EXCLUDED.HA_Low, 
                        Supertrend = EXCLUDED.Supertrend, Acc_Dist = EXCLUDED.Acc_Dist, CCI = EXCLUDED.CCI, 
                        CMF = EXCLUDED.CMF, MFI = EXCLUDED.MFI, On_Balance_Volume = EXCLUDED.On_Balance_Volume,
                        Williams_R = EXCLUDED.Williams_R, Bollinger_B = EXCLUDED.Bollinger_B,
                        Intraday_Intensity = EXCLUDED.Intraday_Intensity, Force_Index = EXCLUDED.Force_Index,
                        STC = EXCLUDED.STC, sma10 = EXCLUDED.sma10, sma20 = EXCLUDED.sma20, sma50 = EXCLUDED.sma50, 
                        sma100 = EXCLUDED.sma100, sma200 = EXCLUDED.sma200, ema10 = EXCLUDED.ema10, ema50 = EXCLUDED.ema50, 
                        ema100 = EXCLUDED.ema100, ema200 = EXCLUDED.ema200, wma10 = EXCLUDED.wma10, wma50 = EXCLUDED.wma50, 
                        wma100 = EXCLUDED.wma100, wma200 = EXCLUDED.wma200, tma10 = EXCLUDED.tma10, tma50 = EXCLUDED.tma50, 
                        tma100 = EXCLUDED.tma100, tma200 = EXCLUDED.tma200, rma10 = EXCLUDED.rma10, rma50 = EXCLUDED.rma50, 
                        rma100 = EXCLUDED.rma100, rma200 = EXCLUDED.rma200, tema10 = EXCLUDED.tema10, tema50 = EXCLUDED.tema50, 
                        tema100 = EXCLUDED.tema100, tema200 = EXCLUDED.tema200, hma10 = EXCLUDED.hma10, hma50 = EXCLUDED.hma50, 
                        hma100 = EXCLUDED.hma100, hma200 = EXCLUDED.hma200, vwma10 = EXCLUDED.vwma10, vwma50 = EXCLUDED.vwma50, 
                        vwma100 = EXCLUDED.vwma100, vwma200 = EXCLUDED.vwma200, std10 = EXCLUDED.std10, std50 = EXCLUDED.std50, 
                        std100 = EXCLUDED.std100, std200 = EXCLUDED.std200, ATR = EXCLUDED.ATR, TRIX = EXCLUDED.TRIX, 
                        ROC = EXCLUDED.ROC, Keltner_Middle = EXCLUDED.Keltner_Middle, Keltner_Upper = EXCLUDED.Keltner_Upper, 
                        Keltner_Lower = EXCLUDED.Keltner_Lower, Donchian_High = EXCLUDED.Donchian_High, Donchian_Low = EXCLUDED.Donchian_Low, 
                        Chaikin_Oscillator = EXCLUDED.Chaikin_Oscillator, rsi = EXCLUDED.rsi, true_range = EXCLUDED.true_range, 
                        aroon_up = EXCLUDED.aroon_up, aroon_down = EXCLUDED.aroon_down, aroon_osc = EXCLUDED.aroon_osc,
                        buyer_initiated_trades = EXCLUDED.buyer_initiated_trades,
                        buyer_initiated_quantity = EXCLUDED.buyer_initiated_quantity,
                        buyer_initiated_avg_qty = EXCLUDED.buyer_initiated_avg_qty,
                        seller_initiated_trades = EXCLUDED.seller_initiated_trades,
                        seller_initiated_quantity = EXCLUDED.seller_initiated_quantity,
                        seller_initiated_avg_qty = EXCLUDED.seller_initiated_avg_qty,
                        buyer_seller_ratio = EXCLUDED.buyer_seller_ratio,
                        buyer_seller_quantity_ratio = EXCLUDED.buyer_seller_quantity_ratio,
                        buyer_initiated_vwap = EXCLUDED.buyer_initiated_vwap,
                        seller_initiated_vwap = EXCLUDED.seller_initiated_vwap,
                        wave_trend = EXCLUDED.wave_trend,
                        wave_trend_trigger = EXCLUDED.wave_trend_trigger,
                        wave_trend_momentum = EXCLUDED.wave_trend_momentum,
                        book_value = EXCLUDED.book_value,
                        debt_to_equity = EXCLUDED.debt_to_equity,
                        dividend_rate = EXCLUDED.dividend_rate,
                        earnings_growth = EXCLUDED.earnings_growth,
                        earnings_quarterly_growth = EXCLUDED.earnings_quarterly_growth,
                        ebitda_margins = EXCLUDED.ebitda_margins,
                        enterprise_to_ebitda = EXCLUDED.enterprise_to_ebitda,
                        enterprise_to_revenue = EXCLUDED.enterprise_to_revenue,
                        enterprise_value = EXCLUDED.enterprise_value,
                        eps_trailing_twelve_months = EXCLUDED.eps_trailing_twelve_months,
                        five_year_avg_dividend_yield = EXCLUDED.five_year_avg_dividend_yield,
                        float_shares = EXCLUDED.float_shares,
                        forward_eps = EXCLUDED.forward_eps,
                        forward_pe = EXCLUDED.forward_pe,
                        free_cashflow = EXCLUDED.free_cashflow,
                        gross_margins = EXCLUDED.gross_margins,
                        gross_profits = EXCLUDED.gross_profits,
                        industry = EXCLUDED.industry,
                        operating_cashflow = EXCLUDED.operating_cashflow,
                        operating_margins = EXCLUDED.operating_margins,
                        payout_ratio = EXCLUDED.payout_ratio,
                        profit_margins = EXCLUDED.profit_margins,
                        return_on_assets = EXCLUDED.return_on_assets,
                        return_on_equity = EXCLUDED.return_on_equity,
                        revenue_growth = EXCLUDED.revenue_growth,
                        revenue_per_share = EXCLUDED.revenue_per_share,
                        sector = EXCLUDED.sector,
                        total_cash = EXCLUDED.total_cash,
                        total_cash_per_share = EXCLUDED.total_cash_per_share,
                        total_debt = EXCLUDED.total_debt,
                        total_revenue = EXCLUDED.total_revenue,
                        trailing_eps = EXCLUDED.trailing_eps,
                        trailing_pe = EXCLUDED.trailing_pe,
                        trailing_peg_ratio = EXCLUDED.trailing_peg_ratio,
                        week52_high = EXCLUDED.week52_high,
                        week52_low = EXCLUDED.week52_low,
                        pct_from_week52_high = EXCLUDED.pct_from_week52_high,
                        pct_from_week52_low = EXCLUDED.pct_from_week52_low,
                        days_since_week52_high = EXCLUDED.days_since_week52_high,
                        days_since_week52_low = EXCLUDED.days_since_week52_low,
                        week52_status = EXCLUDED.week52_status
                """

                params = (
                    symbol,                                                    # 1
                    interval,                                                  # 2
                    self._convert_numpy_types(timestamp),                      # 3
                    self._convert_numpy_types(Open),                     # 4
                    self._convert_numpy_types(high),                           # 5
                    self._convert_numpy_types(low),                            # 6
                    self._convert_numpy_types(close),                          # 7
                    self._convert_numpy_types(volume),                         # 8
                    self._convert_numpy_types(pivot),                          # 9
                    self._convert_numpy_types(r1),                            # 10
                    self._convert_numpy_types(r2),                            # 11
                    self._convert_numpy_types(r3),                            # 12
                    self._convert_numpy_types(s1),                            # 13
                    self._convert_numpy_types(s2),                            # 14
                    self._convert_numpy_types(s3),                            # 15
                    self._convert_numpy_types(price_change),                  # 16
                    self._convert_numpy_types(percent_change),                # 17
                    self._convert_numpy_types(vwap),                          # 18
                    self._convert_numpy_types(pe_ratio),                      # 19
                    self._convert_numpy_types(beta),                          # 23
                    # Ichimoku
                    self._convert_numpy_types(latest_data['ichimoku_base']),            # 24
                    self._convert_numpy_types(latest_data['ichimoku_conversion']),      # 25
                    self._convert_numpy_types(latest_data['ichimoku_span_a']),          # 26
                    self._convert_numpy_types(latest_data['ichimoku_span_b']),          # 27
                    self._convert_numpy_types(latest_data['ichimoku_cloud_top']),       # 28
                    self._convert_numpy_types(latest_data['ichimoku_cloud_bottom']),    # 29
                    # Other indicators
                    self._convert_numpy_types(latest_data['stoch_rsi']),               # 30
                    self._convert_numpy_types(latest_data['parabolic_sar']),           # 31
                    # Bollinger Bands
                    self._convert_numpy_types(latest_data['upper_bollinger']),         # 32
                    self._convert_numpy_types(latest_data['lower_bollinger']),         # 33
                    self._convert_numpy_types(latest_data['middle_bollinger']),        # 34
                    # MACD
                    self._convert_numpy_types(latest_data['macd_line']),               # 35
                    self._convert_numpy_types(latest_data['macd_signal']),             # 36
                    self._convert_numpy_types(latest_data['macd_histogram']),          # 37
                    # ADX
                    self._convert_numpy_types(latest_data['adx']),                     # 38
                    self._convert_numpy_types(latest_data['adx_di_positive']),         # 39
                    self._convert_numpy_types(latest_data['adx_di_negative']),         # 40
                    # Heikin Ashi
                    self._convert_numpy_types(latest_data['HA_Open']),                 # 41
                    self._convert_numpy_types(latest_data['HA_Close']),                # 42
                    self._convert_numpy_types(latest_data['HA_High']),                 # 43
                    self._convert_numpy_types(latest_data['HA_Low']),                  # 44
                    # Volume indicators
                    self._convert_numpy_types(latest_data['Supertrend']),              # 45
                    self._convert_numpy_types(latest_data['Acc_Dist']),                # 46
                    self._convert_numpy_types(latest_data['CCI']),                     # 47
                    self._convert_numpy_types(latest_data['CMF']),                     # 48
                    self._convert_numpy_types(latest_data['MFI']),                     # 49
                    self._convert_numpy_types(latest_data['On_Balance_Volume']),       # 50
                    self._convert_numpy_types(latest_data['Williams_R']),              # 51
                    self._convert_numpy_types(latest_data['Bollinger_B']),             # 52
                    self._convert_numpy_types(latest_data['Intraday_Intensity']),      # 53
                    self._convert_numpy_types(latest_data['Force_Index']),             # 54
                    self._convert_numpy_types(latest_data['STC']),                     # 55
                    # Moving averages
                    self._convert_numpy_types(latest_data['sma10']),
                    self._convert_numpy_types(latest_data.get('sma20', None)),# 56
                    self._convert_numpy_types(latest_data['sma50']),                   # 57
                    self._convert_numpy_types(latest_data['sma100']),                  # 58
                    self._convert_numpy_types(latest_data['sma200']),                  # 59
                    self._convert_numpy_types(latest_data['ema10']),                   # 60
                    self._convert_numpy_types(latest_data['ema50']),                   # 61
                    self._convert_numpy_types(latest_data['ema100']),                  # 62
                    self._convert_numpy_types(latest_data['ema200']),                  # 63
                    self._convert_numpy_types(latest_data['wma10']),                   # 64
                    self._convert_numpy_types(latest_data['wma50']),                   # 65
                    self._convert_numpy_types(latest_data['wma100']),                  # 66
                    self._convert_numpy_types(latest_data['wma200']),                  # 67
                    self._convert_numpy_types(latest_data['tma10']),                   # 68
                    self._convert_numpy_types(latest_data['tma50']),                   # 69
                    self._convert_numpy_types(latest_data['tma100']),                  # 70
                    self._convert_numpy_types(latest_data['tma200']),                  # 71
                    self._convert_numpy_types(latest_data['rma10']),                   # 72
                    self._convert_numpy_types(latest_data['rma50']),                   # 73
                    self._convert_numpy_types(latest_data['rma100']),                  # 74
                    self._convert_numpy_types(latest_data['rma200']),                  # 75
                    self._convert_numpy_types(latest_data['tema10']),                  # 76
                    self._convert_numpy_types(latest_data['tema50']),                  # 77
                    self._convert_numpy_types(latest_data['tema100']),                 # 78
                    self._convert_numpy_types(latest_data['tema200']),                 # 79
                    self._convert_numpy_types(latest_data['hma10']),                   # 80
                    self._convert_numpy_types(latest_data['hma50']),                   # 81
                    self._convert_numpy_types(latest_data['hma100']),                  # 82
                    self._convert_numpy_types(latest_data['hma200']),                  # 83
                    self._convert_numpy_types(latest_data['vwma10']),                  # 84
                    self._convert_numpy_types(latest_data['vwma50']),                  # 85
                    self._convert_numpy_types(latest_data['vwma100']),                 # 86
                    self._convert_numpy_types(latest_data['vwma200']),                 # 87
                    # Standard deviations
                    self._convert_numpy_types(latest_data['std10']),                   # 88
                    self._convert_numpy_types(latest_data['std50']),                   # 89
                    self._convert_numpy_types(latest_data['std100']),                  # 90
                    self._convert_numpy_types(latest_data['std200']),                  # 91
                    # Other indicators
                    self._convert_numpy_types(latest_data['ATR']),                     # 92
                    self._convert_numpy_types(latest_data['TRIX']),                    # 93
                    self._convert_numpy_types(latest_data['ROC']),                     # 94
                    # Keltner Channels
                    self._convert_numpy_types(latest_data['Keltner_Middle']),          # 95
                    self._convert_numpy_types(latest_data['Keltner_Upper']),           # 96
                    self._convert_numpy_types(latest_data['Keltner_Lower']),           # 97
                    # Donchian Channels
                    self._convert_numpy_types(latest_data['Donchian_High']),           # 98
                    self._convert_numpy_types(latest_data['Donchian_Low']),            # 99
                    # Chaikin
                    self._convert_numpy_types(latest_data['Chaikin_Oscillator']),      # 100

                    # These may be the missing parameters:
                    self._convert_numpy_types(latest_data.get('rsi', None)),           # 101  <-- RSI was missing
                    self._convert_numpy_types(latest_data.get('true_range', None)),    # 102  <-- true_range was missing
                    self._convert_numpy_types(latest_data.get('aroon_up', None)),      # 103  <-- aroon_up was missing
                    self._convert_numpy_types(latest_data.get('aroon_down', None)),    # 104  <-- aroon_down was missing
                    self._convert_numpy_types(latest_data.get('aroon_osc', None)),     # 105 <-- aroon_osc was missing
                    self._convert_numpy_types(latest_data['buyer_initiated_trades']),
                    self._convert_numpy_types(latest_data['buyer_initiated_quantity']),
                    self._convert_numpy_types(latest_data['buyer_initiated_avg_qty']),
                    self._convert_numpy_types(latest_data['seller_initiated_trades']),
                    self._convert_numpy_types(latest_data['seller_initiated_quantity']),
                    self._convert_numpy_types(latest_data['seller_initiated_avg_qty']),
                    self._convert_numpy_types(latest_data['buyer_seller_ratio']),
                    self._convert_numpy_types(latest_data['buyer_seller_quantity_ratio']),
                    self._convert_numpy_types(latest_data['buyer_initiated_vwap']),
                    self._convert_numpy_types(latest_data['seller_initiated_vwap']),
                    self._convert_numpy_types(latest_data['wave_trend']),
                    self._convert_numpy_types(latest_data['wave_trend_trigger']),
                    self._convert_numpy_types(latest_data['wave_trend_momentum']),
                    self._convert_numpy_types(book_value),                      # book_value
                    self._convert_numpy_types(debt_to_equity),                  # debt_to_equity
                    self._convert_numpy_types(dividend_rate),                   # dividend_rate
                    self._convert_numpy_types(dividend_yield),                  # dividend_yield
                    self._convert_numpy_types(earnings_growth),                 # earnings_growth
                    self._convert_numpy_types(earnings_quarterly_growth),       # earnings_quarterly_growth
                    self._convert_numpy_types(ebitda_margins),                  # ebitda_margins
                    self._convert_numpy_types(enterprise_to_ebitda),            # enterprise_to_ebitda
                    self._convert_numpy_types(enterprise_to_revenue),           # enterprise_to_revenue
                    self._convert_numpy_types(enterprise_value),                # enterprise_value
                    self._convert_numpy_types(eps_trailing_twelve_months),      # eps_trailing_twelve_months
                    self._convert_numpy_types(five_year_avg_dividend_yield),    # five_year_avg_dividend_yield
                    self._convert_numpy_types(float_shares),                    # float_shares
                    self._convert_numpy_types(forward_eps),                     # forward_eps
                    self._convert_numpy_types(forward_pe),                      # forward_pe
                    self._convert_numpy_types(free_cashflow),                   # free_cashflow
                    self._convert_numpy_types(gross_margins),                   # gross_margins
                    self._convert_numpy_types(gross_profits),                   # gross_profits
                    self._convert_numpy_types(industry),                        # industry
                    self._convert_numpy_types(market_cap),                      # market_cap
                    self._convert_numpy_types(operating_cashflow),              # operating_cashflow
                    self._convert_numpy_types(operating_margins),               # operating_margins
                    self._convert_numpy_types(payout_ratio),                    # payout_ratio
                    self._convert_numpy_types(price_to_book),                   # price_to_book
                    self._convert_numpy_types(profit_margins),                  # profit_margins
                    self._convert_numpy_types(return_on_assets),                # return_on_assets
                    self._convert_numpy_types(return_on_equity),                # return_on_equity
                    self._convert_numpy_types(revenue_growth),                  # revenue_growth
                    self._convert_numpy_types(revenue_per_share),               # revenue_per_share
                    self._convert_numpy_types(sector),                          # sector
                    self._convert_numpy_types(total_cash),                      # total_cash
                    self._convert_numpy_types(total_cash_per_share),            # total_cash_per_share
                    self._convert_numpy_types(total_debt),                      # total_debt
                    self._convert_numpy_types(total_revenue),                   # total_revenue
                    self._convert_numpy_types(trailing_eps),                    # trailing_eps
                    self._convert_numpy_types(trailing_pe),                     # trailing_pe
                    self._convert_numpy_types(trailing_peg_ratio),              # trailing_peg_ratio
                    self._convert_numpy_types(week52_high),                     # week52_high
                    self._convert_numpy_types(week52_low),                      # week52_low
                    self._convert_numpy_types(pct_from_high),                   # pct_from_high
                    self._convert_numpy_types(pct_from_low),                    # pct_from_low
                    self._convert_numpy_types(days_since_high),                 # days_since_high
                    self._convert_numpy_types(days_since_low),                  # days_since_low
                    w52_status                                                # week52_status
                )

                cur.execute(upsert_sql, params)

            return True
        except Exception as e:
            import traceback
            print(f"Error updating {symbol} data: {str(e)}")
            traceback.print_exc()
            return False


    def update_only_stock_data(self, symbol, interval, data, info_data=None):
        """Update stock data with daily recalculation of pivot points and indicators"""
        try:
            # Convert to DataFrame if it's not already
            if not isinstance(data, pd.DataFrame):
                if isinstance(data, dict):
                    data = pd.DataFrame(data)
                else:
                    raise TypeError(f"Expected DataFrame or dict, got {type(data)}")

            # Make copy to avoid modifying original
            df = data.copy()

            # Ensure the index is datetime
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)



            # Take only the most recent valid data point
            latest_data = df.iloc[-1] if not df.empty else None
            if latest_data is None:
                print(f"No valid data available for {symbol} at interval {interval}.")
                return False

            latest_data = latest_data.apply(lambda x: self._convert_numpy_types(x))

            # Extract values for current data
            timestamp = latest_data.name.to_pydatetime()
            high = float(latest_data['High'])
            low = float(latest_data['Low'])
            close = float(latest_data['Close'])
            Open = float(latest_data['Open'])
            volume = float(latest_data['Volume']) if 'Volume' in latest_data else 0


            if len(df) > 1:
                prev_close = float(df.iloc[-2]['Close'])
                price_change = close - prev_close
                percent_change = (price_change / prev_close) * 100 if prev_close != 0 else 0
            else:
                price_change = None
                percent_change = None



            # Update database using a single query with upsert
            with self._get_cursor() as cur:
                upsert_sql = """
                    INSERT INTO stock_data_cache (
                        symbol, interval, timestamp, open, high, low, close, volume,
                        price_change, percent_change
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (symbol, interval)
                    DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
                        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                        close = EXCLUDED.close, volume = EXCLUDED.volume, price_change = EXCLUDED.price_change,
                        percent_change = EXCLUDED.percent_change
                """

                params = (
                    symbol,                                                    # 1
                    interval,                                                  # 2
                    self._convert_numpy_types(timestamp),                      # 3
                    self._convert_numpy_types(Open),                     # 4
                    self._convert_numpy_types(high),                           # 5
                    self._convert_numpy_types(low),                            # 6
                    self._convert_numpy_types(close),                          # 7
                    self._convert_numpy_types(volume),                         # 8

                    self._convert_numpy_types(price_change),                  # 16
                    self._convert_numpy_types(percent_change),                # 17

                )

                cur.execute(upsert_sql, params)

            return True
        except Exception as e:
            import traceback
            print(f"Error updating {symbol} data: {str(e)}")
            traceback.print_exc()
            return False


    def get_stock_data(self, symbol, interval):
        """Get cached stock data"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT data FROM stock_data_cache
                WHERE symbol = %s AND interval = %s
            """, (symbol, interval))
            result = cur.fetchone()
            return json.loads(result[0]) if result else None


    def _convert_numpy_types(self, value):
        """Convert numpy types to native Python types"""
        if isinstance(value, (np.floating, np.float32, np.float64)):
            return float(value)
        elif isinstance(value, (np.integer, np.int32, np.int64)):
            return int(value)
        elif isinstance(value, str):
            # Attempt to convert strings to float if possible
            return float(value) if value.replace('.', '', 1).isdigit() else value
        elif isinstance(value, np.ndarray):
            return value.tolist()
        elif isinstance(value, (list, tuple)):
            return [self._convert_numpy_types(x) for x in value]
        elif isinstance(value, dict):
            return {k: self._convert_numpy_types(v) for k, v in value.items()}
        elif isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        elif isinstance(value, Decimal):
            return float(value)
        return value

    def get_top_strikes(self, metric="volume", limit=10, offset=0):
        """Fetch top strikes based on a given metric (volume or pct_change)"""
        with self._get_cursor() as cur:
            cur.execute(f"""
                WITH ranked_strikes AS (
                    SELECT 
                        symbol, 
                        strike_price, 
                        option_type, 
                        {metric}, 
                        price,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY {metric} DESC) AS rank
                    FROM oi_volume_history
                    WHERE {metric} IS NOT NULL
                      AND option_type != 'FU'
                )
                SELECT symbol, strike_price, option_type, {metric}, price
                FROM ranked_strikes
                WHERE rank = 1
                ORDER BY {metric} DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))

            return [
                {
                    'symbol': row[0],
                    'strike_price': float(row[1]),
                    'option_type': row[2],
                    metric: float(row[3]) if row[3] is not None else 0.0,
                    'price': float(row[4]) if row[4] is not None else 0.0
                }
                for row in cur.fetchall()
            ]

    def save_instrument_keys(self, instruments):
        """Save instrument keys to database"""
        if not instruments:
            return

        records = []

        # Define index keys outside the loop
        index_keys = {
            "NIFTY": "NSE_INDEX|Nifty 50",
            "BANKNIFTY": "NSE_INDEX|Nifty Bank",
            "MIDCPNIFTY": "NSE_INDEX|Nifty Midcap 50",
            "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
            "SENSEX": "BSE_INDEX|SENSEX",
            "BANKEX": "BSE_INDEX|BANKEX"
        }

        # Process regular instruments first
        for inst in instruments:
            try:
                # Parse symbol and option details if applicable
                trading_symbol = inst.get('tradingsymbol', '')
                instrument_key = inst.get('instrument_key', '')
                exchange = inst.get('exchange', '')

                # For equity instruments, use trading symbol directly as the symbol
                if exchange == 'NSE_EQ':
                    symbol = trading_symbol
                # For futures and options, extract symbol from trading symbol
                elif exchange in ['NSE_FO', 'BSE_FO']:
                    # Check if it's a futures contract first (e.g., INFY25MAYFUT)
                    if trading_symbol.endswith('FUT'):
                        import re
                        fut_match = re.match(r"([A-Z]+)\d+[A-Z]{3}FUT", trading_symbol)
                        if fut_match:
                            symbol = fut_match.group(1)
                        else:
                            symbol = inst.get('name', trading_symbol)
                    else:
                        # Try to parse option details
                        parsed = self._parse_option_symbol(trading_symbol)
                        if parsed:
                            symbol = parsed['stock']
                        else:
                            symbol = inst.get('name', trading_symbol)
                # For indices or other instruments
                elif 'INDEX' in exchange:
                    symbol = inst.get('name', trading_symbol)
                else:
                    # Default fallback using name from instrument or tradingsymbol
                    symbol = inst.get('name', trading_symbol)

                # Truncate symbol if too long (50 chars limit in DB)
                if len(symbol) > 50:
                    symbol = symbol[:50]

                instrument_type = inst.get('instrument_type', 'EQ')
                option_type = None
                strike_price = None
                lot_size = inst.get('lot_size', 0)

                # Process expiry date - it could be in DD/MM/YY format from CSV
                expiry_date = inst.get('expiry')

                # Handle NaN or None values for expiry_date
                if expiry_date is None or (isinstance(expiry_date, float) and (pd.isna(expiry_date) or np.isnan(expiry_date))):
                    expiry_date = None
                # Standardize expiry date format if it exists and is valid
                elif expiry_date and not pd.isna(expiry_date):
                    try:
                        expiry_str = str(expiry_date)
                        if '/' in expiry_str:  # DD/MM/YY format
                            day, month, year = expiry_str.split('/')
                            if len(year) == 2:
                                year = f"20{year}"
                            expiry_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        elif '-' in expiry_str:  # Could be DD-MM-YYYY or YYYY-MM-DD
                            parts = expiry_str.split('-')
                            if len(parts[0]) == 4:  # YYYY-MM-DD
                                expiry_date = expiry_str
                            else:  # DD-MM-YYYY
                                day, month, year = parts
                                expiry_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    except Exception as e:
                        print(f"Error processing expiry date '{expiry_date}': {str(e)}")
                        expiry_date = None  # Set to None if there's any error

                # Set instrument type based on exchange
                if exchange in ['NSE_FO', 'BSE_FO']:
                    instrument_type = 'FO'

                    # Extract option details for FO instruments
                    if trading_symbol.endswith('FUT'):
                        option_type = 'FU'
                    else:
                        parsed = self._parse_option_symbol(trading_symbol)
                        if parsed:
                            option_type = parsed['option_type']
                            strike_price = parsed['strike_price']
                elif 'INDEX' in exchange:
                    instrument_type = 'INDEX'
                elif exchange == 'NSE_EQ':
                    instrument_type = 'EQUITY'

                # Only add valid records with both instrument_key and tradingsymbol
                if instrument_key and trading_symbol:
                    records.append((
                        symbol,
                        instrument_key,
                        exchange,
                        trading_symbol,
                        lot_size,
                        instrument_type,
                        expiry_date,  # This could be None now, which is fine for DATE fields
                        strike_price,
                        option_type
                    ))
            except Exception as e:
                print(f"Error processing instrument: {str(e)}")
                continue

        # Now add index instruments
        for index_name, instrument_key in index_keys.items():
            exchange = "BSE_INDEX" if "BSE_INDEX" in instrument_key else "NSE_INDEX"
            tradingsymbol = instrument_key.split('|')[1]

            records.append((
                index_name,  # Use the index name (NIFTY, BANKNIFTY, etc.) as the symbol
                instrument_key,
                exchange,
                tradingsymbol,
                0,  # lot_size (0 for indices)
                'INDEX',  # instrument_type
                None,  # expiry_date (None for indices)
                None,  # strike_price (None for indices)
                None   # option_type (None for indices)
            ))

        if records:
            with self._get_cursor() as cur:
                execute_batch(cur, """
                    INSERT INTO instrument_keys 
                    (symbol, instrument_key, exchange, tradingsymbol, lot_size, instrument_type, 
                     expiry_date, strike_price, option_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tradingsymbol, exchange) 
                    DO UPDATE SET
                        instrument_key = EXCLUDED.instrument_key,
                        lot_size = EXCLUDED.lot_size,
                        symbol = EXCLUDED.symbol,
                        instrument_type = EXCLUDED.instrument_type,
                        expiry_date = EXCLUDED.expiry_date,
                        strike_price = EXCLUDED.strike_price,
                        option_type = EXCLUDED.option_type,
                        last_updated = NOW()
                """, records, page_size=100)

    def get_instrument_keys(self, symbol=None, instrument_type=None, limit=300):
        """Get instrument keys from database"""
        with self._get_cursor() as cur:
            query = """
                SELECT symbol, instrument_key, exchange, tradingsymbol, lot_size, 
                       instrument_type, expiry_date, strike_price, option_type, prev_close
                FROM instrument_keys
                WHERE exchange IN ('NSE_EQ', 'NSE_INDEX', 'BSE_INDEX')
            """
            params = []

            if symbol:
                query += " AND symbol = %s"
                params.append(symbol)

            if instrument_type:
                query += " AND instrument_type = %s"
                params.append(instrument_type)

            query += " ORDER BY tradingsymbol LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            results = cur.fetchall()

            return [
                {
                    'symbol': row[0],
                    'instrument_key': row[1],
                    'exchange': row[2],
                    'tradingsymbol': row[3],
                    'lot_size': row[4],
                    'instrument_type': row[5],
                    'expiry_date': row[6].isoformat() if row[6] else None,
                    'strike_price': float(row[7]) if row[7] else None,
                    'option_type': row[8],
                    'prev_close': float(row[9]) if row[9] is not None else None
                }
                for row in results
            ]

    def _parse_option_symbol(self, symbol):
        """Parse option symbol into components"""
        try:
            # Try to match standard monthly pattern like RELIANCE24APR4000CE or NIFTY24APR24000CE
            import re
            match = re.match(r"([A-Z]+)(\d{2}[A-Z]{3})(\d+(?:\.\d+)?)(CE|PE)$", symbol)
            if match:
                return {
                    "stock": match.group(1),
                    "expiry": match.group(2),
                    "strike_price": float(match.group(3)),  # Convert to float to handle decimals
                    "option_type": match.group(4)
                }

            # For weekly options with specific 5-digit strike price handling
            weekly_indices = ["NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

            for index in weekly_indices:
                if symbol.startswith(index) and symbol.endswith(("CE", "PE")):
                    option_type = symbol[-2:]  # CE or PE
                    remaining = symbol[len(index):-2]  # Remove index name and option type

                    if len(remaining) < 9:  # Not enough chars for a weekly symbol, skip
                        continue

                    # For NIFTY indices:
                    # First 2 chars: YY (year)
                    # Next 1-2 chars: MM or M (month)
                    # Next 2 chars: DD (day)
                    # Remaining: Strike price (should be 5 digits for NIFTY, SENSEX)

                    # Extract year
                    year_digits = remaining[:2]

                    # Check if month is 1 or 2 digits
                    if remaining[2:4].isdigit() and 1 <= int(remaining[2:4]) <= 12:
                        # Two-digit month
                        month_digits = remaining[2:4]
                        remaining_after_month = remaining[4:]
                    else:
                        # One-digit month
                        month_digits = remaining[2:3]
                        remaining_after_month = remaining[3:]

                    # Extract day
                    day_digits = remaining_after_month[:2]

                    # Extract strike price - all remaining digits
                    strike_part = remaining_after_month[2:]

                    if not strike_part.isdigit():
                        # Not a valid strike price
                        continue

                    # For NIFTY/SENSEX, strike should be 5 digits
                    expected_strike_length = 5
                    if len(strike_part) != expected_strike_length and index in ["NIFTY", "SENSEX", "BANKNIFTY"]:
                        print(f"Warning: Unexpected strike length for {index}: {strike_part} (expected {expected_strike_length} digits)")
                        # Continue to try other patterns
                        continue

                    # Format the expiry as DDMMYY for consistency
                    expiry = f"{day_digits}{month_digits.zfill(2)}{year_digits}"

                    # Construct the result
                    result = {
                        "stock": index,
                        "expiry": expiry,
                        "strike_price": float(strike_part),
                        "option_type": option_type,
                        "is_weekly": True
                    }

                    return result

            # If no patterns matched, return None
            return None
        except Exception as e:
            print(f"Error parsing symbol {symbol}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_all_instrument_keys(self):
        """Fetch all instrument keys from the database."""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT instrument_key, tradingsymbol FROM instrument_keys
            """)
            return cur.fetchall()
    def update_instrument_keys_with_prev_close(self, prev_close_data):
        """
        Update the instrument keys table with previous close prices.
        :param prev_close_data: List of tuples (instrument_key, prev_close)
        """
        with self._get_cursor() as cur:
            execute_batch(cur, """
                UPDATE instrument_keys
                SET prev_close = %s, last_updated = NOW()
                WHERE instrument_key = %s
            """, [(data['prev_close'], data['instrument_key']) for data in prev_close_data], page_size=100)

    def get_fii_market_data(self, data_type):
        """Get cached market data"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT data FROM market_data_cache
                WHERE data_type = %s
            """, (data_type,))
            result = cur.fetchone()
            return result[0] if result else None

    def save_quarterly_financials(self, symbol, financials, next_earnings_date=None):
        """Save quarterly financial data to the database."""
        with self._get_cursor() as cur:
            # If we have quarterly financials, save them
            if financials:
                data = [
                    (
                        symbol,
                        f["quarter_ending"],
                        f["revenue"],  # Ensure revenue is included
                        f["net_income"],
                        f["operating_income"],
                        f["ebitda"],
                        f["type"]
                    )
                    for f in financials
                ]
                execute_batch(cur, """
                    INSERT INTO quarterly_financials (
                        symbol, quarter_ending, revenue, net_income, operating_income, ebitda, type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, quarter_ending) DO UPDATE SET
                        revenue = EXCLUDED.revenue,  -- Ensure revenue is updated
                        net_income = EXCLUDED.net_income,
                        operating_income = EXCLUDED.operating_income,
                        ebitda = EXCLUDED.ebitda,
                        type = EXCLUDED.type
                """, data)

            # Save next earnings date to stock_earnings table if provided
            if next_earnings_date:
                cur.execute("""
                    INSERT INTO stock_earnings (symbol, next_earnings_date)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        next_earnings_date = EXCLUDED.next_earnings_date
                """, (symbol, next_earnings_date))

    def save_quarterly_financials_batch(self, batch_results):
        """Save a batch of quarterly financial data more efficiently."""
        with self._get_cursor() as cur:
            # Process financial data
            financials_data = []
            earnings_data = []

            for result in batch_results:
                symbol = result["symbol"]
                financials = result.get("financials", [])
                next_earnings_date = result.get("next_earnings_date")

                # Prepare earnings data if available and handle different types
                if next_earnings_date:
                    try:
                        # Ensure earnings_date is properly converted to datetime
                        if isinstance(next_earnings_date, str):
                            next_earnings_date = pd.to_datetime(next_earnings_date)
                        elif hasattr(next_earnings_date, 'to_pydatetime'):
                            next_earnings_date = next_earnings_date.to_pydatetime()

                        # Add to earnings data
                        earnings_data.append((symbol, next_earnings_date))
                    except Exception as e:
                        print(f"Error converting earnings date for {symbol}: {e}")
                        # Simply skip this earnings date

                # Prepare financials data
                for fin in financials:
                    try:
                        quarter_ending = fin["quarter_ending"]
                        # Ensure quarter_ending is a datetime
                        if hasattr(quarter_ending, 'to_pydatetime'):
                            quarter_ending = quarter_ending.to_pydatetime()

                        financials_data.append((
                            symbol,
                            quarter_ending,
                            self._convert_numpy_types(fin["revenue"]),
                            self._convert_numpy_types(fin["net_income"]),
                            self._convert_numpy_types(fin["operating_income"]),
                            self._convert_numpy_types(fin["ebitda"]),
                            fin["type"]
                        ))
                    except Exception as e:
                        print(f"Error processing financial data for {symbol}: {e}")
                        # Skip this financial record

            # Batch insert quarterly financials
            if financials_data:
                execute_batch(cur, """
                    INSERT INTO quarterly_financials (
                        symbol, quarter_ending, revenue, net_income, operating_income, ebitda, type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, quarter_ending) DO UPDATE SET
                        revenue = EXCLUDED.revenue,
                        net_income = EXCLUDED.net_income,
                        operating_income = EXCLUDED.operating_income,
                        ebitda = EXCLUDED.ebitda,
                        type = EXCLUDED.type
                """, financials_data, page_size=100)

            # Batch insert earnings dates
            if earnings_data:
                execute_batch(cur, """
                    INSERT INTO stock_earnings (symbol, next_earnings_date)
                    VALUES (%s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        next_earnings_date = EXCLUDED.next_earnings_date
                """, earnings_data, page_size=100)

    def get_quarterly_financials(self, symbol, limit=5):
        """Retrieve the last `limit` quarters of financial data for a stock."""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT quarter_ending, revenue, net_income, operating_income, ebitda, type
                FROM quarterly_financials
                WHERE symbol = %s
                ORDER BY quarter_ending DESC
                LIMIT %s
            """, (symbol, limit))
            return cur.fetchall()

    def get_upcoming_financial_results(self):
        """Fetch upcoming financial results."""
        with self._get_cursor() as cur:
            # Query both the quarterly_financials table and stock_earnings table
            # to get upcoming results and scheduled earnings dates
            cur.execute("""
                SELECT se.symbol, se.next_earnings_date
                FROM stock_earnings se
                WHERE se.next_earnings_date > NOW()
                  AND se.next_earnings_date <= NOW() + INTERVAL '20 days'
                ORDER BY se.next_earnings_date ASC
            """)
            earnings_results = [
                {
                    "symbol": row[0],
                    "earnings_date": row[1].strftime('%Y-%m-%d') if row[1] else None
                }
                for row in cur.fetchall()
            ]

            # Also fetch upcoming quarterly financials
            cur.execute("""
                SELECT symbol, quarter_ending
                FROM quarterly_financials
                WHERE quarter_ending > NOW()
                ORDER BY quarter_ending ASC
            """)
            quarterly_results = [
                {"symbol": row[0], "quarter_ending": row[1].strftime('%Y-%m-%d')}
                for row in cur.fetchall()
            ]

            # Combine and deduplicate results
            combined_results = []
            symbols_added = set()

            # Add earnings results first (priority because they're more imminent)
            for result in earnings_results:
                combined_results.append(result)
                symbols_added.add(result["symbol"])

            # Add quarterly results that aren't already included
            for result in quarterly_results:
                if result["symbol"] not in symbols_added:
                    combined_results.append(result)
                    symbols_added.add(result["symbol"])

            return combined_results

    def get_declared_financial_results(self):
        """Fetch declared financial results."""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT symbol, quarter_ending, revenue, net_income, operating_income, ebitda
                FROM quarterly_financials
                ORDER BY quarter_ending DESC
            """)  # Removed any date-based filtering
            results = []
            for row in cur.fetchall():
                results.append({
                    "symbol": row[0],
                    "quarter_ending": row[1].strftime('%Y-%m-%d'),
                    "revenue": float(row[2]) if row[2] is not None else None,  # Replace NaN with None
                    "net_income": float(row[3]) if row[3] is not None else None,  # Replace NaN with None
                    "operating_income": float(row[4]) if row[4] is not None else None,  # Add operating_income
                    "ebitda": float(row[5]) if row[5] is not None else None  # Add ebitda
                })
            return results

    def get_market_data(self, data_type):
        """
        Get market data including indices, top gainers, and top losers from stock_data_cache

        Args:
            data_type: Type of market data to fetch ('indices', 'gainers', 'losers', or 'all')

        Returns:
            dict: Structured market data
        """
        print("inside get_market_data........")
        try:
            market_data = {}

            with self._get_cursor() as cur:
                # Fetch indices data (NIFTY, BANKNIFTY, etc.)
                cur.execute("""
                        SELECT symbol, close, price_change, percent_change, timestamp
                        FROM stock_data_cache
                        WHERE symbol IN ('NIFTY.NS', 'BANKNIFTY.NS', 'BANKEX.NS', 'SENSEX.NS', 'FINNIFTY.NS', 'MIDCPNIFTY.NS')
                        AND interval = '1d'
                        ORDER BY symbol
                    """)

                indices = []
                for row in cur.fetchall():
                    # Ensure we have valid numeric values
                    close = float(row[1]) if row[1] is not None else 0.0
                    price_change = float(row[2]) if row[2] is not None else 0.0
                    percent_change = float(row[3]) if row[3] is not None else 0.0

                    indices.append({
                        'symbol': row[0].replace('.NS', ''),
                        'close': close,
                        'price_change': price_change,
                        'percent_change': percent_change,
                        'timestamp': row[4].isoformat() if row[4] else None
                    })

                market_data['indices'] = indices

                # Fetch top gainers (stocks with highest positive percent change)
                cur.execute("""
                        SELECT symbol, close, price_change, percent_change, timestamp
                        FROM stock_data_cache
                        WHERE interval = '1d'
                        AND percent_change > 0
                        AND symbol NOT LIKE '%INDEX%'
                        ORDER BY percent_change DESC
                        LIMIT 10
                    """)

                gainers = []
                for row in cur.fetchall():
                    # Ensure we have valid numeric values
                    close = float(row[1]) if row[1] is not None else 0.0
                    price_change = float(row[2]) if row[2] is not None else 0.0
                    percent_change = float(row[3]) if row[3] is not None else 0.0

                    gainers.append({
                        'symbol': row[0].replace('.NS', ''),
                        'close': close,
                        'price_change': price_change,
                        'percent_change': percent_change,
                        'timestamp': row[4].isoformat() if row[4] else None
                    })

                market_data['gainers'] = gainers

                # Fetch top losers (stocks with highest negative percent change)
                cur.execute("""
                        SELECT symbol, close, price_change, percent_change, timestamp
                        FROM stock_data_cache
                        WHERE interval = '1d'
                        AND percent_change < 0
                        AND symbol NOT LIKE '%INDEX%'
                        ORDER BY percent_change ASC
                        LIMIT 10
                    """)

                losers = []
                for row in cur.fetchall():
                    # Ensure we have valid numeric values
                    close = float(row[1]) if row[1] is not None else 0.0
                    price_change = float(row[2]) if row[2] is not None else 0.0
                    percent_change = float(row[3]) if row[3] is not None else 0.0

                    losers.append({
                        'symbol': row[0].replace('.NS', ''),
                        'close': close,
                        'price_change': price_change,
                        'percent_change': percent_change,
                        'timestamp': row[4].isoformat() if row[4] else None
                    })

                market_data['losers'] = losers

            return market_data

        except Exception as e:
            import traceback
            print(f"Error fetching market data: {e}")
            traceback.print_exc()
            return None

    def clear_old_market_data(self):
        """Clear market data older than 1 day"""
        with self._get_cursor() as cur:
            cur.execute("""
                DELETE FROM market_data_cache
                WHERE last_updated < NOW() - INTERVAL '1 day'
            """)

    def save_buildup_results(self, results):
        """Save buildup analysis results to the database with proper conflict handling"""
        if not results:
            return

        data = []
        timestamp = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M')

        # Process buildup data
        for result_type, items in results.items():
            if result_type in ['futures_long_buildup', 'futures_short_buildup',
                               'options_long_buildup', 'options_short_buildup']:
                for item in items:
                    data.append((
                        item['symbol'],
                        'buildup',  # analytics_type
                        result_type.replace('_buildup', ''),  # category
                        item.get('strike', 0),
                        item.get('option_type', 'FUT'),
                        item.get('price_change', 0),
                        item.get('oi_change', 0),
                        item.get('volume_change', 0),
                        item.get('absolute_oi', 0),
                        item.get('timestamp', timestamp)
                    ))

        # Process OI extremes (if present in results)
        if 'oi_gainers' in results:
            for item in results['oi_gainers']:
                data.append((
                    item['symbol'],
                    'oi_analytics',
                    'oi_gainer',
                    item.get('strike', 0),
                    item.get('type', 'FUT'),
                    None,  # price_change
                    item['oi_change'],
                    None,  # volume_change
                    item['oi'],
                    item.get('timestamp', timestamp)
                ))

        if 'oi_losers' in results:
            for item in results['oi_losers']:
                data.append((
                    item['symbol'],
                    'oi_analytics',
                    'oi_loser',
                    item.get('strike', 0),
                    item.get('type', 'FUT'),
                    None,  # price_change
                    item['oi_change'],
                    None,  # volume_change
                    item['oi'],
                    item.get('timestamp', timestamp)
                ))

        with self._get_cursor() as cur:
            # Insert into fno_analytics
            execute_batch(cur, """
                INSERT INTO fno_analytics 
                (symbol, analytics_type, category, strike, option_type,
                 price_change, oi_change, volume_change, absolute_oi, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, analytics_type, category, strike, option_type, timestamp) 
                DO UPDATE SET
                    price_change = EXCLUDED.price_change,
                    oi_change = EXCLUDED.oi_change,
                    volume_change = EXCLUDED.volume_change,
                    absolute_oi = EXCLUDED.absolute_oi
            """, data, page_size=100)

    def get_buildup_results(self, limit=20):
        """Get recent buildup results"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT symbol, result_type, category, strike, option_type,
                       price_change, oi_change, volume_change, absolute_oi, timestamp
                FROM buildup_results
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))

            results = []
            for row in cur.fetchall():
                results.append({
                    'symbol': row[0],
                    'result_type': row[1],
                    'category': row[2],
                    'strike': float(row[3]),
                    'option_type': row[4],
                    'price_change': float(row[5]),
                    'oi_change': float(row[6]),
                    'volume_change': float(row[7]),
                    'absolute_oi': int(row[8]),
                    'timestamp': row[9]
                })
            return results

    @lru_cache(maxsize=100)
    def get_anchored_pivot_data(self, ticker, interval):
        """Cache yfinance data for pivot calculations to reduce API calls"""
        try:
            stock = yf.Ticker(ticker, session=self.session)

            # Determine anchor timeframe based on current interval
            if interval in ['5m', '15m']:
                # Intraday pivots use previous day's daily data
                return stock.history(period='2d', interval='1d')
            elif interval in ['30m', '1h', '1d']:
                # Daily pivots use previous month's monthly data
                return stock.history(period='2y', interval='1d')  # Increased to 2 years
            elif interval == '1wk':
                # Weekly pivots use previous quarter's data
                return stock.history(period='6mo', interval='3mo')

            # Default case - return None
            return None
        except Exception as e:
            print(f"Error fetching anchor data for {ticker}: {str(e)}")
            return None

    def update_index_data(self, symbol, interval, data):
        """
        Update index data with only price information, no indicators calculation.
        Used for indices like NIFTY, BANKNIFTY, etc. to save processing time.

        Args:
            symbol: The index symbol (e.g. 'NIFTY', 'BANKNIFTY')
            interval: Data interval (e.g. '1d', '1h')
            data: DataFrame with OHLCV data

        Returns:
            bool: Success status
        """
        try:
            # Convert to DataFrame if it's not already
            if not isinstance(data, pd.DataFrame):
                if isinstance(data, dict):
                    data = pd.DataFrame(data)
                else:
                    raise TypeError(f"Expected DataFrame or dict, got {type(data)}")

            # Make copy to avoid modifying original
            df = data.copy()

            # Ensure the index is datetime
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # Sort by date
            df = df.sort_index()

            # Take only the most recent valid data point
            latest_data = df.iloc[-1] if not df.empty else None
            if latest_data is None:
                print(f"No valid data available for index {symbol} at interval {interval}.")
                return False

            latest_data = latest_data.apply(lambda x: self._convert_numpy_types(x))

            # Extract only the price data we need
            timestamp = latest_data.name.to_pydatetime()
            high = float(latest_data['High'])
            low = float(latest_data['Low'])
            close = float(latest_data['Close'])
            Open = float(latest_data['Open'])
            volume = float(latest_data['Volume']) if 'Volume' in latest_data else 0

            # Calculate price change and percent change
            if len(df) > 1:
                prev_close = float(df.iloc[-2]['Close'])
                price_change = close - prev_close
                percent_change = (price_change / prev_close) * 100 if prev_close != 0 else 0
            else:
                price_change = 0
                percent_change = 0

            # Update database using a simplified query for indices
            with self._get_cursor() as cur:
                upsert_sql = """
                    INSERT INTO stock_data_cache (
                        symbol, interval, timestamp, open, high, low, close, volume,
                        price_change, percent_change
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (symbol, interval)
                    DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
                        open = EXCLUDED.open, 
                        high = EXCLUDED.high, 
                        low = EXCLUDED.low,
                        close = EXCLUDED.close, 
                        volume = EXCLUDED.volume,
                        price_change = EXCLUDED.price_change,
                        percent_change = EXCLUDED.percent_change,
                        last_updated = NOW()
                """

                params = (
                    symbol,
                    interval,
                    self._convert_numpy_types(timestamp),
                    self._convert_numpy_types(Open),
                    self._convert_numpy_types(high),
                    self._convert_numpy_types(low),
                    self._convert_numpy_types(close),
                    self._convert_numpy_types(volume),
                    self._convert_numpy_types(price_change),
                    self._convert_numpy_types(percent_change)
                )

                cur.execute(upsert_sql, params)

            return True
        except Exception as e:
            import traceback
            print(f"Error updating index {symbol} data: {str(e)}")
            traceback.print_exc()
            return False

    def update_stock_prices_batch(self, stock_data_list):
        """Update stock prices in bulk"""
        if not stock_data_list:
            return

        try:
            with self._get_cursor() as cur:
                # Prepare data for batch update
                records = []
                for data in stock_data_list:
                    symbol = data['symbol']
                    # Ensure symbol has .NS suffix
                    if not symbol.endswith('.NS'):
                        symbol = f"{symbol}.NS"

                    records.append((
                        symbol,
                        float(data['close']) if data['close'] is not None else 0.0,
                        float(data['price_change']) if data['price_change'] is not None else 0.0,
                        float(data['percent_change']) if data['percent_change'] is not None else 0.0,
                        data['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(data['timestamp'], datetime) else data['timestamp']
                    ))

                # Use execute_batch for efficient batch update
                execute_batch(cur, """
                    UPDATE stock_data_cache
                    SET close = %s, price_change = %s, percent_change = %s, last_updated = %s
                    WHERE symbol = %s
                """, [(r[1], r[2], r[3], r[4], r[0]) for r in records], page_size=100)

                rows_affected = cur.rowcount
                print(f"Updated {rows_affected} stock price records")
        except Exception as e:
            print(f"Error updating stock prices batch: {e}")
            import traceback
            traceback.print_exc()

    def get_option_instruments_by_symbol(self, symbol):
        """Get all option instrument keys for a specific symbol"""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        instrument_key, 
                        symbol, 
                        strike_price, 
                        expiry_date as expiry, 
                        option_type, 
                        prev_close as last_close
                    FROM instrument_keys 
                    WHERE symbol = %s 
                    AND (option_type = 'CE' OR option_type = 'PE')
                    """
                            , (symbol,))
                results = cur.fetchall()

                # Format the result as a list of dictionaries
                return [
                    {
                        'instrument_key': row[0],
                        'symbol': row[1],
                        'strike_price': float(row[2]) if row[2] else None,
                        'expiry': row[3].isoformat() if row[3] else None,
                        'option_type': row[4],
                        'last_close': float(row[5]) if row[5] else 0
                    }
                    for row in results
                ]
        except Exception as e:
            print(f"Error fetching option instruments: {str(e)}")
            return []

    def get_option_stock_symbols(self):
        """Get all unique stock symbols that have options available"""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT symbol 
                    FROM instrument_keys 
                    ORDER BY symbol
                    """
                            )
                results = cur.fetchall()
                return [row[0] for row in results]
        except Exception as e:
            print(f"Error fetching option stock symbols: {str(e)}")
            return []

    def get_all_symbols(self):
        """Get all unique stock symbols that have options available"""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT tradingsymbol 
                    FROM instrument_keys 
                    ORDER BY tradingsymbol
                    """
                            )
                results = cur.fetchall()
                return [row[0] for row in results]
        except Exception as e:
            print(f"Error fetching option stock symbols: {str(e)}")
            return []

    def get_instrument_key_by_symbol_and_exchange(self, symbol, exchange):
        """Get instrument key by symbol and exchange."""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        instrument_key, 
                        symbol, 
                        exchange, 
                        tradingsymbol,
                        prev_close as last_close,
                        lot_size
                    FROM instrument_keys
                    WHERE symbol = %s AND exchange = %s
                    LIMIT 1
                """, (symbol, exchange))
                result = cur.fetchone()
                if result:
                    return {
                        'instrument_key': result[0],
                        'symbol': result[1],
                        'exchange': result[2],
                        'tradingsymbol': result[3],
                        'last_close': float(result[4]) if result[4] else 0,
                        'lot_size': int(result[5]) if result[5] else 1  # Default lot size to 1 if not available
                    }
                return None
        except Exception as e:
            print(f"Error fetching instrument key: {str(e)}")
            return None

    def get_instrument_key_by_trading_symbol(self, tradingsymbol):
        """Get instrument key by trading symbol."""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        instrument_key, 
                        symbol, 
                        exchange, 
                        prev_close as last_close,
                        lot_size
                    FROM instrument_keys
                    WHERE tradingsymbol = %s
                    LIMIT 1
                """, (tradingsymbol,))  # Fixed: The comma is needed to make this a tuple parameter
                result = cur.fetchone()
                if result:
                    return {
                        'instrument_key': result[0],
                        'symbol': result[1],
                        'exchange': result[2],
                        'tradingsymbol': tradingsymbol,
                        'last_close': float(result[3]) if result[3] else 0,
                        'lot_size': int(result[4]) if result[4] else 1  # Default lot size to 1 if not available
                    }
                return None
        except Exception as e:
            print(f"Error fetching instrument key for trading symbol '{tradingsymbol}': {str(e)}")
            return None

    def get_instrument_key_by_key(self, instrument_key):
        """Get instrument details by instrument key."""
        try:
            with self._get_cursor() as cur:
                cur.execute("""
                    SELECT 
                        instrument_key, 
                        symbol, 
                        exchange, 
                        tradingsymbol,
                        prev_close as last_close
                    FROM instrument_keys
                    WHERE instrument_key = %s
                    LIMIT 1
                """, (instrument_key,))
                result = cur.fetchone()
                if result:
                    return {
                        'instrument_key': result[0],
                        'symbol': result[1],
                        'exchange': result[2],
                        'tradingsymbol': result[3],
                        'last_close': float(result[4]) if result[4] else 0
                    }
                return None
        except Exception as e:
            print(f"Error fetching instrument by key: {str(e)}")
            return None

    def get_option_instrument_key(self, symbol, expiry, strike, option_type):
        """Get option instrument key by symbol, expiry, strike and option type."""
        try:
            with self._get_cursor() as cur:
                # Convert expiry to date if it's a string
                if isinstance(expiry, str):
                    expiry_date = datetime.strptime(expiry, '%Y-%m-%d').date()
                else:
                    expiry_date = expiry

                cur.execute("""
                    SELECT 
                        instrument_key, 
                        symbol,
                        strike_price,
                        expiry_date,
                        option_type,
                        prev_close as last_close
                    FROM instrument_keys
                    WHERE symbol = %s 
                    AND expiry_date = %s 
                    AND strike_price = %s 
                    AND option_type = %s
                    LIMIT 1
                """, (symbol, expiry_date, strike, option_type))
                result = cur.fetchone()
                if result:
                    return {
                        'instrument_key': result[0],
                        'symbol': result[1],
                        'strike_price': float(result[2]) if result[2] else 0,
                        'expiry_date': result[3].isoformat() if result[3] else None,
                        'option_type': result[4],
                        'last_close': float(result[5]) if result[5] else 0
                    }
                return None
        except Exception as e:
            print(f"Error fetching option instrument key: {str(e)}")
            return None

    def get_upstox_accounts(self):
        """Get all upstox accounts from the database"""
        try:
            with self._get_cursor() as cursor:
                cursor.execute("""
                    SELECT id, api_key, api_secret, totp_secret, redirect_uri, username, password, 
                           access_token
                    FROM upstox_accounts
                """)

                results = cursor.fetchall()
                return [
                    {
                        'id': row[0],
                        'api_key': row[1],
                        'api_secret': row[2],
                        'totp_secret': row[3],
                        'redirect_uri': row[4],
                        'username': row[5],
                        'password': row[6],
                        'access_token': row[7]
                    }
                    for row in results
                ]
        except Exception as e:
            print(f"Error getting upstox accounts: {str(e)}")
            return []

    def update_upstox_token(self, api_key, token_data):
        """Update the access token for a specific account"""
        try:
            access_token = token_data.get('access_token')

            with self._get_cursor() as cursor:
                cursor.execute("""
                    UPDATE upstox_accounts
                    SET access_token = %s
                    WHERE api_key = %s
                """, (access_token, api_key))

                return True
        except Exception as e:
            print(f"Error updating upstox token: {str(e)}")
            return False

    def save_upstox_account(self, account_data):
        """Save a new upstox account or update an existing one"""
        try:
            api_key = account_data.get('api_key')
            api_secret = account_data.get('api_secret')
            totp_secret = account_data.get('totp_secret')
            redirect_uri = account_data.get('redirect_uri')
            username = account_data.get('username')
            password = account_data.get('password')
            access_token = account_data.get('access_token')

            if not api_key:
                print("API key is required")
                return False

            with self._get_cursor() as cursor:
                # Check if account exists
                cursor.execute("SELECT id FROM upstox_accounts WHERE api_key = %s", (api_key,))
                result = cursor.fetchone()

                if result:
                    # Update existing account
                    cursor.execute("""
                        UPDATE upstox_accounts
                        SET api_secret = %s,
                            totp_secret = %s,
                            redirect_uri = %s,
                            username = %s,
                            password = %s,
                            access_token = %s,
                            created_at = NOW()
                        WHERE api_key = %s
                    """, (
                        api_secret,
                        totp_secret,
                        redirect_uri,
                        username,
                        password,
                        access_token,
                        api_key
                    ))
                else:
                    # Insert new account
                    cursor.execute("""
                        INSERT INTO upstox_accounts
                        (api_key, api_secret, totp_secret, redirect_uri, username, password, access_token)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        api_key,
                        api_secret,
                        totp_secret,
                        redirect_uri,
                        username,
                        password,
                        access_token
                    ))

                return True
        except Exception as e:
            print(f"Error saving upstox account: {str(e)}")
            return False
