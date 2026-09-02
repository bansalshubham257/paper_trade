import asyncio
import json
import ssl
from datetime import datetime
import requests
import websockets
import os
from google.protobuf.json_format import MessageToDict
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import threading
import time
import uvicorn
from typing import List, Dict, Any, Optional
import concurrent.futures

from config import Config
from services.database import DatabaseService
import MarketDataFeed_pb2

# FastAPI app setup
app = FastAPI()

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared data structure
market_data: Dict[str, Dict[str, Any]] = {}
active_subscription: List[str] = []

# Keep track of the current WebSocket connection
current_websocket = None
websocket_lock = asyncio.Lock()
# Track active websocket connections by token
active_websocket_connections = {}

db_service = DatabaseService()

def get_market_data_feed_authorize_v3(bad_tokens=None):
    """Get authorization for market data feed with token fallback."""
    if bad_tokens is None:
        bad_tokens = set()
    # Load all tokens from DB and try them in order
    accounts = db_service.get_upstox_accounts()
    # Try last used ID first, then all others
    preferred_order = [5, 6, 4, 3, 2, 1]
    account_tokens = []
    for pid in preferred_order:
        for acc in accounts:
            if acc['id'] == pid and acc.get('access_token') and acc['access_token'] not in bad_tokens:
                account_tokens.append((acc['id'], acc['access_token']))
    # Also append any accounts not in preferred order
    for acc in accounts:
        if acc['id'] not in preferred_order and acc.get('access_token') and acc['access_token'] not in bad_tokens:
            account_tokens.append((acc['id'], acc['access_token']))

    last_error = None
    for acc_id, token in account_tokens:
        try:
            print(f"Using access token ending with ...{token[-4:]} from database (ID={acc_id})")
            headers = {'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
            url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
            response = requests.get(url=url, headers=headers, timeout=5)
            response.raise_for_status()
            result = response.json()
            if 'data' in result and 'authorized_redirect_uri' in result['data']:
                active_websocket_connections[token] = {
                    'uri': result['data']['authorized_redirect_uri'],
                    'websocket': None,
                    'created_at': time.time()
                }
                return result, token
            else:
                print(f"Token ID={acc_id}: No authorized_redirect_uri in response")
                last_error = "No authorized redirect URI"
        except requests.exceptions.HTTPError as e:
            print(f"Token ID={acc_id} failed: {e}")
            last_error = e
            continue
        except Exception as e:
            print(f"Token ID={acc_id} error: {e}")
            last_error = e
            continue

    # If all tokens failed, try Config fallback
    if Config.ACCESS_TOKEN:
        print("All DB tokens failed, falling back to Config.ACCESS_TOKEN")
        headers = {'Accept': 'application/json', 'Authorization': f'Bearer {Config.ACCESS_TOKEN}'}
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
        response = requests.get(url=url, headers=headers)
        result = response.json()
        if 'data' in result and 'authorized_redirect_uri' in result['data']:
            return result, Config.ACCESS_TOKEN

    raise Exception(f"All tokens failed. Last error: {last_error}")

def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = MarketDataFeed_pb2.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response

async def close_websocket_by_token(token):
    """Close WebSocket connection associated with a specific token."""
    if token in active_websocket_connections:
        conn_info = active_websocket_connections[token]
        websocket = conn_info.get('websocket')

        if websocket is not None:
            try:
                print(f"Closing existing WebSocket for token ending ...{token[-4:]}")
                await websocket.close(code=1000, reason="New connection requested")
                # Wait for connection to fully close
                await asyncio.sleep(1)
                print(f"Successfully closed WebSocket for token ending ...{token[-4:]}")
            except Exception as e:
                print(f"Error closing WebSocket for token {token[-4:]}: {e}")
            finally:
                # Update connection info
                active_websocket_connections[token]['websocket'] = None

async def close_existing_websocket():
    """Close any existing WebSocket connection."""
    global current_websocket

    async with websocket_lock:
        if current_websocket is not None:
            try:
                print("Closing existing WebSocket connection...")
                await current_websocket.close()
                # Wait a moment to ensure the connection is fully closed
                await asyncio.sleep(1)
                print("Existing WebSocket connection closed successfully")
            except Exception as e:
                print(f"Error closing existing WebSocket: {e}")
            finally:
                current_websocket = None

async def check_and_close_stale_connections():
    """Check for stale connections and close them."""
    now = time.time()
    tokens_to_check = list(active_websocket_connections.keys())

    for token in tokens_to_check:
        conn_info = active_websocket_connections[token]
        websocket = conn_info.get('websocket')
        created_at = conn_info.get('created_at', 0)

        # If connection is older than 6 hours
        if now - created_at > 21600:
            await close_websocket_by_token(token)
            # Clean up the dictionary to prevent memory leaks
            del active_websocket_connections[token]
            print(f"Removed stale connection for token ending ...{token[-4:]}")
        # Check if websocket is closed using exception-safe method
        elif websocket is not None:
            try:
                # For websockets library, we can send a ping to check if it's still open
                # If the connection is closed, this will raise an exception
                pong_waiter = await websocket.ping()
                await asyncio.wait_for(pong_waiter, timeout=2.0)
            except Exception:
                # Connection is dead, clean it up
                await close_websocket_by_token(token)
                del active_websocket_connections[token]
                print(f"Removed closed connection for token ending ...{token[-4:]}")

async def websocket_worker():
    """Main WebSocket connection handler."""
    global market_data, active_subscription, current_websocket

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    bad_tokens = set()

    while True:
        try:
            # Check and close any stale connections
            await check_and_close_stale_connections()

            # Close any existing WebSocket connection before creating a new one
            await close_existing_websocket()

            # Get authorization and token (skipping previously bad tokens)
            auth, current_token = get_market_data_feed_authorize_v3(bad_tokens)

            # Close any existing websocket with this token
            await close_websocket_by_token(current_token)

            uri = auth["data"]["authorized_redirect_uri"]

            print(f"Connecting to WebSocket at {uri}")

            try:
                websocket = await asyncio.wait_for(
                    websockets.connect(uri, ssl=ssl_context),
                    timeout=10
                )
            except Exception as e:
                error_str = str(e)
                if '403' in error_str or 'Forbidden' in error_str:
                    print(f"WebSocket 403 for token ...{current_token[-4:]}, marking as bad and retrying")
                    bad_tokens.add(current_token)
                    await asyncio.sleep(2)
                    continue
                raise

            async with websocket:
                async with websocket_lock:
                    current_websocket = websocket
                    # Store in active connections
                    if current_token in active_websocket_connections:
                        active_websocket_connections[current_token]['websocket'] = websocket
                        active_websocket_connections[current_token]['created_at'] = time.time()

                print("WebSocket connected successfully")

                # Send a ping to make sure the connection is working
                try:
                    await websocket.ping()
                    print("WebSocket ping successful")
                except Exception as e:
                    print(f"WebSocket ping failed: {e}")
                    raise

                # Initialize a local subscription list to track what we've already subscribed to
                local_subscribed = []

                while True:
                    # Only send subscription if there are new instruments in active_subscription
                    subscription_to_send = [item for item in active_subscription if item not in local_subscribed]

                    if subscription_to_send:
                        print(f"New subscription requested: {subscription_to_send}")
                        subscription_data = {
                            "guid": str(time.time()),
                            "method": "sub",
                            "data": {
                                "mode": "full",
                                "instrumentKeys": subscription_to_send
                            }
                        }
                        print(f"Sending subscription request for {len(subscription_to_send)} instruments...")
                        await websocket.send(json.dumps(subscription_data).encode('utf-8'))

                        # Add these instruments to our local tracking
                        local_subscribed.extend(subscription_to_send)
                        print(f"Total subscribed instruments: {len(local_subscribed)}")

                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=25)
                        decoded = decode_protobuf(message)
                        data = MessageToDict(decoded)
                        for instrument, feed in data.get("feeds", {}).items():
                            full_feed = feed.get("fullFeed", {})
                            market_ff = full_feed.get("marketFF", {})
                            index_ff = full_feed.get("indexFF", {})

                            ltpc = market_ff.get("ltpc", index_ff.get("ltpc", {}))
                            oi = market_ff.get("oi", 0)

                            volume = 0
                            market_ohlc = market_ff.get("marketOHLC", {}).get("ohlc", [])
                            if market_ohlc:
                                for ohlc_data in market_ohlc:
                                    if ohlc_data.get("interval") == "1d":
                                        volume = int(ohlc_data.get("vol", 0))
                                        break
                                if volume == 0 and market_ohlc:
                                    volume = int(market_ohlc[0].get("vol", 0))
                            if volume == 0:
                                volume = int(market_ff.get("vtt", 0))

                            bid_ask_quote = market_ff.get("marketLevel", {}).get("bidAskQuote", [{}])
                            bidQ = max([int(quote.get("bidQ", 0) or 0) for quote in bid_ask_quote[:5]]) if bid_ask_quote else 0
                            askQ = max([int(quote.get("askQ", 0) or 0) for quote in bid_ask_quote[:5]]) if bid_ask_quote else 0

                            market_data[instrument] = {
                                "ltp": ltpc.get("ltp"),
                                "volume": volume,
                                "oi": oi,
                                "bidQ": bidQ,
                                "askQ": askQ
                            }
                    except asyncio.TimeoutError:
                        # Send a heartbeat to keep the connection alive
                        try:
                            await websocket.ping()
                        except Exception as e:
                            print(f"Heartbeat ping failed: {e}")
                            raise  # Re-raise to trigger reconnection
                    except Exception as e:
                        print(f"WebSocket connection error: {e}")
                        raise  # Re-raise to trigger reconnection

        except Exception as e:
            print(f"Connection error: {e}, reconnecting in 5 seconds...")

            # Make sure to clean up the current websocket reference
            async with websocket_lock:
                current_websocket = None

            await asyncio.sleep(5)

@app.get("/api/market_data")
async def get_market_data(request: Request):
    """Fetch and return market data for requested instruments."""
    global active_subscription

    requested_keys = request.query_params.getlist('keys')
    if not requested_keys:
        raise HTTPException(status_code=400, detail="No keys specified")

    # Add new instruments to the active_subscription list
    new_instruments = [key for key in requested_keys if key not in active_subscription]
    if new_instruments:
        active_subscription.extend(new_instruments)
        print(f"Added {len(new_instruments)} new instruments to subscription: {new_instruments}")

    timeout = 30
    start_time = time.time()
    while time.time() - start_time < timeout:
        if all(key in market_data for key in requested_keys):
            break
        time.sleep(0.5)

    return {key: market_data.get(key, {}) for key in requested_keys}

@app.get("/api/live_indices")
async def get_live_indices():
    """Fetch live data for static indices."""
    global active_subscription

    static_indices = {
        "NIFTY": "NSE_INDEX|Nifty 50",
        "BANKNIFTY": "NSE_INDEX|Nifty Bank",
        "MIDCPNIFTY": "NSE_INDEX|Nifty Midcap 50",
        "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
        "SENSEX": "BSE_INDEX|SENSEX",
        "BANKEX": "BSE_INDEX|BANKEX"
    }

    active_subscription = list(static_indices.values())

    timeout = 30
    start_time = time.time()
    while time.time() - start_time < timeout:
        if all(key in market_data for key in active_subscription):
            break
        time.sleep(0.5)

    indices = []
    for index, key in static_indices.items():
        data = market_data.get(key, {})
        ltp = data.get("ltp", 0) or 0

        instrument = db_service.get_instrument_key_by_key(key)
        prev_close = instrument.get('last_close', 0) if instrument else 0

        price_change = ltp - prev_close
        percent_change = (price_change / prev_close * 100) if prev_close != 0 else 0

        indices.append({
            "symbol": index,
            "close": ltp,
            "price_change": price_change,
            "percent_change": percent_change
        })

    return {"indices": indices}

@app.get('/api/latest_price')
async def get_latest_price(instrument_key: str):
    """Fetch the latest price for a given instrument key."""
    global active_subscription

    if not instrument_key:
        raise HTTPException(status_code=400, detail="Instrument key is required")

    # Fetch the last close price from the database
    try:
        instrument = db_service.get_instrument_key_by_key(instrument_key)
        if not instrument:
            raise HTTPException(status_code=404, detail=f"No instrument found for key: {instrument_key}")
        last_close = instrument.get('last_close', 0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching last close: {str(e)}")

    # Update the active subscription
    active_subscription = [instrument_key]

    # Wait for the WebSocket to process the subscription and fetch data
    timeout = 30  # Maximum wait time in seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check if data for the requested key is available
        if instrument_key in market_data:
            break
        time.sleep(0.5)  # Wait for 500ms before checking again

    # Fetch the latest data for the instrument key
    data = market_data.get(instrument_key, {})
    if not data:
        raise HTTPException(status_code=404, detail="No data available for the given instrument key")

    # Prepare the response
    ltp = data.get("ltp", 0)
    price_change = ltp - last_close
    percent_change = (price_change / last_close * 100) if last_close != 0 else 0

    return {
        "ltp": ltp,
        "last_close": last_close,
        "price_change": price_change,
        "percent_change": percent_change
    }

@app.get('/api/latest-option-price')
async def fetch_latest_option_price(symbol: str, expiry: str, strike: str, optionType: str):
    """Fetch the latest price for a specific option, strike, and option type."""
    global active_subscription

    if not symbol or not expiry or not strike or not optionType:
        raise HTTPException(status_code=400, detail="Symbol, expiry, strike, and optionType are required")

    try:
        # Fetch the instrument key for the given parameters
        instrument = db_service.get_option_instrument_key(symbol, expiry, strike, optionType)
        if not instrument:
            raise HTTPException(status_code=404, detail="Instrument key not found for the given parameters")

        instrument_key = instrument['instrument_key']
        prev_close = instrument.get('last_close', 0)

        # Update the active subscription
        active_subscription = [instrument_key]

        # Wait for the WebSocket to process the subscription and fetch data
        timeout = 30  # Maximum wait time in seconds
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check if data for the requested key is available
            if instrument_key in market_data:
                break
            time.sleep(0.5)  # Wait for 500ms before checking again

        # Fetch the latest data for the instrument key
        data = market_data.get(instrument_key, {})
        if not data:
            raise HTTPException(status_code=404, detail="No data available for the given instrument key")

        # Prepare the response
        ltp = data.get("ltp", 0)
        price_change = ltp - prev_close
        percent_change = (price_change / prev_close * 100) if prev_close != 0 else 0

        return {
            "price": ltp,
            "prev_close": prev_close,
            "price_change": price_change,
            "percent_change": percent_change,
            "timestamp": time.time()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching latest price: {str(e)}")

@app.get('/api/live_oi_volume')
async def fetch_live_oi_volume(symbol: str, expiry: str, strike: str, optionType: str):
    """Fetch live OI and volume for a specific option."""
    global active_subscription

    if not symbol or not expiry or not strike or not optionType:
        raise HTTPException(status_code=400, detail="Symbol, expiry, strike, and optionType are required")

    try:
        # Fetch the instrument key for the given parameters
        instrument = db_service.get_option_instrument_key(symbol, expiry, strike, optionType)
        if not instrument:
            raise HTTPException(status_code=404, detail="Instrument key not found for the given parameters")

        instrument_key = instrument['instrument_key']

        # Update the active subscription
        active_subscription = [instrument_key]

        # Wait for the WebSocket to process the subscription and fetch data
        timeout = 30  # Maximum wait time in seconds
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check if data for the requested key is available
            if instrument_key in market_data:
                break
            time.sleep(0.5)  # Wait for 500ms before checking again

        # Fetch the latest data for the instrument key
        data = market_data.get(instrument_key, {})
        if not data:
            raise HTTPException(status_code=404, detail="No data available for the given instrument key")

        # Prepare the response with correct OI and volume values
        return {
            "oi": data.get("oi", 0),
            "volume": data.get("volume", 0),
            "timestamp": time.time()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching live OI and volume: {str(e)}")

@app.get('/api/instruments')
async def get_instrument_key(symbol: str, exchange: str):
    """Fetch the instrument key for a specific symbol and exchange."""
    if not symbol or not exchange:
        raise HTTPException(status_code=400, detail="Symbol and exchange parameters are required")

    try:
        # Fetch the instrument key for the given symbol and exchange from the database
        instrument = db_service.get_instrument_key_by_symbol_and_exchange(symbol, exchange)
        if not instrument:
            raise HTTPException(status_code=404,
                                detail=f"No instrument key found for symbol: {symbol} and exchange: {exchange}")

        return {
            'symbol': instrument['symbol'],
            'instrument_key': instrument['instrument_key'],
            'exchange': instrument['exchange'],
            'tradingsymbol': instrument['tradingsymbol']
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/api/trading_instruments')
async def get_trading_instruments_key(symbol: str):
    """Fetch the instrument key for a specific symbol and exchange."""
    if not symbol:
        raise HTTPException(status_code=400, detail="Symbol parameter is required")

    try:
        instrument = db_service.get_instrument_key_by_trading_symbol(symbol)

        if not instrument:
            raise HTTPException(status_code=404,
                                detail=f"No instrument key found for symbol: {symbol} ")

        return {
            'symbol': instrument['symbol'],
            'instrument_key': instrument['instrument_key'],
            'exchange': instrument['exchange'],
            'tradingsymbol': instrument['tradingsymbol'],
            'last_close': instrument.get('last_close', 0),
            'lot_size': instrument.get('lot_size', 1)  # Add lot_size to the response
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/api/option_instruments')
async def get_option_instruments(symbol: str):
    """Get all option instrument keys for a specific stock."""
    if not symbol:
        raise HTTPException(status_code=400, detail="Symbol parameter is required")

    try:
        # Fetch all option instruments for the given symbol from the database
        instruments = db_service.get_option_instruments_by_symbol(symbol)
        if not instruments or len(instruments) == 0:
            raise HTTPException(status_code=404, detail=f"No option instruments found for symbol: {symbol}")

        # Group instruments by expiry and strike
        grouped_instruments = {}
        for instrument in instruments:
            expiry = instrument.get('expiry', '')
            strike = instrument.get('strike_price', '')
            option_type = instrument.get('option_type', '')

            if expiry not in grouped_instruments:
                grouped_instruments[expiry] = {}

            if strike not in grouped_instruments[expiry]:
                grouped_instruments[expiry][strike] = {}

            grouped_instruments[expiry][strike][option_type] = instrument.get('instrument_key', '')

        return {
            'symbol': symbol,
            'instruments': grouped_instruments
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/api/multiple_oi_volume')
async def fetch_multiple_oi_volume(instrument_keys: str):
    """Fetch OI and volume for multiple instrument keys at once."""
    global active_subscription

    if not instrument_keys:
        raise HTTPException(status_code=400, detail="instrument_keys parameter is required")

    # Split into a list
    instrument_keys_list = instrument_keys.split(',')

    # Update the active subscription
    active_subscription = instrument_keys_list

    # Wait for the WebSocket to process the subscription and fetch data
    timeout = 30  # Maximum wait time in seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check if data for all requested keys is available
        if all(key in market_data for key in instrument_keys_list):
            break
        time.sleep(0.5)  # Wait for 500ms before checking again

    # Prepare the response with OI and volume data for all instruments
    result = {}
    for key in instrument_keys_list:
        data = market_data.get(key, {})
        result[key] = {
            "oi": data.get("oi", 0),
            "volume": data.get("volume", 0),
            "ltp": data.get("ltp", 0)
        }

    return result


@app.get("/api/fno_stocks")
async def get_fno_stocks():
    """Fetch all available stock symbols for which options are available."""
    try:
        stocks = db_service.get_option_stock_symbols()
        if not stocks:
            raise HTTPException(status_code=404, detail="No stocks found with option instruments")
        return stocks
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks")
async def get_stocks():
    """Fetch all available stock symbols for which options are available."""
    try:
        stocks = db_service.get_all_symbols()
        if not stocks:
            raise HTTPException(status_code=404, detail="No stocks found with option instruments")
        return stocks
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/watchlist_data")
async def get_watchlist_data(symbols: str):
    """Fetch live price data for watchlist symbols."""
    global active_subscription

    if not symbols:
        raise HTTPException(status_code=400, detail="No symbols specified")

    symbol_list = symbols.split(',')

    # Limit the number of symbols that can be processed in a single request
    if len(symbol_list) > 50:
        raise HTTPException(status_code=400, detail="Too many symbols requested. Maximum is 50 per request.")

    print(f"Processing watchlist data request for {len(symbol_list)} symbols")

    # Use concurrent processing for fetching instrument keys
    instrument_keys = []
    instrument_details = {}

    # Define a function to fetch a single instrument key
    def fetch_instrument_key(symbol):
        try:
            # Try with trading symbol first
            instrument = db_service.get_instrument_key_by_trading_symbol(symbol)

            if instrument:
                return {
                    'symbol': symbol,
                    'found': True,
                    'instrument_key': instrument['instrument_key'],
                    'lot_size': instrument.get('lot_size', 1),
                    'last_close': instrument.get('last_close', 0)
                }
            else:
                # If not found by trading symbol, try by symbol and exchange
                instrument = db_service.get_instrument_key_by_symbol_and_exchange(symbol, "NSE_EQ")
                if instrument:
                    return {
                        'symbol': symbol,
                        'found': True,
                        'instrument_key': instrument['instrument_key'],
                        'lot_size': instrument.get('lot_size', 1),
                        'last_close': instrument.get('last_close', 0)
                    }

                return {
                    'symbol': symbol,
                    'found': False
                }
        except Exception as e:
            print(f"Error getting instrument key for {symbol}: {e}")
            return {
                'symbol': symbol,
                'found': False,
                'error': str(e)
            }

    # Use batch lookup if available in db_service
    try:
        # First try batch lookup
        batch_results = db_service.batch_get_instrument_keys(symbol_list)
        if batch_results:
            print(f"Successfully used batch lookup for {len(batch_results)} symbols")

            for symbol, data in batch_results.items():
                if data and 'instrument_key' in data:
                    instrument_keys.append(data['instrument_key'])
                    instrument_details[symbol] = {
                        'instrument_key': data['instrument_key'],
                        'lot_size': data.get('lot_size', 1),
                        'last_close': float(data.get('last_close', 0))  # Convert Decimal to float
                    }
        else:
            # Fall back to concurrent processing if batch lookup is not implemented
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(symbol_list))) as executor:
                results = list(executor.map(fetch_instrument_key, symbol_list))

                for result in results:
                    if result['found']:
                        symbol = result['symbol']
                        instrument_keys.append(result['instrument_key'])
                        instrument_details[symbol] = {
                            'instrument_key': result['instrument_key'],
                            'lot_size': result.get('lot_size', 1),
                            'last_close': float(result.get('last_close', 0))  # Convert Decimal to float
                        }
    except AttributeError:
        # If batch_get_instrument_keys is not implemented, use concurrent processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(symbol_list))) as executor:
            results = list(executor.map(fetch_instrument_key, symbol_list))

            for result in results:
                if result['found']:
                    symbol = result['symbol']
                    instrument_keys.append(result['instrument_key'])
                    instrument_details[symbol] = {
                        'instrument_key': result['instrument_key'],
                        'lot_size': result.get('lot_size', 1),
                        'last_close': float(result.get('last_close', 0))  # Convert Decimal to float
                    }

    # If we don't have any valid instrument keys, return empty result
    if not instrument_keys:
        return {}

    # Update the active subscription with all the instrument keys
    active_subscription = instrument_keys
    print(f"Updating active subscription with {len(instrument_keys)} instrument keys")

    # Wait for data to be fetched - use a more efficient approach
    timeout = 25  # 25 seconds timeout
    start_time = time.time()

    # Check more frequently at the beginning, then back off
    check_intervals = [0.05, 0.1, 0.2, 0.5, 1.0, 2.0]
    check_index = 0
    min_wait_time = 0.05

    while time.time() - start_time < timeout:
        # Count how many instruments we have data for
        available_count = sum(1 for key in instrument_keys if key in market_data)

        # If we have data for all instruments or at least 75% of them after 5 seconds, return what we have
        elapsed = time.time() - start_time
        if available_count == len(instrument_keys) or (elapsed > 5 and available_count >= len(instrument_keys) * 0.75):
            break

        # Use adaptive wait times that increase over time
        check_index = min(check_index, len(check_intervals) - 1)
        wait_time = check_intervals[check_index]
        check_index += 1

        # Don't wait less than min_wait_time
        await asyncio.sleep(max(min_wait_time, wait_time))

    # Prepare the result data - use dictionary comprehension for efficiency
    result = {}
    for symbol in symbol_list:
        details = instrument_details.get(symbol)
        if not details:
            continue

        instrument_key = details['instrument_key']
        lot_size = details['lot_size']
        last_close = details['last_close']  # This is now a float

        if instrument_key and instrument_key in market_data:
            data = market_data.get(instrument_key, {})

            # Calculate price_change and percent_change
            ltp = data.get("ltp", 0) or 0
            price_change = ltp - last_close  # No more type error
            percent_change = (price_change / last_close * 100) if last_close != 0 else 0

            result[symbol] = {
                "ltp": ltp,
                "last_close": last_close,
                "price_change": price_change,
                "percent_change": percent_change,
                "volume": data.get("volume", 0),
                "timestamp": time.time(),
                "lot_size": lot_size  # Include lot_size in the response
            }

    # Print performance stats
    processing_time = time.time() - start_time
    found_count = sum(1 for sym_data in result.values() if sym_data.get("ltp", 0) > 0)
    print(f"Processed watchlist data: {found_count}/{len(symbol_list)} symbols in {processing_time:.2f}s")

    return result

@app.get("/api/batch_trading_instruments")
async def get_batch_trading_instruments(symbols: str):
    """Fetch instrument keys for multiple trading symbols in a single request."""
    if not symbols:
        raise HTTPException(status_code=400, detail="Symbols parameter is required")

    symbols_list = symbols.split(',')
    print(f"Processing batch request for {len(symbols_list)} symbols: {symbols}")

    result = {}
    not_found = []

    try:
        # Use a more efficient batch query if available in your database service
        for symbol in symbols_list:
            instrument = db_service.get_instrument_key_by_trading_symbol(symbol)
            if instrument:
                result[symbol] = {
                    'symbol': instrument['symbol'],
                    'instrument_key': instrument['instrument_key'],
                    'exchange': instrument['exchange'],
                    'tradingsymbol': instrument['tradingsymbol'],
                    'last_close': instrument.get('last_close', 0),
                    'lot_size': instrument.get('lot_size', 1)
                }
            else:
                not_found.append(symbol)

        return {
            "success": True,
            "found": result,
            "not_found": not_found
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching batch instrument data: {str(e)}")

# API endpoint to manually close and restart the WebSocket connection
@app.post("/api/restart_websocket")
async def restart_websocket():
    """Manually close and restart the WebSocket connection."""
    try:
        await close_existing_websocket()
        return {"message": "WebSocket connection closed and will be restarted automatically"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error restarting WebSocket: {str(e)}")

@app.post("/api/restart_all_websockets")
async def restart_all_websockets():
    """Manually close all WebSocket connections and restart."""
    try:
        # Get all tokens
        tokens = list(active_websocket_connections.keys())

        # Close each connection
        for token in tokens:
            await close_websocket_by_token(token)

        # Also close the current websocket
        await close_existing_websocket()

        return {"message": f"Closed {len(tokens)} WebSocket connections. New connections will be established automatically."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error restarting WebSockets: {str(e)}")

@app.get("/api/options-orders-analysis")
async def get_options_orders_analysis():
    """Fetch options orders with live market data."""
    try:
        # First get all options orders from the database
        options_orders = db_service.get_full_options_orders()
        if not options_orders:
            return {"data": []}

        # Get all instrument keys from the options orders
        instrument_keys = [order['instrument_key'] for order in options_orders if order.get('instrument_key')]

        # If we have instrument keys, fetch live market data for them
        if instrument_keys:
            # Update the active subscription
            global active_subscription
            active_subscription = instrument_keys

            # Wait for data to be available (with timeout)
            timeout = 90  # seconds
            start_time = time.time()

            while time.time() - start_time < timeout:
                # Check if we have data for all requested keys
                if all(key in market_data for key in instrument_keys):
                    break
                await asyncio.sleep(0.5)

        # Prepare the response by combining database and live data
        response_data = []
        orders_to_update = []  # Track orders that need status update

        for order in options_orders:
            instrument_key = order.get('instrument_key')
            live_data = market_data.get(instrument_key, {}) if instrument_key else {}

            # Calculate days captured from timestamp
            days_captured = 'N/A'
            if order.get('timestamp'):
                try:
                    # Parse the timestamp as UTC
                    capture_date = datetime.fromisoformat(order['timestamp'].replace('Z', '+00:00'))

                    # Make sure current_date is also timezone-aware (UTC)
                    current_date = datetime.now(capture_date.tzinfo)

                    # Now both dates have timezone info, we can safely subtract
                    days_captured = (current_date - capture_date).days
                except Exception as e:
                    print(f"Error calculating days captured: {e}")

            # Explicitly convert values to float to avoid decimal.Decimal vs float issues
            try:
                stored_ltp = float(order.get('ltp', 0) or 0)
                current_ltp = float(live_data.get('ltp', stored_ltp) or stored_ltp)

                percent_change = 0
                if stored_ltp and stored_ltp != 0:
                    percent_change = ((current_ltp - stored_ltp) / stored_ltp) * 100
            except (TypeError, ValueError) as e:
                print(f"Error calculating percent change: {e}, stored_ltp={order.get('ltp')}, current_ltp={live_data.get('ltp')}")
                stored_ltp = 0
                current_ltp = 0
                percent_change = 0

            # Calculate today's return using prev_close
            todays_return = 0
            try:
                prev_close = float(order.get('prev_close', 0) or 0)
                if prev_close and prev_close > 0:
                    todays_return = ((current_ltp - prev_close) / prev_close) * 100
            except (TypeError, ValueError) as e:
                print(f"Error calculating today's return: {e}, prev_close={order.get('prev_close')}, current_ltp={current_ltp}")
                todays_return = 0

            # Get live OI and volume from market data
            live_oi = float(live_data.get('oi', 0) or 0)
            live_volume = float(live_data.get('volume', 0) or 0)

            # Get original OI and volume
            original_oi = float(order.get('oi', 0) or 0)
            original_volume = float(order.get('volume', 0) or 0)

            # Calculate OI and volume changes
            oi_change = ((live_oi - original_oi) / original_oi * 100) if original_oi != 0 else 0
            volume_change = ((live_volume - original_volume) / original_volume * 100) if original_volume != 0 else 0

            # Get current and original greek values
            original_iv = float(order.get('iv', 0) or 0)
            original_delta = float(order.get('delta', 0) or 0)
            original_gamma = float(order.get('gamma', 0) or 0)
            original_theta = float(order.get('theta', 0) or 0)
            original_vega = float(order.get('vega', 0) or 0)

            import pytz
            ist = pytz.timezone('Asia/Kolkata')

            # Get current greek values - these would typically come from a live options pricing API
            # For now, we'll use the stored values
            current_iv = float(order.get('current_iv', original_iv) or original_iv)
            current_delta = float(order.get('current_delta', original_delta) or original_delta)
            current_gamma = float(order.get('current_gamma', original_gamma) or original_gamma)
            current_theta = float(order.get('current_theta', original_theta) or original_theta)
            current_vega = float(order.get('current_vega', original_vega) or original_vega)

            # Calculate greek changes
            iv_change = ((current_iv - original_iv) / original_iv * 100) if original_iv != 0 else 0
            delta_change = ((current_delta - original_delta) / original_delta * 100) if original_delta != 0 else 0
            gamma_change = ((current_gamma - original_gamma) / original_gamma * 100) if original_gamma != 0 else 0
            theta_change = ((current_theta - original_theta) / original_theta * 100) if original_theta != 0 else 0
            vega_change = ((current_vega - original_vega) / original_vega * 100) if original_vega != 0 else 0

            # Check if status should be "Done" (> 100% change)
            current_status = order.get('status', 'Open')

            # Get current less than flags and initialize recovery flags
            is_less_than_25pct = order.get('is_less_than_25pct', False)
            is_less_than_50pct = order.get('is_less_than_50pct', False)
            is_less_than_75pct = order.get('is_less_than_75pct', False)
            is_greater_than_25pct = order.get('is_greater_than_25pct', False)
            is_greater_than_50pct = order.get('is_greater_than_50pct', False)
            is_greater_than_75pct = order.get('is_greater_than_75pct', False)

            # Track lowest price point
            lowest_point = order.get('lowest_point', min(current_ltp, stored_ltp))
            if current_ltp < lowest_point:
                lowest_point = current_ltp

            # Check conditions for updating
            need_update = False

            if percent_change > 90 and current_status != 'Done':
                current_status = 'Done'  # Update for response
                need_update = True

            # Calculate if price is less than 25% or 50% of original price
            if current_ltp < (stored_ltp * 0.25) and not is_less_than_25pct:
                is_less_than_25pct = True
                need_update = True

            if current_ltp < (stored_ltp * 0.75) and not is_less_than_75pct:
                is_less_than_75pct = True
                need_update = True

            if current_ltp < (stored_ltp * 0.50) and not is_less_than_50pct:
                is_less_than_50pct = True
                need_update = True

            if current_ltp > (stored_ltp * 1.25) and not is_greater_than_25pct:
                is_greater_than_25pct = True
                need_update = True

            if current_ltp > (stored_ltp * 1.50) and not is_greater_than_50pct:
                is_greater_than_50pct = True
                need_update = True

            if current_ltp > (stored_ltp * 1.75) and not is_greater_than_75pct:
                is_greater_than_75pct = True
                need_update = True

            # Hit recovery detection: mark as hit when recovery >= 33% after having dropped below thresholds
            is_hit = order.get('is_hit', False)
            hit_peak_pct = float(order.get('hit_peak_pct', 0) or 0)
            has_dropped_below = is_less_than_25pct or is_less_than_50pct
            if has_dropped_below and percent_change >= 33 and not is_hit:
                is_hit = True
                hit_peak_pct = percent_change
                need_update = True
            elif is_hit and percent_change > hit_peak_pct:
                hit_peak_pct = percent_change
                need_update = True

            # Track all-time high/low LTP since order insertion
            todays_max_ltp = order.get('todays_max_ltp')
            todays_min_ltp = order.get('todays_min_ltp')
            if todays_max_ltp is None or current_ltp > todays_max_ltp:
                todays_max_ltp = current_ltp
                need_update = True
            if todays_min_ltp is None or current_ltp < todays_min_ltp:
                todays_min_ltp = current_ltp
                need_update = True

            # Mark for update in database if needed
            if need_update:
                orders_to_update.append({
                    'symbol': order['symbol'],
                    'strike_price': order['strike_price'],
                    'option_type': order['option_type'],
                    'new_status': current_status,
                    'is_less_than_25pct': is_less_than_25pct,
                    'is_less_than_50pct': is_less_than_50pct,
                    'is_less_than_75pct': is_less_than_75pct,
                    'is_greater_than_25pct': is_greater_than_25pct,
                    'is_greater_than_50pct': is_greater_than_50pct,
                    'is_greater_than_75pct': is_greater_than_75pct,
                    'is_hit': is_hit,
                    'hit_peak_pct': hit_peak_pct,
                    'lowest_point': lowest_point,
                    'todays_max_ltp': todays_max_ltp,
                    'todays_min_ltp': todays_min_ltp
                })

            # Safely convert all values to appropriate types
            response_data.append({
                'symbol': order['symbol'],
                'strike_price': float(order['strike_price']),
                'option_type': order['option_type'],
                'stored_ltp': stored_ltp,
                'current_ltp': current_ltp,
                'percent_change': percent_change,
                'todays_return': todays_return,  # Add today's return to response
                'status': current_status,  # Use the current status (could be "Done" now)
                'daysCaptured': days_captured,  # Added days captured
                'oi': live_oi,  # Use live OI
                'original_oi': original_oi,  # Original OI
                'oi_change': oi_change,  # OI change percentage
                'volume': live_volume,  # Use live volume
                'original_volume': original_volume,  # Original volume
                'volume_change': volume_change,  # Volume change percentage
                'iv': current_iv,
                'original_iv': original_iv,
                'iv_change': iv_change,
                'delta': current_delta,
                'original_delta': original_delta,
                'delta_change': delta_change,
                'gamma': current_gamma,
                'original_gamma': original_gamma,
                'gamma_change': gamma_change,
                'theta': current_theta,
                'original_theta': original_theta,
                'theta_change': theta_change,
                'vega': current_vega,
                'original_vega': original_vega,
                'vega_change': vega_change,
                'pop': float(order.get('pop', 0) or 0),
                'stored_bidq': float(order.get('bid_qty', 0) or 0),
                'stored_askq': float(order.get('ask_qty', 0) or 0),
                'bidQ': float(live_data.get('bidQ', 0) or 0),
                'askQ': float(live_data.get('askQ', 0) or 0),
                'lot_size': float(order.get('lot_size', 1) or 1),
                'instrument_key': instrument_key,
                'timestamp': order.get('timestamp', ''),
                'is_less_than_25pct': is_less_than_25pct,  # Include the flag
                'is_less_than_50pct': is_less_than_50pct,  # Include the flag
                'is_less_than_75pct': is_less_than_75pct,
                'is_greater_than_25pct': is_greater_than_25pct,  # Include the flag
                'is_greater_than_50pct': is_greater_than_50pct,  # Include the flag
                'is_greater_than_75pct': is_greater_than_75pct,  # Include the flag
                'lowest_point': lowest_point,  # Include the lowest point
                'role': order.get('role', 'Unknown'),  # Include role if available
                'pcr': float(order.get('pcr', 0) or 0),
                'prev_close': float(order.get('prev_close', 0) or 0),
                'is_hit': is_hit,
                'hit_peak_pct': hit_peak_pct,
                'todays_max_ltp': todays_max_ltp,
                'todays_min_ltp': todays_min_ltp
            })

        # Update status in database for orders that need it
        if orders_to_update:
            # Call database service to update statuses
            db_service.update_options_orders_status(orders_to_update)
            print(f"Updated status for {len(orders_to_update)} orders")

        return {
            "success": True,
            "data": response_data,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error in options orders analysis: {str(e)}")
        import traceback
        traceback.print_exc()  # Add traceback for better debugging
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/mark-hit")
async def mark_hit(request: Request):
    """Manually mark an option as hit recovery trade in the database."""
    try:
        body = await request.json()
        symbol = body.get('symbol')
        strike_price = body.get('strike_price')
        option_type = body.get('option_type')
        if not all([symbol, strike_price, option_type]):
            raise HTTPException(status_code=400, detail="symbol, strike_price, option_type required")
        with db_service._get_cursor() as cur:
            cur.execute("""
                UPDATE options_orders
                SET is_hit = TRUE, hit_peak_pct = GREATEST(COALESCE(hit_peak_pct, 0), %s)
                WHERE symbol = %s AND strike_price = %s AND option_type = %s
            """, (float(body.get('peak_pct', 50)), symbol, strike_price, option_type))
        return {"success": True, "message": f"Marked {symbol} {strike_price} {option_type} as hit"}
    except Exception as e:
        logger.error(f"Error marking hit: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/remove-hit")
async def remove_hit(request: Request):
    """Remove hit recovery trade mark from an option in the database."""
    try:
        body = await request.json()
        symbol = body.get('symbol')
        strike_price = body.get('strike_price')
        option_type = body.get('option_type')
        if not all([symbol, strike_price, option_type]):
            raise HTTPException(status_code=400, detail="symbol, strike_price, option_type required")
        with db_service._get_cursor() as cur:
            cur.execute("""
                UPDATE options_orders
                SET is_hit = FALSE, hit_peak_pct = 0
                WHERE symbol = %s AND strike_price = %s AND option_type = %s
            """, (symbol, strike_price, option_type))
        return {"success": True, "message": f"Removed hit for {symbol} {strike_price} {option_type}"}
    except Exception as e:
        logger.error(f"Error removing hit: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def close_all_websockets_sync():
    """Synchronous function to close all WebSocket connections at startup."""
    print("Closing all existing WebSocket connections at startup...")

    try:
        # Get all tokens with active connections
        tokens = list(active_websocket_connections.keys())

        # Create an event loop if needed (for the main thread)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Close each connection
        for token in tokens:
            conn_info = active_websocket_connections.get(token, {})
            websocket = conn_info.get('websocket')

            if websocket is not None:
                try:
                    print(f"Closing existing WebSocket for token ending ...{token[-4:]}")
                    # Use run_until_complete to run the coroutine synchronously
                    loop.run_until_complete(websocket.close(code=1000, reason="Application startup"))
                    print(f"Successfully closed WebSocket for token ending ...{token[-4:]}")
                except Exception as e:
                    print(f"Error closing WebSocket for token {token[-4:]}: {e}")
                finally:
                    # Update connection info
                    active_websocket_connections[token]['websocket'] = None

        # Also close the current websocket if it exists
        global current_websocket
        if current_websocket is not None:
            try:
                print("Closing current WebSocket connection...")
                loop.run_until_complete(current_websocket.close())
                print("Current WebSocket connection closed successfully")
            except Exception as e:
                print(f"Error closing current WebSocket: {e}")
            finally:
                current_websocket = None

        # Reset the active_websocket_connections dictionary
        active_websocket_connections.clear()

        # Find and kill any orphaned WebSocket connections using socket module
        try:
            import socket
            import psutil

            # Get all connections for this process
            proc = psutil.Process()
            for conn in proc.connections():
                if conn.status == 'ESTABLISHED' and 'upstox.com' in str(conn.raddr):
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.connect((conn.raddr.ip, conn.raddr.port))
                        sock.close()
                        print(f"Closed orphaned WebSocket connection to {conn.raddr}")
                    except Exception as e:
                        print(f"Error closing orphaned connection: {e}")
        except ImportError:
            print("psutil module not available, skipping orphaned connection cleanup")
        except Exception as e:
            print(f"Error checking for orphaned connections: {e}")

        print("All existing WebSocket connections closed")
    except Exception as e:
        print(f"Error in close_all_websockets_sync: {e}")
        import traceback
        traceback.print_exc()

from datetime import datetime

def aggregate_oi_data_by_interval(data, interval):
    """Aggregate OI data by time interval."""
    if not data:
        return []

    data.sort(key=lambda x: x["time"])
    aggregated_data = {}
    interval_minutes = {
        "5m": 5,
        "15m": 15,
        "1h": 60
    }.get(interval, 5)

    for item in data:
        try:
            # Try parsing with full datetime, fallback to time only
            try:
                dt = datetime.strptime(item["time"], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                # If only time is provided, use today's date
                today = datetime.now().strftime('%Y-%m-%d')
                dt = datetime.strptime(f"{today} {item['time']}", '%Y-%m-%d %H:%M')

            minutes = dt.minute
            normalized_minutes = (minutes // interval_minutes) * interval_minutes
            interval_key = dt.replace(minute=normalized_minutes, second=0).strftime('%Y-%m-%d %H:%M:%S')

            if interval_key not in aggregated_data:
                aggregated_data[interval_key] = {
                    "time": interval_key,
                    "call_oi": item["call_oi"],
                    "put_oi": item["put_oi"],
                    "call_put_ratio": item["call_put_ratio"],
                    "timestamp": item["timestamp"],
                    "count": 1
                }
            else:
                current = aggregated_data[interval_key]
                current["call_oi"] = max(current["call_oi"], item["call_oi"])
                current["put_oi"] = max(current["put_oi"], item["put_oi"])
                current["call_put_ratio"] = (current["call_put_ratio"] * current["count"] + item["call_put_ratio"]) / (current["count"] + 1)
                current["count"] += 1
        except Exception as e:
            print(f"Error processing data point: {item}, error: {str(e)}")
            continue

    result = list(aggregated_data.values())
    result.sort(key=lambda x: x["time"])
    for item in result:
        if "count" in item:
            del item["count"]
    return result

@app.get("/api/total-oi-history")
async def get_total_oi_history(symbol: str, time_interval: str = "5m", limit: int = 100):
    try:
        with db_service._get_cursor() as cur:
            # Get the total OI history data from the database
            query = """
                    SELECT display_time, call_oi, put_oi, call_put_ratio, timestamp
                    FROM total_oi_history
                    WHERE symbol = %s
                    ORDER BY display_time DESC
                    LIMIT %s
                """
            cur.execute(query, (symbol, limit))
            rows = cur.fetchall()

            if not rows:
                return {"success": False, "message": "No data found for the symbol", "data": []}

            # Process the data based on time interval
            data = []
            for row in rows:
                if isinstance(row[0], str):
                    display_time = row[0]
                else:
                    display_time = row[0].strftime('%Y-%m-%d %H:%M:%S') if row[0] else None

                if isinstance(row[4], str):
                    timestamp = row[4]
                else:
                    timestamp = row[4].strftime('%Y-%m-%d %H:%M:%S') if row[4] else None

                data.append({
                    "time": display_time,
                    "call_oi": float(row[1]) if row[1] is not None else 0,
                    "put_oi": float(row[2]) if row[2] is not None else 0,
                    "call_put_ratio": float(row[3]) if row[3] is not None else 0,
                    "timestamp": timestamp
                })

            # Group by time interval if necessary
            if time_interval != "raw":
                data = aggregate_oi_data_by_interval(data, time_interval)

            # Calculate OI change for each interval
            if len(data) > 1:
                for i in range(1, len(data)):
                    data[i]["call_oi_change"] = data[i]["call_oi"] - data[i-1]["call_oi"]
                    data[i]["put_oi_change"] = data[i]["put_oi"] - data[i-1]["put_oi"]

                # Set first interval changes to 0
                data[0]["call_oi_change"] = 0
                data[0]["put_oi_change"] = 0

            return {
                "success": True,
                "data": data,
                "symbol": symbol,
                "time_interval": time_interval
            }
    except Exception as e:
        print(f"Error fetching total OI history: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

def run_websocket():
    asyncio.run(websocket_worker())

def start_services():
    # First, close any existing WebSocket connections
    close_all_websockets_sync()

    # Print debug information at startup
    print("Starting worker with empty active_subscription list")
    print(f"Active subscription state: {active_subscription}")

    # Start WebSocket in a separate thread
    threading.Thread(target=run_websocket, daemon=True).start()

    # Start FastAPI
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv('PORT', 8000)))

if __name__ == '__main__':
    # First close all existing WebSocket connections before starting any services
    close_all_websockets_sync()

    # Ensure active_subscription is empty at startup
    active_subscription = []

    start_services()