-- =============================================================================
-- PAPER TRADE PROJECT - FOCUSED DATABASE SCHEMA
-- Only the tables required for recovery-trade paper trading.
-- Safe to run repeatedly: uses CREATE TABLE IF NOT EXISTS.
-- Run: psql "$DATABASE_URL" -f sql/paper_trade_schema.sql
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. OPTIONS ORDERS  (recovery trade source - populated by worker live feed,
--    monitored by paper_trader)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS options_orders (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    strike_price DECIMAL NOT NULL,
    option_type CHAR(2) CHECK (option_type IN ('CE', 'PE')),
    ltp DECIMAL,
    bid_qty INTEGER,
    ask_qty INTEGER,
    lot_size INTEGER,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    oi DECIMAL,
    volume DECIMAL,
    vega DECIMAL,
    theta DECIMAL,
    gamma DECIMAL,
    delta DECIMAL,
    iv DECIMAL,
    pop DECIMAL,
    status VARCHAR(20) DEFAULT 'Open',
    pcr DECIMAL DEFAULT 0,
    is_hit BOOLEAN DEFAULT FALSE,
    hit_peak_pct DECIMAL DEFAULT 0,
    is_less_than_25pct BOOLEAN DEFAULT FALSE,
    is_less_than_50pct BOOLEAN DEFAULT FALSE,
    is_less_than_75pct BOOLEAN DEFAULT FALSE,
    is_greater_than_25pct BOOLEAN DEFAULT FALSE,
    is_greater_than_50pct BOOLEAN DEFAULT FALSE,
    is_greater_than_75pct BOOLEAN DEFAULT FALSE,
    todays_max_ltp DECIMAL,
    todays_min_ltp DECIMAL,
    UNIQUE (symbol, strike_price, option_type)
);
CREATE INDEX IF NOT EXISTS idx_options_symbol_time ON options_orders(symbol, timestamp);

-- -----------------------------------------------------------------------------
-- 2. INSTRUMENT KEYS  (symbols, lot sizes, expiry mapping)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument_keys (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    instrument_key VARCHAR(100) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    tradingsymbol VARCHAR(100) NOT NULL,
    lot_size INTEGER,
    instrument_type VARCHAR(20) NOT NULL,
    expiry_date DATE,
    strike_price DECIMAL(20, 2),
    option_type VARCHAR(5),
    prev_close DECIMAL(20, 4),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (tradingsymbol, exchange)
);
CREATE INDEX IF NOT EXISTS idx_instrument_keys_symbol ON instrument_keys(symbol);
CREATE INDEX IF NOT EXISTS idx_instrument_keys_instrument_key ON instrument_keys(instrument_key);
CREATE INDEX IF NOT EXISTS idx_instrument_keys_instrument_type ON instrument_keys(instrument_type);
CREATE INDEX IF NOT EXISTS idx_instrument_keys_expiry_date ON instrument_keys(expiry_date);

-- -----------------------------------------------------------------------------
-- 3. UPSTOX ACCOUNTS  (credentials + daily-generated access token)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS upstox_accounts (
    id SERIAL PRIMARY KEY,
    api_key VARCHAR(255) NOT NULL UNIQUE,
    api_secret VARCHAR(255) NOT NULL,
    totp_secret VARCHAR(255) NOT NULL,
    redirect_uri VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL,
    access_token TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_upstox_accounts_api_key ON upstox_accounts(api_key);

-- -----------------------------------------------------------------------------
-- 4. PAPER TRADES  (recovery trade paper trading record)
--    ENTRY at 33% from stored LTP, PROFIT exit at 47-50%, STOP at -90%.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS paper_trades (
    id BIGSERIAL PRIMARY KEY,
    source_symbol VARCHAR(20) NOT NULL,
    source_strike_price DECIMAL NOT NULL,
    source_option_type CHAR(2) NOT NULL,
    source_stored_ltp DECIMAL NOT NULL,
    lot_size INTEGER DEFAULT 1,
    quantity INTEGER DEFAULT 1,
    notional_value DECIMAL(12, 2),
    entry_ltp DECIMAL NOT NULL,
    entry_time TIMESTAMPTZ DEFAULT NOW(),
    entry_pct_change DECIMAL,
    exit_ltp DECIMAL,
    exit_time TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'PROFIT', 'LOSS', 'EXPIRED')),
    exit_reason VARCHAR(20),
    exit_pct_change DECIMAL,
    pnl_pct DECIMAL,
    current_ltp DECIMAL,
    current_pct_change DECIMAL,
    peak_pct DECIMAL DEFAULT 0,
    entry_threshold DECIMAL DEFAULT 33,
    exit_threshold_min DECIMAL DEFAULT 47,
    exit_threshold_max DECIMAL DEFAULT 50,
    stop_loss_threshold DECIMAL DEFAULT -90,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_symbol, source_strike_price, source_option_type, source_stored_ltp)
);
CREATE INDEX IF NOT EXISTS idx_paper_trades_status ON paper_trades(status);
CREATE INDEX IF NOT EXISTS idx_paper_trades_source ON paper_trades(source_symbol, source_strike_price, source_option_type);
CREATE INDEX IF NOT EXISTS idx_paper_trades_entry_time ON paper_trades(entry_time);
CREATE INDEX IF NOT EXISTS idx_paper_trades_created_at ON paper_trades(created_at);

COMMIT;
