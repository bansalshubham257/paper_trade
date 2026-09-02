# Paper Trade - Recovery Trade Paper Trader

Monitors `options_orders` (populated by the Upstox WebSocket feed) for **recovery
trades** and paper-trades them in real time:

- **Entry**: when a recovery trade recovers to **33%** return from its stored LTP.
- **Profit exit**: book profit the first time price enters the **47-50%** zone.
- **Stop loss**: exit at **-90%** from stored LTP.
- **Position**: ~₹20k notional, 1 lot default (never reduce a single lot).

## Services (one per Railway service)

| Service | Command | Purpose |
|---------|---------|---------|
| `feed` | `python upstox_feed.py` | WebSocket feed -> populates `options_orders` (auto tokens) |
| `worker` | `python worker.py` | FastAPI `/api/options-orders-analysis`, maintains recovery flags |
| `paper-trader` | `python paper_trader.py` | Live recovery paper trading loop (headless) |
| `token` | `python generate_token.py` | Daily morning Upstox token generation into `upstox_accounts` |

## Flow

```
upstox_feed.py  --(populates)-->  options_orders  --(flags)-->  worker.py
                                                                   |
paper_trader.py  <==== reads options_orders + live Upstox LTP =====+
     -> paper_trades (entry 33%, profit 47-50%, SL -90%)
```

## Env vars

- `DATABASE_URL` - Railway Postgres connection string (all services)
- `PORT` - worker's FastAPI port (Railway injects)
- `POLL_INTERVAL_S` - paper trader poll interval (default 30)

## Database

Tables: `options_orders`, `instrument_keys`, `upstox_accounts`, `paper_trades`.
Apply/verify schema:

```bash
DATABASE_URL=postgresql://... python setup_database.py
```
