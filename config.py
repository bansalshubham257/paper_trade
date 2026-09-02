import os
from datetime import time

class Config:
    # Database configuration
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://swingtrade_db_user:ZlewRq8aZKimqMwrP2LdRTuFsvhi9qDw@dpg-cvh8gfpu0jms73bj6gm0-a.oregon-postgres.render.com/swingtrade_db')

    WORKER_URL = os.getenv('WORKER_URL', '')

    # Upstox API configuration
    ACCESS_TOKE = os.getenv('ACCESS_TOKEN', '.eyJzdiOiIyWEJSUFMiLqdGkiOiI2NDFZjBjpZW50IjpmYWLOiJ1ZGFwaAiOjEMDB9.Ra7Bclq3ysxWNmi7oJol_1mcgz1sCK7WWgFG-59ZFmM')
    ACCESS_TOKEN2 = os.getenv('ACCESS_TOKEN2', 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ..Ra7Bclq3ysxWNmi7oJol_1mcgz1sCK7WWgFG-59ZFmM')

    # Other configurations
    EXPIRY_DATE = "2026-07-28"
    # Market hours configuraion
    MARKET_OPEN = time(9, 0)  # 09:15 AM
    MARKET_CLOSE = time(15, 32)  # 03:30 PM

    TOKEN_MARKET_OPEN = time(6, 10)
    TOKEN_MARKET_CLOSE = time(8, 33)

    # Post-market window for financial data collection (3:35 PM - 3:39 PM)
    POST_MARKET_START = time(15, 30)
    POST_MARKET_END = time(15, 50)

    # Database clearing time window
    DB_CLEARING_START = time(9, 0)  # 09:00 AM
    DB_CLEARING_END = time(9, 9)  # 09:15 AM

    TRADING_DAYS = {0, 1, 2, 3, 4}  # Monday to Friday

    SENSEX_EXPIRIES = [
        "2000-07-01"
    ]

    BANKEX_EXPIRIES = [
        "2000-07-29"  # Fixed typo: 067-29 -> 07-29
    ]

    NIFTY_EXPIRIES = [
        "2000-07-03"
    ]

    # These should include all relevant expiry dates for different instruments
    INSTRUMENT_EXPIRIES = [
        "2026-07-28"
    ]