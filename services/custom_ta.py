# custom_ta.py
import numpy as np
import pandas as pd

class TA:
    """Custom Technical Analysis functions"""

    @staticmethod
    def SMA(series, timeperiod=None):
        """Simple Moving Average"""
        return series.rolling(window=timeperiod).mean()

    @staticmethod
    def EMA(series, timeperiod=None):
        """Exponential Moving Average"""
        return series.ewm(span=timeperiod, adjust=False).mean()

    @staticmethod
    def MACD(close, fastperiod=12, slowperiod=26, signalperiod=9):
        """Moving Average Convergence Divergence"""
        fast_ema = pd.Series(close).ewm(span=fastperiod, adjust=False).mean()
        slow_ema = pd.Series(close).ewm(span=slowperiod, adjust=False).mean()
        macd_line = fast_ema - slow_ema
        signal_line = macd_line.ewm(span=signalperiod, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def RSI(close, timeperiod=14):
        """Relative Strength Index"""
        delta = pd.Series(close).diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        ema_up = up.ewm(com=timeperiod-1, adjust=False).mean()
        ema_down = down.ewm(com=timeperiod-1, adjust=False).mean()
        rs = ema_up / ema_down.replace(0, np.finfo(float).eps)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2):
        """Bollinger Bands"""
        middle = pd.Series(close).rolling(window=timeperiod).mean()
        stddev = pd.Series(close).rolling(window=timeperiod).std()
        upper = middle + (stddev * nbdevup)
        lower = middle - (stddev * nbdevdn)
        return upper, middle, lower

    # Fix for CMF calculation
    @staticmethod
    def CMF(high, low, close, volume, timeperiod=20):
        """Chaikin Money Flow"""
        # Money Flow Multiplier
        mfm = pd.Series(np.zeros(len(close)), index=close.index)
        range_mask = (high - low) > 0  # Avoid division by zero

        # Calculate only where range is non-zero
        mfm[range_mask] = ((close[range_mask] - low[range_mask]) -
                           (high[range_mask] - close[range_mask])) / (high[range_mask] - low[range_mask])

        # Money Flow Volume
        mfv = mfm * volume

        # CMF is the sum of Money Flow Volume divided by the sum of Volume over n periods
        cmf = pd.Series(np.nan, index=close.index)
        vol_sum = volume.rolling(window=timeperiod).sum()
        mfv_sum = mfv.rolling(window=timeperiod).sum()

        # Calculate only where volume sum is non-zero
        mask = vol_sum > 0
        cmf[mask] = mfv_sum[mask] / vol_sum[mask]

        return cmf

    # Fix for ADX calculation
    @staticmethod
    def ADX(high, low, close, timeperiod=14):
        """Average Directional Index"""
        if len(high) < timeperiod + 1:
            return pd.Series(np.nan, index=high.index)

        # Calculate True Range
        tr = pd.Series(np.zeros(len(high)), index=high.index)
        for i in range(1, len(high)):
            tr.iloc[i] = max(
                high.iloc[i] - low.iloc[i],  # Current high - current low
                abs(high.iloc[i] - close.iloc[i-1]),  # Current high - previous close
                abs(low.iloc[i] - close.iloc[i-1])  # Current low - previous close
            )

        # Smooth the TR using Wilder's smoothing
        atr = pd.Series(np.zeros(len(high)), index=high.index)
        atr.iloc[timeperiod] = tr.iloc[1:timeperiod+1].sum() / timeperiod
        for i in range(timeperiod + 1, len(high)):
            atr.iloc[i] = (atr.iloc[i-1] * (timeperiod - 1) + tr.iloc[i]) / timeperiod

        # +DM and -DM
        plus_dm = pd.Series(np.zeros(len(high)), index=high.index)
        minus_dm = pd.Series(np.zeros(len(high)), index=high.index)

        for i in range(1, len(high)):
            up_move = high.iloc[i] - high.iloc[i-1]
            down_move = low.iloc[i-1] - low.iloc[i]

            if up_move > down_move and up_move > 0:
                plus_dm.iloc[i] = up_move
            else:
                plus_dm.iloc[i] = 0

            if down_move > up_move and down_move > 0:
                minus_dm.iloc[i] = down_move
            else:
                minus_dm.iloc[i] = 0

        # Smooth +DM and -DM
        smoothed_plus_dm = pd.Series(np.zeros(len(high)), index=high.index)
        smoothed_minus_dm = pd.Series(np.zeros(len(high)), index=high.index)

        smoothed_plus_dm.iloc[timeperiod] = plus_dm.iloc[1:timeperiod+1].sum()
        smoothed_minus_dm.iloc[timeperiod] = minus_dm.iloc[1:timeperiod+1].sum()

        for i in range(timeperiod + 1, len(high)):
            smoothed_plus_dm.iloc[i] = smoothed_plus_dm.iloc[i-1] - (smoothed_plus_dm.iloc[i-1] / timeperiod) + plus_dm.iloc[i]
            smoothed_minus_dm.iloc[i] = smoothed_minus_dm.iloc[i-1] - (smoothed_minus_dm.iloc[i-1] / timeperiod) + minus_dm.iloc[i]

        # +DI and -DI
        plus_di = pd.Series(np.zeros(len(high)), index=high.index)
        minus_di = pd.Series(np.zeros(len(high)), index=high.index)

        for i in range(timeperiod, len(high)):
            if atr.iloc[i] != 0:
                plus_di.iloc[i] = 100 * smoothed_plus_dm.iloc[i] / atr.iloc[i]
                minus_di.iloc[i] = 100 * smoothed_minus_dm.iloc[i] / atr.iloc[i]

        # DX
        dx = pd.Series(np.zeros(len(high)), index=high.index)
        for i in range(timeperiod, len(high)):
            if (plus_di.iloc[i] + minus_di.iloc[i]) != 0:
                dx.iloc[i] = 100 * abs(plus_di.iloc[i] - minus_di.iloc[i]) / (plus_di.iloc[i] + minus_di.iloc[i])

        # ADX
        adx = pd.Series(np.zeros(len(high)), index=high.index)
        adx.iloc[2 * timeperiod - 1] = dx.iloc[timeperiod:2*timeperiod].mean()

        for i in range(2 * timeperiod, len(high)):
            adx.iloc[i] = (adx.iloc[i-1] * (timeperiod - 1) + dx.iloc[i]) / timeperiod

        return adx

    @staticmethod
    def PLUS_DI(high, low, close, timeperiod=14):
        """Plus Directional Indicator"""
        if len(high) < timeperiod + 1:
            return pd.Series(np.nan, index=high.index)
            
        # Calculate True Range
        tr = TA.TRUE_RANGE(high, low, close)
        
        # Calculate Directional Movement
        up_move = high.diff()
        down_move = low.shift(1) - low
        
        # Positive Directional Movement
        plus_dm = pd.Series(np.zeros(len(high)), index=high.index)
        for i in range(1, len(high)):
            if (up_move.iloc[i] > down_move.iloc[i]) and up_move.iloc[i] > 0:
                plus_dm.iloc[i] = up_move.iloc[i]
        
        # Smooth the calculations using Wilder's method
        smoothed_tr = tr.rolling(timeperiod).sum()
        smoothed_plus_dm = plus_dm.rolling(timeperiod).sum()
        
        # Calculate +DI
        plus_di = 100 * smoothed_plus_dm / smoothed_tr.replace(0, np.finfo(float).eps)
        
        return plus_di

    @staticmethod
    def MINUS_DI(high, low, close, timeperiod=14):
        """Minus Directional Indicator"""
        if len(high) < timeperiod + 1:
            return pd.Series(np.nan, index=high.index)
            
        # Calculate True Range
        tr = TA.TRUE_RANGE(high, low, close)
        
        # Calculate Directional Movement
        up_move = high.diff()
        down_move = low.shift(1) - low
        
        # Negative Directional Movement
        minus_dm = pd.Series(np.zeros(len(high)), index=high.index)
        for i in range(1, len(high)):
            if (down_move.iloc[i] > up_move.iloc[i]) and down_move.iloc[i] > 0:
                minus_dm.iloc[i] = down_move.iloc[i]
        
        # Smooth the calculations using Wilder's method
        smoothed_tr = tr.rolling(timeperiod).sum()
        smoothed_minus_dm = minus_dm.rolling(timeperiod).sum()
        
        # Calculate -DI
        minus_di = 100 * smoothed_minus_dm / smoothed_tr.replace(0, np.finfo(float).eps)
        
        return minus_di

    @staticmethod
    def STOCHRSI(close, timeperiod=14, fastk_period=5, fastd_period=3):
        """StochRSI"""
        rsi = TA.RSI(close, timeperiod)
        stoch_k = 100 * (rsi - rsi.rolling(fastk_period).min()) / (rsi.rolling(fastk_period).max() - rsi.rolling(fastk_period).min()).replace(0, np.finfo(float).eps)
        stoch_d = stoch_k.rolling(fastd_period).mean()
        return stoch_k, stoch_d

    @staticmethod
    def ATR(high, low, close, timeperiod=14):
        """Average True Range"""
        true_range = TA.TRUE_RANGE(high, low, close)
        return true_range.rolling(window=timeperiod).mean()

    @staticmethod
    def TRUE_RANGE(high, low, close):
        """True Range"""
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    @staticmethod
    def AROON_UP(high, timeperiod=14):
        """Aroon Up"""
        return 100 * (timeperiod - high.rolling(window=timeperiod).apply(lambda x: timeperiod - x.argmax() - 1)) / timeperiod

    @staticmethod
    def AROON_DOWN(low, timeperiod=14):
        """Aroon Down"""
        return 100 * (timeperiod - low.rolling(window=timeperiod).apply(lambda x: timeperiod - x.argmin() - 1)) / timeperiod

    @staticmethod
    def AROON_OSC(high, low, timeperiod=14):
        """Aroon Oscillator"""
        aroon_up = TA.AROON_UP(high, timeperiod)
        aroon_down = TA.AROON_DOWN(low, timeperiod)
        return aroon_up - aroon_down

    @staticmethod
    def STOCH(high, low, close, fastk_period=5, slowk_period=3, slowd_period=3):
        """Stochastic Oscillator"""
        highest_high = high.rolling(window=fastk_period).max()
        lowest_low = low.rolling(window=fastk_period).min()

        # Fast %K
        fastk = 100 * (close - lowest_low) / (highest_high - lowest_low).replace(0, np.finfo(float).eps)

        # Slow %K (smoothed fast %K)
        slowk = fastk.rolling(window=slowk_period).mean()

        # Slow %D (smoothed slow %K)
        slowd = slowk.rolling(window=slowd_period).mean()

        return slowk, slowd

    @staticmethod
    def SAR(high, low, acceleration=0.02, maximum=0.2):
        """Parabolic SAR"""
        prices = pd.DataFrame({'high': high, 'low': low})
        prices.index = pd.RangeIndex(len(prices))

        sar = np.zeros(len(prices))
        trend = np.zeros(len(prices))
        ep = np.zeros(len(prices))
        af = np.zeros(len(prices))

        # Initialize
        trend[0] = 1  # Start with uptrend
        sar[0] = prices['low'].iloc[0]
        ep[0] = prices['high'].iloc[0]
        af[0] = acceleration

        # Calculate SAR values
        for i in range(1, len(prices)):
            # Previous trend was up
            if trend[i-1] == 1:
                sar[i] = sar[i-1] + af[i-1] * (ep[i-1] - sar[i-1])
                # Ensure SAR is below previous low
                sar[i] = min(sar[i], prices['low'].iloc[i-1], prices['low'].iloc[max(0, i-2)])

                # Check for trend reversal
                if prices['low'].iloc[i] < sar[i]:
                    trend[i] = -1  # Trend changes to down
                    sar[i] = ep[i-1]  # SAR becomes previous EP
                    ep[i] = prices['low'].iloc[i]  # EP becomes current low
                    af[i] = acceleration  # Reset AF
                else:
                    trend[i] = 1  # Trend remains up
                    if prices['high'].iloc[i] > ep[i-1]:
                        ep[i] = prices['high'].iloc[i]  # New high becomes EP
                        af[i] = min(af[i-1] + acceleration, maximum)  # Increase AF
                    else:
                        ep[i] = ep[i-1]  # EP remains the same
                        af[i] = af[i-1]  # AF remains the same
            # Previous trend was down
            else:
                sar[i] = sar[i-1] - af[i-1] * (sar[i-1] - ep[i-1])
                # Ensure SAR is above previous high
                sar[i] = max(sar[i], prices['high'].iloc[i-1], prices['high'].iloc[max(0, i-2)])

                # Check for trend reversal
                if prices['high'].iloc[i] > sar[i]:
                    trend[i] = 1  # Trend changes to up
                    sar[i] = ep[i-1]  # SAR becomes previous EP
                    ep[i] = prices['high'].iloc[i]  # EP becomes current high
                    af[i] = acceleration  # Reset AF
                else:
                    trend[i] = -1  # Trend remains down
                    if prices['low'].iloc[i] < ep[i-1]:
                        ep[i] = prices['low'].iloc[i]  # New low becomes EP
                        af[i] = min(af[i-1] + acceleration, maximum)  # Increase AF
                    else:
                        ep[i] = ep[i-1]  # EP remains the same
                        af[i] = af[i-1]  # AF remains the same

        return pd.Series(sar, index=high.index)

    @staticmethod
    def CCI(high, low, close, timeperiod=20):
        """Commodity Channel Index"""
        tp = (high + low + close) / 3
        tp_ma = tp.rolling(window=timeperiod).mean()
        md = (tp - tp_ma).abs().rolling(window=timeperiod).mean()
        return (tp - tp_ma) / (0.015 * md.replace(0, np.finfo(float).eps))

    @staticmethod
    def MFI(high, low, close, volume, timeperiod=14):
        """Money Flow Index"""
        typical_price = (high + low + close) / 3
        money_flow = typical_price * volume

        delta = typical_price.diff()
        pos_flow = (money_flow * (delta > 0)).rolling(window=timeperiod).sum()
        neg_flow = (money_flow * (delta < 0)).rolling(window=timeperiod).sum()

        money_ratio = pos_flow / neg_flow.replace(0, np.finfo(float).eps)
        return 100 - (100 / (1 + money_ratio))

    @staticmethod
    def ROC(close, timeperiod=10):
        """Rate of Change"""
        return 100 * (close / close.shift(timeperiod) - 1)

    @staticmethod
    def WILLR(high, low, close, timeperiod=14):
        """Williams' %R"""
        highest = high.rolling(window=timeperiod).max()
        lowest = low.rolling(window=timeperiod).min()
        return -100 * (highest - close) / (highest - lowest).replace(0, np.finfo(float).eps)

    @staticmethod
    def OBV(close, volume):
        """On Balance Volume"""
        obv = np.zeros(len(close))
        for i in range(1, len(close)):
            if close.iloc[i] > close.iloc[i-1]:
                obv[i] = obv[i-1] + volume.iloc[i]
            elif close.iloc[i] < close.iloc[i-1]:
                obv[i] = obv[i-1] - volume.iloc[i]
            else:
                obv[i] = obv[i-1]
        return pd.Series(obv, index=close.index)

    @staticmethod
    def AD(high, low, close, volume):
        """Accumulation/Distribution Line"""
        clv = ((close - low) - (high - close)) / (high - low).replace(0, np.finfo(float).eps)
        ad = clv * volume
        return ad.cumsum()

    @staticmethod
    def ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10):
        """Chaikin A/D Oscillator"""
        ad = TA.AD(high, low, close, volume)
        fast_ema = ad.ewm(span=fastperiod, adjust=False).mean()
        slow_ema = ad.ewm(span=slowperiod, adjust=False).mean()
        return fast_ema - slow_ema

    @staticmethod
    def TRIX(close, timeperiod=14):
        """Triple Exponential Moving Average (TRIX)"""
        ema1 = close.ewm(span=timeperiod, adjust=False).mean()
        ema2 = ema1.ewm(span=timeperiod, adjust=False).mean()
        ema3 = ema2.ewm(span=timeperiod, adjust=False).mean()
        return 100 * (ema3 / ema3.shift(1) - 1)

    @staticmethod
    def STDDEV(close, timeperiod=20, nbdev=1):
        """Standard Deviation"""
        return close.rolling(window=timeperiod).std() * nbdev

    @staticmethod
    def VAR(close, timeperiod=20, nbdev=1):
        """Variance"""
        return close.rolling(window=timeperiod).var() * nbdev

    @staticmethod
    def TSF(close, timeperiod=14):
        """Time Series Forecast"""
        # Simple linear regression forecast
        x = np.arange(1, timeperiod + 1)
        result = pd.Series(index=close.index)

        for i in range(timeperiod, len(close) + 1):
            y = close.iloc[i - timeperiod:i].values
            if len(y) == timeperiod:
                slope, intercept = np.polyfit(x, y, 1)
                result.iloc[i - 1] = slope * (timeperiod + 1) + intercept

        return result

    @staticmethod
    def NATR(high, low, close, timeperiod=14):
        """Normalized Average True Range"""
        atr = TA.ATR(high, low, close, timeperiod)
        return 100 * atr / close

    @staticmethod
    def HT_TRENDLINE(close):
        """Hilbert Transform - Instantaneous Trendline"""
        # This is a simplified approximation
        return close.ewm(span=40, adjust=False).mean()

    @staticmethod
    def LINEARREG(close, timeperiod=14):
        """Linear Regression"""
        result = pd.Series(index=close.index)
        for i in range(timeperiod, len(close) + 1):
            y = close.iloc[i - timeperiod:i].values
            if len(y) == timeperiod:
                x = np.arange(timeperiod)
                slope, intercept = np.polyfit(x, y, 1)
                result.iloc[i - 1] = slope * (timeperiod - 1) + intercept
        return result

    @staticmethod
    def LINEARREG_SLOPE(close, timeperiod=14):
        """Linear Regression Slope"""
        result = pd.Series(index=close.index)
        for i in range(timeperiod, len(close) + 1):
            y = close.iloc[i - timeperiod:i].values
            if len(y) == timeperiod:
                x = np.arange(timeperiod)
                slope, _ = np.polyfit(x, y, 1)
                result.iloc[i - 1] = slope
        return result

    @staticmethod
    def LINEARREG_ANGLE(close, timeperiod=14):
        """Linear Regression Angle"""
        slope = TA.LINEARREG_SLOPE(close, timeperiod)
        return np.degrees(np.arctan(slope))

    @staticmethod
    def LINEARREG_INTERCEPT(close, timeperiod=14):
        """Linear Regression Intercept"""
        result = pd.Series(index=close.index)
        for i in range(timeperiod, len(close) + 1):
            y = close.iloc[i - timeperiod:i].values
            if len(y) == timeperiod:
                x = np.arange(timeperiod)
                _, intercept = np.polyfit(x, y, 1)
                result.iloc[i - 1] = intercept
        return result

    @staticmethod
    def bbands(close, length=20, std=2):
        """Bollinger Bands for pandas_ta compatibility"""
        upper, middle, lower = TA.BBANDS(close, timeperiod=length, nbdevup=std, nbdevdn=std)
        return pd.DataFrame({
            'BBU_20_2.0': upper,
            'BBM_20_2.0': middle,
            'BBL_20_2.0': lower
        })

    @staticmethod
    def sma(close, length=None):
        """Simple Moving Average for pandas_ta compatibility"""
        return TA.SMA(close, timeperiod=length)

    @staticmethod
    def ema(close, length=None):
        """Exponential Moving Average for pandas_ta compatibility"""
        return TA.EMA(close, timeperiod=length)

    @staticmethod
    def WMA(series, timeperiod=None):
        """Weighted Moving Average"""
        weights = np.arange(1, timeperiod + 1)
        result = pd.Series(index=series.index)

        for i in range(timeperiod - 1, len(series)):
            window = series.iloc[i - timeperiod + 1:i + 1]
            result.iloc[i] = np.sum(weights * window) / weights.sum()

        return result

    @staticmethod
    def DEMA(series, timeperiod=None):
        """Double Exponential Moving Average"""
        ema = series.ewm(span=timeperiod, adjust=False).mean()
        return 2 * ema - ema.ewm(span=timeperiod, adjust=False).mean()

    @staticmethod
    def TEMA(series, timeperiod=None):
        """Triple Exponential Moving Average"""
        ema1 = series.ewm(span=timeperiod, adjust=False).mean()
        ema2 = ema1.ewm(span=timeperiod, adjust=False).mean()
        ema3 = ema2.ewm(span=timeperiod, adjust=False).mean()
        return 3 * ema1 - 3 * ema2 + ema3

    @staticmethod
    def KAMA(close, timeperiod=10, fast_period=2, slow_period=30):
        """Kaufman Adaptive Moving Average"""
        # Calculate efficiency ratio
        change = abs(close - close.shift(timeperiod))
        volatility = (abs(close - close.shift(1))).rolling(window=timeperiod).sum()
        er = change / volatility.replace(0, np.finfo(float).eps)

        # Calculate smoothing constant
        fast_sc = 2/(fast_period + 1)
        slow_sc = 2/(slow_period + 1)
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2

        # Calculate KAMA
        kama = pd.Series(index=close.index)
        kama.iloc[timeperiod-1] = close.iloc[timeperiod-1]
        for i in range(timeperiod, len(close)):
            kama.iloc[i] = kama.iloc[i-1] + sc.iloc[i] * (close.iloc[i] - kama.iloc[i-1])

        return kama

    @staticmethod
    def buyer_seller_initiated_trades(close, volume):
        """
        Calculate buyer/seller initiated trades and related metrics.
        Assumes 'close' is a pandas Series of prices and 'volume' is a pandas Series of volumes.
        """
        price_diff = close.diff()
        buyer_initiated = volume.where(price_diff > 0, 0)  # Volume when price increases
        seller_initiated = volume.where(price_diff < 0, 0)  # Volume when price decreases

        buyer_initiated_trades = (buyer_initiated > 0).astype(int).cumsum()
        seller_initiated_trades = (seller_initiated > 0).astype(int).cumsum()

        buyer_initiated_quantity = buyer_initiated.cumsum()
        seller_initiated_quantity = seller_initiated.cumsum()

        buyer_initiated_avg_qty = buyer_initiated_quantity / buyer_initiated_trades.replace(0, np.nan)
        seller_initiated_avg_qty = seller_initiated_quantity / seller_initiated_trades.replace(0, np.nan)

        buyer_seller_ratio = buyer_initiated_trades / seller_initiated_trades.replace(0, np.nan)
        buyer_seller_quantity_ratio = buyer_initiated_quantity / seller_initiated_quantity.replace(0, np.nan)

        buyer_initiated_vwap = (close * buyer_initiated).cumsum() / buyer_initiated_quantity.replace(0, np.nan)
        seller_initiated_vwap = (close * seller_initiated).cumsum() / seller_initiated_quantity.replace(0, np.nan)

        return {
            "buyer_initiated_trades": buyer_initiated_trades,
            "buyer_initiated_quantity": buyer_initiated_quantity,
            "buyer_initiated_avg_qty": buyer_initiated_avg_qty,
            "seller_initiated_trades": seller_initiated_trades,
            "seller_initiated_quantity": seller_initiated_quantity,
            "seller_initiated_avg_qty": seller_initiated_avg_qty,
            "buyer_seller_ratio": buyer_seller_ratio,
            "buyer_seller_quantity_ratio": buyer_seller_quantity_ratio,
            "buyer_initiated_vwap": buyer_initiated_vwap,
            "seller_initiated_vwap": seller_initiated_vwap,
        }

    @staticmethod
    def calculate_order_metrics(orders, prices):
        """
        Calculate various order-related metrics.
        :param orders: DataFrame with columns ['buy_orders', 'sell_orders', 'buy_qty', 'sell_qty', 'cancelled_buy_orders', 'cancelled_sell_orders']
        :param prices: Series of prices corresponding to the orders.
        :return: Dictionary of calculated metrics.
        """
        metrics = {}

        # Total orders and quantities
        metrics['total_orders'] = orders['buy_orders'] + orders['sell_orders']
        metrics['total_orders_quantity'] = orders['buy_qty'] + orders['sell_qty']

        # Buy vs Sell ratios
        metrics['buy_sell_orders_ratio'] = orders['buy_orders'] / orders['sell_orders'].replace(0, np.nan)
        metrics['buy_sell_quantity_ratio'] = orders['buy_qty'] / orders['sell_qty'].replace(0, np.nan)

        # Cancelled orders and quantities
        metrics['total_cancelled_orders'] = orders['cancelled_buy_orders'] + orders['cancelled_sell_orders']
        metrics['total_cancelled_quantity'] = orders['cancelled_buy_qty'] + orders['cancelled_sell_qty']
        metrics['cancelled_orders_ratio'] = metrics['total_cancelled_orders'] / metrics['total_orders'].replace(0, np.nan)
        metrics['cancelled_quantity_ratio'] = metrics['total_cancelled_quantity'] / metrics['total_orders_quantity'].replace(0, np.nan)

        # VWAP calculations
        metrics['buy_orders_vwap'] = (prices * orders['buy_qty']).cumsum() / orders['buy_qty'].cumsum().replace(0, np.nan)
        metrics['sell_orders_vwap'] = (prices * orders['sell_qty']).cumsum() / orders['sell_qty'].cumsum().replace(0, np.nan)
        metrics['orders_vwap'] = (prices * metrics['total_orders_quantity']).cumsum() / metrics['total_orders_quantity'].cumsum().replace(0, np.nan)

        return metrics

    @staticmethod
    def WAVE_TREND(high, low, close, channel_length=10, average_length=21):
        """
        Calculate Wave Trend (WT), Wave Trend Trigger (WTT), and Wave Trend Momentum (WTM).
        """
        hlc3 = (high + low + close) / 3  # Typical price
        esa = hlc3.ewm(span=channel_length, adjust=False).mean()  # Exponential smoothing
        de = abs(hlc3 - esa).ewm(span=channel_length, adjust=False).mean()  # Deviation
        ci = (hlc3 - esa) / (0.015 * de.replace(0, np.finfo(float).eps))  # Channel Index
        wt = ci.ewm(span=average_length, adjust=False).mean()  # Wave Trend
        wt_trigger = wt.shift(1)  # Trigger line
        wt_momentum = wt - wt_trigger  # Momentum

        return wt, wt_trigger, wt_momentum

# Create a singleton instance
ta = TA()
