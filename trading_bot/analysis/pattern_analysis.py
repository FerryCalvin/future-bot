import talib
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def detect_candlestick_patterns(df):
    """
    Detects various candlestick patterns using TA-Lib.
    
    Args:
        df (pd.DataFrame): DataFrame with 'open', 'high', 'low', 'close' columns.
    
    Returns:
        dict: Dictionary with each pattern's Series as values.
    """
    required_columns = {'open', 'high', 'low', 'close'}
    if not required_columns.issubset(df.columns):
        logging.error(f"DataFrame is missing required columns: {required_columns - set(df.columns)}")
        return {}

    try:
        # Ensure data types are numeric
        df = df.astype(float)

        patterns = {
            "hammer": talib.CDLHAMMER(df["open"], df["high"], df["low"], df["close"]),
            "inverted_hammer": talib.CDLINVERTEDHAMMER(df["open"], df["high"], df["low"], df["close"]),
            "engulfing": talib.CDLENGULFING(df["open"], df["high"], df["low"], df["close"]),
            "doji": talib.CDLDOJI(df["open"], df["high"], df["low"], df["close"]),
            "shooting_star": talib.CDLSHOOTINGSTAR(df["open"], df["high"], df["low"], df["close"]),
            "morning_star": talib.CDLMORNINGSTAR(df["open"], df["high"], df["low"], df["close"]),
            "evening_star": talib.CDLEVENINGSTAR(df["open"], df["high"], df["low"], df["close"]),
            "bullish_harami": talib.CDLHARAMI(df["open"], df["high"], df["low"], df["close"]),
            "bearish_harami": -talib.CDLHARAMICROSS(df["open"], df["high"], df["low"], df["close"])
        }

        return patterns

    except Exception as e:
        logging.error(f"Error detecting candlestick patterns: {e}")
        return {}

def aggregate_patterns(patterns):
    """
    Aggregates candlestick pattern signals into bullish and bearish scores.

    Args:
        patterns (dict): Dictionary of detected patterns.

    Returns:
        tuple: (bullish_score, bearish_score)
    """
    bullish_patterns = ["hammer", "inverted_hammer", "engulfing", "morning_star", "bullish_harami"]
    bearish_patterns = ["shooting_star", "evening_star", "bearish_harami"]

    bullish_score = sum(patterns[p].gt(0).sum() for p in bullish_patterns if p in patterns)
    bearish_score = sum(patterns[p].lt(0).sum() for p in bearish_patterns if p in patterns)

    return bullish_score, bearish_score

if __name__ == "__main__":
    # Example usage with dummy data
    data = {
        "open": [100, 105, 102, 103, 108],
        "high": [110, 107, 106, 104, 112],
        "low": [95, 103, 100, 101, 107],
        "close": [102, 104, 103, 102, 110]
    }

    df = pd.DataFrame(data)
    patterns = detect_candlestick_patterns(df)

    if patterns:
        bullish, bearish = aggregate_patterns(patterns)
        print("Bullish Score:", bullish)
        print("Bearish Score:", bearish)