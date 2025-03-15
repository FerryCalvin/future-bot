import requests
import pandas as pd
import logging
import certifi
from trading_bot.config.config import BYBIT_BASE_URL, BYBIT_API_KEY

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_ohlcv(symbol="BTCUSDT", interval="1", limit=200, verify_ssl=True):
    """
    Fetches OHLCV (candlestick) data from Bybit.

    :param symbol: Trading pair (default: BTCUSDT)
    :param interval: Time frame interval in minutes (e.g., '1' for 1 minute)
    :param limit: Number of candlesticks to fetch (max: 200)
    :param verify_ssl: Whether to verify SSL certificates (default: True)
    :return: DataFrame with OHLCV data or None if an error occurs
    """
    url = f"{BYBIT_BASE_URL}/v5/market/kline"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
        "category": "linear"  # Required for USDT perpetual futures data
    }
    headers = {
        "X-BYBIT-API-KEY": BYBIT_API_KEY
    }
    
    logging.info(f"Requesting OHLCV data from {url} with params: {params}")
    
    try:
        verify_param = certifi.where() if verify_ssl else False
        response = requests.get(url, params=params, headers=headers, verify=verify_param)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("retCode") == 0:
                raw_candles = data["result"]["list"]
                if not raw_candles:
                    logging.error("❌ Received empty OHLCV data.")
                    return None
                # Convert to DataFrame (expecting 7 columns)
                df = pd.DataFrame(raw_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
                # Convert 'timestamp' column to numeric (coerce errors), then drop invalid rows
                df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
                df = df.dropna(subset=['timestamp'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'turnover']
                df[numeric_cols] = df[numeric_cols].astype(float)
                logging.info(f"✅ Successfully fetched {len(df)} OHLCV data points for {symbol} ({interval} minute candles)")
                return df
            else:
                logging.error(f"❌ Bybit API Error: {data.get('retMsg')}")
        else:
            logging.error(f"❌ HTTP Error {response.status_code}: {response.text}")
            
    except requests.exceptions.SSLError as ssl_err:
        logging.error(f"❌ SSL Error: {ssl_err}")
        if verify_ssl:
            logging.info("🔄 Retrying without SSL verification...")
            return fetch_ohlcv(symbol, interval, limit, verify_ssl=False)
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Request Error: {e}")
        
    return None

# Example usage
if __name__ == "__main__":
    df = fetch_ohlcv("BTCUSDT", "1", 200)
    if df is not None and not df.empty:
        print(df.head())
    else:
        logging.warning("No OHLCV data returned.")
