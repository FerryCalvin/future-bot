import requests
import logging
import pandas as pd
import certifi
from trading_bot.config.config import BYBIT_BASE_URL, BYBIT_API_KEY

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_orderbook(symbol="BTCUSDT", depth=50, verify_ssl=True):
    """
    Fetches the order book data from Bybit.

    :param symbol: Trading pair (default: BTCUSDT)
    :param depth: Number of order levels to retrieve (max: 200)
    :param verify_ssl: Whether to verify SSL certificates (default: True)
    :return: Dictionary with order book data or None if an error occurs
    """
    url = f"{BYBIT_BASE_URL}/v5/market/orderbook"
    params = {
        "symbol": symbol,
        "limit": depth,
        "category": "linear"  # Set category for futures data
    }
    headers = {
        "X-BYBIT-API-KEY": BYBIT_API_KEY
    }
    
    logging.info(f"Requesting Order Book data from {url} with params: {params}")
    
    try:
        verify_param = certifi.where() if verify_ssl else False
        response = requests.get(url, params=params, headers=headers, verify=verify_param)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("retCode") == 0:
                orderbook_data = data["result"]
                logging.info(f"✅ Successfully fetched Order Book data for {symbol}")
                return orderbook_data
            else:
                logging.error(f"❌ Bybit API Error: {data.get('retMsg')}")
        else:
            logging.error(f"❌ HTTP Error {response.status_code}: {response.text}")
            
    except requests.exceptions.SSLError as ssl_err:
        logging.error(f"❌ SSL Error: {ssl_err}")
        if verify_ssl:
            logging.info("🔄 Retrying without SSL verification...")
            return fetch_orderbook(symbol, depth, verify_ssl=False)
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Request Error: {e}")
        
    return None

# Example usage
if __name__ == "__main__":
    orderbook = fetch_orderbook("BTCUSDT", 50)
    if orderbook:
        # Convert bids and asks into DataFrames for display
        bids = pd.DataFrame(orderbook.get("b", []), columns=["price", "size"]).astype(float)
        asks = pd.DataFrame(orderbook.get("a", []), columns=["price", "size"]).astype(float)
        print("Bids:\n", bids.head())
        print("Asks:\n", asks.head())
