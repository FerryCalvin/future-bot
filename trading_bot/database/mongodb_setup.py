from pymongo import MongoClient
import logging
from trading_bot.config.config import MONGO_URI, DB_NAME

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Collection names
OHLCV_COLLECTION = "ohlcv"
ORDERBOOK_COLLECTION = "orderbook"

def get_database():
    """
    Connects to MongoDB and returns the database reference.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        logging.info("✅ Connected to MongoDB successfully!")
        return db, client
    except Exception as e:
        logging.error(f"❌ Failed to connect to MongoDB: {e}")
        return None, None

def save_ohlcv(data, symbol="BTCUSDT", interval="1"):
    """
    Saves OHLCV data to MongoDB.
    
    :param data: List of candlesticks from Bybit API.
    :param symbol: Trading pair.
    :param interval: Timeframe interval.
    """
    db, client = get_database()
    if db is None:
        logging.error("❌ No database connection.")
        return
    
    collection = db[OHLCV_COLLECTION]

    # Format data before saving
    formatted_data = []
    for candle in data:
        formatted_data.append({
            "symbol": symbol,
            "interval": interval,
            "timestamp": int(candle[0]),  # Timestamp
            "open": float(candle[1]),
            "high": float(candle[2]),
            "low": float(candle[3]),
            "close": float(candle[4]),
            "volume": float(candle[5])
        })

    if formatted_data:
        try:
            # Use bulk insert with upsert to avoid duplicates
            for record in formatted_data:
                collection.update_one(
                    {"symbol": record["symbol"], "interval": record["interval"], "timestamp": record["timestamp"]},
                    {"$set": record},
                    upsert=True
                )
            logging.info(f"✅ Inserted {len(formatted_data)} OHLCV data points for {symbol} ({interval}m)")
        except Exception as e:
            logging.error(f"❌ Failed to insert OHLCV data: {e}")

    client.close()

def save_orderbook(data, symbol="BTCUSDT"):
    """
    Saves Order Book data to MongoDB.
    
    :param data: Order book data from Bybit API.
    :param symbol: Trading pair.
    """
    db, client = get_database()
    if db is None:
        logging.error("❌ No database connection.")
        return
    
    collection = db[ORDERBOOK_COLLECTION]
    
    # Ensure required keys exist
    if "ts" not in data or "b" not in data or "a" not in data:
        logging.error("❌ Invalid order book data format.")
        return

    orderbook_entry = {
        "symbol": symbol,
        "timestamp": int(data["ts"]),
        "bids": data["b"],  # List of bid orders
        "asks": data["a"]   # List of ask orders
    }

    try:
        collection.insert_one(orderbook_entry)
        logging.info(f"✅ Inserted Order Book data for {symbol}")
    except Exception as e:
        logging.error(f"❌ Failed to insert Order Book data: {e}")

    client.close()

# Example usage
if __name__ == "__main__":
    from data_fetcher.fetch_ohlcv import fetch_ohlcv
    from data_fetcher.fetch_orderbook import fetch_orderbook

    # Save OHLCV data
    candles = fetch_ohlcv("BTCUSDT", "1", 10)
    if candles:
        save_ohlcv(candles, "BTCUSDT", "1")

    # Save Order Book data
    orderbook = fetch_orderbook("BTCUSDT", 10)
    if orderbook:
        save_orderbook(orderbook, "BTCUSDT")
