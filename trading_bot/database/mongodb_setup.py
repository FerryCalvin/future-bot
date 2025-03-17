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
    Connects to MongoDB and returns the database reference and client.
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

    :param data: DataFrame containing candlestick data.
    :param symbol: Trading pair.
    :param interval: Timeframe interval.
    """
    db, client = get_database()
    if db is None:
        logging.error("❌ No database connection.")
        return

    collection = db[OHLCV_COLLECTION]

    formatted_data = []
    # Iterate over DataFrame rows
    for idx, row in data.iterrows():
        try:
            # Convert the pandas Timestamp to integer (milliseconds)
            ts = int(row['timestamp'].timestamp() * 1000)
        except Exception as e:
            logging.error(f"❌ Error converting timestamp: {e}")
            continue

        record = {
            "symbol": symbol,
            "interval": interval,
            "timestamp": ts,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"])
        }
        formatted_data.append(record)

    if formatted_data:
        try:
            # Insert records with upsert to avoid duplicates
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

    try:
        record = {
            "symbol": symbol,
            "timestamp": int(data["ts"]),
            "bids": data["b"],
            "asks": data["a"]
        }
    except Exception as e:
        logging.error(f"❌ Error processing order book data: {e}")
        return

    try:
        collection.insert_one(record)
        logging.info(f"✅ Inserted Order Book data for {symbol}")
    except Exception as e:
        logging.error(f"❌ Failed to insert Order Book data: {e}")

    client.close()

# Example usage
if __name__ == "__main__":
    from trading_bot.data_fetcher.fetch_ohlcv import fetch_ohlcv
    from trading_bot.data_fetcher.fetch_orderbook import fetch_orderbook

    # Save OHLCV data
    df = fetch_ohlcv("BTCUSDT", "1", 10)
    if df is not None and not df.empty:
        save_ohlcv(df, "BTCUSDT", "1")

    # Save Order Book data
    orderbook = fetch_orderbook("BTCUSDT", 10)
    if orderbook:
        save_orderbook(orderbook, "BTCUSDT")
