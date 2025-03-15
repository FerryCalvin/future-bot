import os
import threading
from dotenv import load_dotenv
import pandas as pd

# Importing configurations and utilities
from trading_bot.config.config import (
    BYBIT_API_KEY, BYBIT_SECRET_KEY, MONGO_URI, DB_NAME, BYBIT_BASE_URL
)
from trading_bot.data_fetcher.fetch_ohlcv import fetch_ohlcv
from trading_bot.data_fetcher.fetch_orderbook import fetch_orderbook
from trading_bot.data_fetcher.fetch_realtime import start_websocket
from trading_bot.database.mongodb_setup import save_ohlcv, save_orderbook
from trading_bot.utils.logger import setup_logger

# Importing analysis modules
from trading_bot.analysis.pattern_analysis import detect_candlestick_patterns, aggregate_patterns
from trading_bot.analysis.news_analysis import analyze_news_sentiment

# Load environment variables
load_dotenv()

# Setup logger
logger = setup_logger()

def generate_signal(bullish_score, bearish_score, news_sentiment):
    """
    Generates a trading signal based on candlestick pattern analysis and news sentiment.
    Returns:
        - "BUY" if bullish signals and positive sentiment dominate.
        - "SELL" if bearish signals and negative sentiment dominate.
        - "NEUTRAL" if no dominant trend is detected.
    """
    if bullish_score > bearish_score and news_sentiment > 0.2:
        return "BUY"
    elif bearish_score > bullish_score and news_sentiment < -0.2:
        return "SELL"
    else:
        return "NEUTRAL"

def main():
    logger.info("🚀 Starting Bybit trading bot...")

    # --- Fetch & Store Market Data ---
    # Fetch OHLCV Data (using interval "1" for 1 minute candles)
    try:
        logger.info("📊 Fetching OHLCV data...")
        ohlcv_data = fetch_ohlcv("BTCUSDT", "1", 200)  # Fetch last 200 1-minute candles
        if ohlcv_data is not None and not ohlcv_data.empty:
            save_ohlcv(ohlcv_data, "BTCUSDT", "1")
            logger.info("✅ Successfully fetched and saved OHLCV data.")
        else:
            logger.warning("⚠️ No OHLCV data received.")
    except Exception as e:
        logger.error(f"❌ Error fetching OHLCV data: {e}")
        ohlcv_data = None

    # Fetch Order Book Data
    try:
        logger.info("📖 Fetching Order Book data...")
        orderbook_data = fetch_orderbook("BTCUSDT", 50)  # Fetch top 50 order book levels
        if orderbook_data:
            save_orderbook(orderbook_data, "BTCUSDT")
            logger.info("✅ Successfully fetched and saved Order Book data.")
        else:
            logger.warning("⚠️ No Order Book data received.")
    except Exception as e:
        logger.error(f"❌ Error fetching Order Book data: {e}")

    # --- Candlestick Pattern Analysis ---
    try:
        bullish_score, bearish_score = 0, 0  # Default values if no data
        if ohlcv_data is not None and not ohlcv_data.empty:
            logger.info("🔎 Analyzing candlestick patterns...")
            # Use only the columns needed for pattern detection
            df_patterns = ohlcv_data[["open", "high", "low", "close"]].copy()
            patterns = detect_candlestick_patterns(df_patterns)
            bullish_score, bearish_score = aggregate_patterns(patterns)
            logger.info(f"✅ Bullish Score: {bullish_score}, Bearish Score: {bearish_score}")
        else:
            logger.warning("⚠️ Skipping candlestick analysis due to missing OHLCV data.")
    except Exception as e:
        logger.error(f"❌ Error in pattern analysis: {e}")

    # --- News Sentiment Analysis ---
    try:
        total_sentiment = 0  # Default sentiment value
        logger.info("🔎 Analyzing news sentiment...")
        # Example using CoinDesk RSS feed
        rss_url = "https://www.coindesk.com/arc/outboundfeeds/rss/"
        news_results = analyze_news_sentiment(rss_url)
        if news_results:
            total_sentiment = sum(s for _, s in news_results) / len(news_results)
            logger.info(f"✅ Average News Sentiment: {total_sentiment:.2f}")
        else:
            logger.warning("⚠️ No news data received for sentiment analysis.")
    except Exception as e:
        logger.error(f"❌ Error in news sentiment analysis: {e}")

    # --- Generate Trading Signal ---
    signal = generate_signal(bullish_score, bearish_score, total_sentiment)
    logger.info(f"📈 Trading Signal: {signal}")

    # --- Start WebSocket for Real-Time Data ---
    try:
        logger.info("📡 Starting WebSocket for real-time data...")
        ws_thread = threading.Thread(target=start_websocket, daemon=True)
        ws_thread.start()
    except Exception as e:
        logger.error(f"❌ Error starting WebSocket: {e}")

if __name__ == "__main__":
    main()
