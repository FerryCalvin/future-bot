import websocket
import json
import threading
import time
import logging
import ssl
from trading_bot.config.config import USE_TESTNET

# Select the correct WebSocket URL based on environment
BYBIT_WS_URL = (
    "wss://stream-testnet.bybit.com/v5/public/linear"
    if USE_TESTNET
    else "wss://stream.bybit.com/v5/public/linear"
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def on_message(ws, message):
    """Handle incoming messages from the WebSocket."""
    try:
        data = json.loads(message)
        logging.info(f"📡 Received: {data}")
    except Exception as e:
        logging.error(f"❌ Error processing message: {e}")

def on_error(ws, error):
    """Handle WebSocket errors."""
    logging.error(f"❌ WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket closure and attempt reconnection."""
    logging.warning(f"🔌 WebSocket closed (code: {close_status_code}, message: {close_msg})")
    logging.info("🔄 Reconnecting in 5 seconds...")
    time.sleep(5)
    start_websocket()

def on_open(ws):
    """Subscribe to desired channels once the connection opens."""
    logging.info("✅ WebSocket connected. Subscribing to channels...")
    payload = {
        "op": "subscribe",
        "args": [
            "kline.1.BTCUSDT",      # 1-minute candlestick data for BTCUSDT
            "orderbook.50.BTCUSDT"   # Order book data (50 levels) for BTCUSDT
        ]
    }
    ws.send(json.dumps(payload))
    logging.info(f"📩 Subscription payload sent: {payload}")

def start_websocket():
    """Starts the WebSocket connection with auto-reconnect logic."""
    try:
        ws = websocket.WebSocketApp(
            BYBIT_WS_URL,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.on_open = on_open
        # For development only: disable SSL certificate verification
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
    except Exception as e:
        logging.error(f"🚨 Critical WebSocket error: {e}")
        logging.info("🔄 Attempting to reconnect in 5 seconds...")
        time.sleep(5)
        start_websocket()

if __name__ == "__main__":
    # Run WebSocket in a daemon thread
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()
    logging.info("🚀 WebSocket thread started. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("🛑 WebSocket stopped by user.")
