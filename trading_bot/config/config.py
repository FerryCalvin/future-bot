import os
from dotenv import load_dotenv

# Load .env file from the root directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ENV_PATH = os.path.join(BASE_DIR, ".env")

if os.path.exists(ENV_PATH):
    load_dotenv(dotenv_path=ENV_PATH)
else:
    raise FileNotFoundError("❌ .env file not found! Please create it and provide necessary credentials.")

# Bybit API Keys (Must be set in .env)
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_SECRET_KEY = os.getenv("BYBIT_SECRET_KEY")

if not BYBIT_API_KEY or not BYBIT_SECRET_KEY:
    raise ValueError("❌ API Key and Secret Key not found! Ensure they are set in .env.")

# Database Configuration (Default values provided)
MONGO_URI = os.getenv("MONGO_URI", "your mongo uri")
DB_NAME = os.getenv("DB_NAME", "your database name")

# Bybit API Endpoints
BYBIT_MAINNET_URL = "https://api.bybit.com"
BYBIT_TESTNET_URL = "https://api-testnet.bybit.com"

# Choose whether to use Testnet or Live Trading
USE_TESTNET = os.getenv("USE_TESTNET", "True").lower() in ("true", "1")
BYBIT_BASE_URL = BYBIT_TESTNET_URL if USE_TESTNET else BYBIT_MAINNET_URL

# Logging confirmation (but avoids printing API keys)
print(f"✅ Using Bybit API: {'Testnet' if USE_TESTNET else 'Mainnet'}")
