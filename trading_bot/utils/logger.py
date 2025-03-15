import logging
import os
import sys

def setup_logger():
    """
    Sets up a logger for the trading bot with both console and file handlers.

    Returns:
        logger (logging.Logger): Configured logger instance.
    """
    logger = logging.getLogger("TradingBot")

    # Prevent adding handlers multiple times
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # Build an absolute path for the 'logs' directory at project root
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_directory = os.path.join(base_dir, "..", "logs")
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # File Handler
    log_path = os.path.join(log_directory, "bot.log")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

# Example usage
if __name__ == "__main__":
    log = setup_logger()
    log.info("🚀 Trading Bot Logger Initialized!")
    log.warning("⚠️ This is a warning message.")
    log.error("❌ This is an error message.")