import feedparser
import logging
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_news(rss_url, max_entries=20):
    """
    Fetches news articles from an RSS feed.
    
    Args:
        rss_url (str): The RSS feed URL.
        max_entries (int): Maximum number of news articles to retrieve.
    
    Returns:
        list: A list of news entries (dicts).
    """
    try:
        feed = feedparser.parse(rss_url)
        if not feed.entries:
            logging.warning("No news entries found in the RSS feed.")
            return []
        return feed.entries[:max_entries]
    except Exception as e:
        logging.error(f"Error fetching news: {e}")
        return []

def analyze_sentiment(text):
    """
    Analyzes sentiment of a given text using VADER.
    
    Args:
        text (str): The text to analyze.
    
    Returns:
        float: Sentiment compound score (-1 to 1).
    """
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound']

def analyze_news_sentiment(rss_url):
    """
    Fetches news from an RSS feed and analyzes sentiment.
    
    Args:
        rss_url (str): The RSS feed URL.
    
    Returns:
        list: A list of tuples (title, sentiment_score).
    """
    entries = fetch_news(rss_url)
    results = []
    
    for entry in entries:
        title = entry.get("title", "No Title")
        description = entry.get("description", "")
        
        # Combine title and description for better sentiment analysis
        full_text = f"{title}. {description}" if description else title
        sentiment = analyze_sentiment(full_text)
        
        results.append((title, sentiment))
        logging.info(f"News: {title} | Sentiment: {sentiment}")
    
    return results

if __name__ == "__main__":
    # Example usage with CoinDesk RSS feed
    rss_url = "https://www.coindesk.com/arc/outboundfeeds/rss/"
    news_results = analyze_news_sentiment(rss_url)

    for title, sentiment in news_results:
        print(f"News: {title}\nSentiment: {sentiment}\n")
