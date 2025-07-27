import logging
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from typing import List, Dict, Any
from textblob import TextBlob
from datetime import date, timedelta
import os
import pandas as pd
import pymongo
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import traceback
import requests

# -------------------- NEW: Logging setup --------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------

load_dotenv(override=True)

KEYWORDS = [
    "Ethereum", "ETH", "Bitcoin", "BTC", "SOL", "Bored Ape", "BAYC",
    "Polygon", "Chainlink", "LINK", "Shiba Inu", "SHIB", "Uniswap", "UNI"
]

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "twitter_analytics")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "analytics_data")

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        logger.info("Checking TextBlob corpora...")
        from textblob.download_corpora import download_all
        download_all()
        logger.info("TextBlob corpora downloaded")
    except Exception as e:
        logger.error(f"Error downloading corpora: {e}")

    scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_analytics_and_store,
        CronTrigger(hour=8, minute=5),
        name="daily_analytics"
    )
    scheduler.start()
    logger.info("Scheduler started - will run daily at 08:05 AM IST")

    yield

    client.close()
    scheduler.shutdown()
    logger.info("MongoDB connection closed and scheduler shutdown")

app = FastAPI(lifespan=lifespan)

# -------------------- NEW: Twitter API Client --------------------
def fetch_tweets_for_keyword(keyword: str, max_tweets: int = 20) -> List[Dict[str, Any]]:
    """Fetch tweets for a keyword using Twitter API"""
    logger.info(f"Fetching tweets for keyword: {keyword}")
    
    today = date.today()
    three_days_ago = today - timedelta(days=3)
    
    params = {
        "query": keyword,
        "since": three_days_ago.strftime('%Y-%m-%d'),
        "until": today.strftime('%Y-%m-%d'),
        "limit": max_tweets
    }
    
    headers = {
        "X-API-Key": TWITTER_API_KEY
    }
    
    try:
        response = requests.get(
            "https://api.twitterapi.io/twitter/tweet/advanced_search",
            headers=headers,
            params=params,
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"API request failed: {response.status_code} - {response.text}")
            return []
        
        data = response.json()
        tweets = data.get('tweets', [])
        logger.info(f"Fetched {len(tweets)} tweets for {keyword}")
        return tweets[:max_tweets]
        
    except Exception as e:
        logger.error(f"Error fetching tweets for {keyword}: {str(e)}")
        return []

# -------------------- UPDATED: Keyword Data Processing --------------------
def get_keyword_data(keyword: str) -> Dict[str, Any]:
    """Get tweet data for a specific keyword and perform sentiment analysis"""
    try:
        # Fetch tweets from API
        api_tweets = fetch_tweets_for_keyword(keyword, max_tweets=20)
        
        # Convert to our expected format
        tweets = []
        for tweet in api_tweets:
            tweets.append({
                "content": tweet.get('text', ''),
                "user_handle": f"@{tweet.get('author', {}).get('userName', '')}",
                "engagements": {
                    "replies": tweet.get('replyCount', 0),
                    "retweets": tweet.get('retweetCount', 0),
                    "likes": tweet.get('likeCount', 0)
                },
                "is_ad": False
            })
        
        # Perform sentiment analysis
        sentiments = []
        for tweet in tweets:
            if tweet and not tweet.get('is_ad', False) and tweet.get('content'):
                analysis = TextBlob(tweet['content'])
                sentiments.append(analysis.sentiment.polarity)
        
        # Calculate metrics
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
        positive_count = sum(1 for s in sentiments if s > 0.2)
        negative_count = sum(1 for s in sentiments if s < -0.2)
        neutral_count = len(sentiments) - positive_count - negative_count
        
        return {
            "keyword": keyword,
            "tweet_count": len(tweets),
            "average_sentiment": avg_sentiment,
            "sentiment_distribution": {
                "positive": positive_count,
                "neutral": neutral_count,
                "negative": negative_count
            }
        }
    except Exception as e:
        logger.error(f"Error analyzing {keyword}: {str(e)}")
        return {"keyword": keyword, "error": str(e)}

# -------------------- UPDATED: Analytics Function --------------------
def run_analytics_and_store():
    """Run analytics and store results incrementally in MongoDB"""
    logger.info("Running scheduled analytics job...")
    
    if not TWITTER_API_KEY:
        logger.warning("Twitter API key not configured. Skipping scheduled job.")
        return
    
    try:
        # Create initial document
        timestamp = pd.Timestamp.now().isoformat()
        base_document = {
            "timestamp": timestamp,
            "status": "in_progress",
            "data": []
        }
        result = collection.insert_one(base_document)
        document_id = result.inserted_id
        
        # Process each keyword sequentially
        for keyword in KEYWORDS:
            try:
                logger.info(f"Processing keyword: {keyword}")
                result = get_keyword_data(keyword)
                
                # Update document with new keyword data
                collection.update_one(
                    {"_id": document_id},
                    {"$push": {"data": result}}
                )
                logger.info(f"Stored data for {keyword}")

            except Exception as e:
                error_msg = f"Failed to process {keyword}: {str(e)}"
                logger.error(error_msg)
                # Store error for this keyword
                collection.update_one(
                    {"_id": document_id},
                    {"$push": {"data": {"keyword": keyword, "error": error_msg}}}
                )
        
        # Mark as completed
        collection.update_one(
            {"_id": document_id},
            {"$set": {"status": "completed"}}
        )
        logger.info(f"Completed analytics job at {timestamp}")

    except Exception as e:
        logger.error(f"Critical error in analytics job: {str(e)}")
        traceback.print_exc()
        
        # Update document with error if we have document_id
        if 'document_id' in locals():
            collection.update_one(
                {"_id": document_id},
                {"$set": {"status": "failed", "error": str(e)}}
            )

# -------------------- UPDATED: API Endpoints --------------------
@app.get("/analytics", response_class=JSONResponse)
async def get_analytics():
    """Endpoint to manually trigger analytics"""
    if not TWITTER_API_KEY:
        return {
            "error": "Twitter API key not configured. Please set environment variable."
        }
    
    try:
        results = []
        for keyword in KEYWORDS:
            try:
                logger.info(f"Processing keyword: {keyword}")
                result = get_keyword_data(keyword)
                results.append(result)
            except Exception as e:
                error_msg = f"Failed to process {keyword}: {str(e)}"
                logger.error(error_msg)
                results.append({"keyword": keyword, "error": error_msg})
        
        return results
        
    except Exception as e:
        return {"error": f"Failed to get analytics: {str(e)}"}

@app.get("/last-analytics", response_class=JSONResponse)
async def get_last_analytics():
    """Fetch last stored analytics from MongoDB"""
    try:
        last_entry = collection.find_one(
            sort=[("timestamp", pymongo.DESCENDING)]
        )
        
        if not last_entry:
            return {"error": "No analytics data found"}
        
        # Remove MongoDB ID and return
        last_entry.pop("_id", None)
        return last_entry
        
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "keywords": KEYWORDS}

@app.get("/wakeup")
async def wakeup():
    return {"status": "awake", "message": "This proxy server is awake."}

@app.head("/wakeup")
async def wakeup_head():
    return

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
