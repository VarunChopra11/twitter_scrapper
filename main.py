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

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    NoSuchElementException,
    WebDriverException,
)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from time import sleep

# -------------------- NEW: Logging setup --------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------

load_dotenv()

KEYWORDS = [
    "Ethereum", "ETH", "Bitcoin", "BTC", "SOL", "Bored Ape", "BAYC",
    "Polygon", "Chainlink", "LINK", "Shiba Inu", "SHIB", "Uniswap", "UNI"
]

TWITTER_MAIL = os.getenv("TWITTER_MAIL")
TWITTER_USERNAME = os.getenv("TWITTER_USERNAME")
TWITTER_PASSWORD = os.getenv("TWITTER_PASSWORD")
HEADLESS_MODE = os.getenv("HEADLESS", "yes")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "twitter_analytics")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "analytics_data")

TWITTER_LOGIN_URL = "https://twitter.com/i/flow/login"

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

class TwitterScraperForAnalytics:
    def __init__(self, mail, username, password, headless_state="yes"):
        self.mail = mail
        self.username = username
        self.password = password
        self.headless_state = headless_state
        self.driver = None
        self.actions = None
        self.tweet_data = []

    def _get_driver(self):
        logger.info("Setup WebDriver...")
        header = "Mozilla/5.0 (Linux; Android 11; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.5414.87 Mobile Safari/537.36"

        browser_option = FirefoxOptions()
        browser_option.add_argument("--no-sandbox")
        browser_option.add_argument("--disable-dev-shm-usage")
        browser_option.add_argument("--ignore-certificate-errors")
        browser_option.add_argument("--disable-gpu")
        browser_option.add_argument("--log-level=3")
        browser_option.add_argument("--disable-notifications")
        browser_option.add_argument("--disable-popup-blocking")
        browser_option.add_argument(f"--user-agent={header}")

        if self.headless_state == 'yes':
            browser_option.add_argument("--headless")

        try:
            logger.info("Initializing FirefoxDriver...")
            driver = webdriver.Firefox(options=browser_option)
            logger.info("WebDriver Setup Complete")
            return driver
        except WebDriverException:
            try:
                logger.info("Downloading FirefoxDriver...")
                firefoxdriver_path = GeckoDriverManager().install()
                firefox_service = FirefoxService(executable_path=firefoxdriver_path)
                logger.info("Initializing FirefoxDriver...")
                driver = webdriver.Firefox(
                    service=firefox_service,
                    options=browser_option,
                )
                logger.info("WebDriver Setup Complete")
                return driver
            except Exception as e:
                logger.error(f"Error setting up WebDriver: {e}")
                raise e

    def login(self):
        logger.info("Logging in to Twitter...")

        try:
            self.driver = self._get_driver()
            self.actions = ActionChains(self.driver)

            self.driver.maximize_window()
            self.driver.execute_script("document.body.style.zoom='150%'")
            self.driver.get(TWITTER_LOGIN_URL)
            sleep(3)

            self._input_username()
            self._input_unusual_activity()
            self._input_password()

            cookies = self.driver.get_cookies()
            auth_token = None

            for cookie in cookies:
                if cookie["name"] == "auth_token":
                    auth_token = cookie["value"]
                    break

            if auth_token is None:
                raise ValueError("Login failed - no auth token found")

            logger.info("Login Successful")
            return True

        except Exception as e:
            logger.error(f"Login Failed: {e}")
            if self.driver:
                self.driver.quit()
            raise e

    def _input_username(self):
        input_attempt = 0
        while True:
            try:
                username = self.driver.find_element("xpath", "//input[@autocomplete='username']")
                username.send_keys(self.username)
                username.send_keys(Keys.RETURN)
                sleep(3)
                break
            except NoSuchElementException:
                input_attempt += 1
                if input_attempt >= 3:
                    raise Exception("Failed to input username after 3 attempts")
                else:
                    logger.warning("Re-attempting to input username...")
                    sleep(2)

    def _input_unusual_activity(self):
        input_attempt = 0
        while True:
            try:
                unusual_activity = self.driver.find_element("xpath", "//input[@data-testid='ocfEnterTextTextInput']")
                unusual_activity.send_keys(self.username)
                unusual_activity.send_keys(Keys.RETURN)
                sleep(3)
                break
            except NoSuchElementException:
                input_attempt += 1
                if input_attempt >= 3:
                    break

    def _input_password(self):
        input_attempt = 0
        while True:
            try:
                password = self.driver.find_element("xpath", "//input[@autocomplete='current-password']")
                password.send_keys(self.password)
                password.send_keys(Keys.RETURN)
                sleep(3)
                break
            except NoSuchElementException:
                input_attempt += 1
                if input_attempt >= 3:
                    raise Exception("Failed to input password after 3 attempts")
                else:
                    logger.warning("Re-attempting to input password...")
                    sleep(2)

    def scrape_keyword_tweets(self, keyword: str, max_tweets: int = 20) -> List[Dict[str, Any]]:
        logger.info(f"Scraping tweets for keyword: {keyword}")

        today = date.today()
        three_days_ago = today - timedelta(days=3)

        search_query = f"{keyword} since:{three_days_ago} until:{today}"
        search_url = f"https://twitter.com/search?q={search_query.replace(' ', '%20')}&src=typed_query&f=live"

        logger.info(f"Navigating to: {search_url}")
        self.driver.get(search_url)
        sleep(5)

        try:
            accept_cookies_btn = self.driver.find_element("xpath", "//span[text()='Refuse non-essential cookies']/../../..")
            accept_cookies_btn.click()
            sleep(2)
        except NoSuchElementException:
            pass

        tweets_data = []
        tweet_ids = set()
        scroll_attempts = 0
        max_scroll_attempts = 10

        while len(tweets_data) < max_tweets and scroll_attempts < max_scroll_attempts:
            try:
                tweet_cards = self.driver.find_elements("xpath", '//article[@data-testid="tweet" and not(@disabled)]')
                added_tweets = 0
                for card in tweet_cards:
                    if len(tweets_data) >= max_tweets:
                        break

                    try:
                        tweet_id = str(card)
                        if tweet_id in tweet_ids:
                            continue

                        tweet_ids.add(tweet_id)

                        tweet_data = self._extract_tweet_data(card)
                        if tweet_data and not tweet_data.get('is_ad', False):
                            tweets_data.append(tweet_data)
                            added_tweets += 1
                            logger.info(f"Extracted tweet {len(tweets_data)}/{max_tweets}")

                    except Exception as e:
                        logger.error(f"Error extracting tweet: {e}")
                        continue

                if added_tweets == 0:
                    scroll_attempts += 1
                    logger.warning(f"No new tweets found, scroll attempt {scroll_attempts}")

                    try:
                        retry_button = self.driver.find_element("xpath", "//span[text()='Retry']/../../..")
                        retry_button.click()
                        sleep(3)
                    except NoSuchElementException:
                        pass

                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    sleep(3)
                else:
                    scroll_attempts = 0

            except Exception as e:
                logger.error(f"Error during scraping: {e}")
                scroll_attempts += 1
                continue

        logger.info(f"Scraped {len(tweets_data)} tweets for {keyword}")
        return tweets_data

    def _extract_tweet_data(self, card) -> Dict[str, Any]:
        try:
            try:
                card.find_element("xpath", ".//time")
                is_ad = False
            except NoSuchElementException:
                is_ad = True

            if is_ad:
                return {"is_ad": True}

            user_handle = ""
            try:
                user_handle = card.find_element("xpath", './/span[contains(text(), "@")]').text
            except NoSuchElementException:
                user_handle = "unknown"

            content = ""
            try:
                content_elements = card.find_elements("xpath", '(.//div[@data-testid="tweetText"])[1]/span | (.//div[@data-testid="tweetText"])[1]/a')
                content = "".join([elem.text for elem in content_elements])
            except NoSuchElementException:
                content = ""

            replies = retweets = likes = 0

            try:
                reply_element = card.find_element("xpath", './/button[@data-testid="reply"]//span')
                replies = int(reply_element.text.strip().replace(",", "")) if reply_element.text.strip().isdigit() else 0
            except (NoSuchElementException, ValueError):
                replies = 0

            try:
                retweet_element = card.find_element("xpath", './/button[@data-testid="retweet"]//span')
                retweets = int(retweet_element.text.strip().replace(",", "")) if retweet_element.text.strip().isdigit() else 0
            except (NoSuchElementException, ValueError):
                retweets = 0

            try:
                like_element = card.find_element("xpath", './/button[@data-testid="like"]//span')
                likes = int(like_element.text.strip().replace(",", "")) if like_element.text.strip().isdigit() else 0
            except (NoSuchElementException, ValueError):
                likes = 0

            return {
                "content": content,
                "user_handle": user_handle,
                "engagements": {
                    "replies": replies,
                    "retweets": retweets,
                    "likes": likes
                },
                "is_ad": False
            }

        except Exception as e:
            logger.error(f"Error extracting tweet data: {e}")
            return None

    def close(self):
        if self.driver:
            self.driver.quit()


def get_keyword_data(keyword: str, scraper: TwitterScraperForAnalytics) -> Dict[str, Any]:
    """Get tweet data for a specific keyword and perform sentiment analysis"""
    try:
        # Scrape tweets
        tweets = scraper.scrape_keyword_tweets(keyword, max_tweets=20)
        
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
        logger.info(f"Error analyzing {keyword}: {str(e)}")
        return {"keyword": keyword, "error": str(e)}

def run_analytics_and_store():
    """Run analytics and store results incrementally in MongoDB"""
    logger.info("Running scheduled analytics job...")
    
    if not all([TWITTER_MAIL, TWITTER_USERNAME, TWITTER_PASSWORD]):
        logger.warning("Twitter credentials not configured. Skipping scheduled job.")
        return
    
    scraper = None
    try:
        # Initialize scraper
        scraper = TwitterScraperForAnalytics(TWITTER_MAIL, TWITTER_USERNAME, TWITTER_PASSWORD, HEADLESS_MODE)
        scraper.login()
        
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
                result = get_keyword_data(keyword, scraper)
                
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
    finally:
        if scraper:
            scraper.close()

@app.get("/analytics", response_class=JSONResponse)
async def get_analytics():
    """Endpoint to manually trigger analytics"""
    if not all([TWITTER_MAIL, TWITTER_USERNAME, TWITTER_PASSWORD]):
        return {
            "error": "Twitter credentials not configured. Please set environment variables."
        }
    
    scraper = None
    try:
        # Initialize scraper
        scraper = TwitterScraperForAnalytics(TWITTER_MAIL, TWITTER_USERNAME, TWITTER_PASSWORD, HEADLESS_MODE)
        scraper.login()
        
        results = []
        for keyword in KEYWORDS:
            try:
                logger.info(f"Processing keyword: {keyword}")
                result = get_keyword_data(keyword, scraper)
                results.append(result)
            except Exception as e:
                error_msg = f"Failed to process {keyword}: {str(e)}"
                logger.error(error_msg)
                results.append({"keyword": keyword, "error": error_msg})
        
        return results
        
    except Exception as e:
        return {"error": f"Failed to get analytics: {str(e)}"}
    finally:
        if scraper:
            scraper.close()

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
