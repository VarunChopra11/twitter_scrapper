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

# Import the Twitter scraper components
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

# Load environment variables
load_dotenv()

KEYWORDS = [
    "Ethereum", "ETH", "Bitcoin", "BTC", "SOL", "Bored Ape", "BAYC", "Polygon", "Chainlink", "LINK", "Shiba Inu", "SHIB", "Uniswap", "UNI"
]

# Twitter credentials
TWITTER_MAIL = os.getenv("TWITTER_MAIL")
TWITTER_USERNAME = os.getenv("TWITTER_USERNAME") 
TWITTER_PASSWORD = os.getenv("TWITTER_PASSWORD")
HEADLESS_MODE = os.getenv("HEADLESS", "yes")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "twitter_analytics")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "analytics_data")

TWITTER_LOGIN_URL = "https://twitter.com/i/flow/login"

# MongoDB client setup
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Download TextBlob corpora if needed
    try:
        print("Checking TextBlob corpora...")
        from textblob.download_corpora import download_all
        download_all()
        print("TextBlob corpora downloaded")
    except Exception as e:
        print(f"Error downloading corpora: {e}")
    
    # Setup scheduler
    scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_analytics_and_store,
        CronTrigger(hour=8, minute=5),  # 08:05 AM IST
        name="daily_analytics"
    )
    scheduler.start()
    print("Scheduler started - will run daily at  08:05 PM IST")
    
    yield
    
    # Cleanup
    client.close()
    scheduler.shutdown()
    print("MongoDB connection closed and scheduler shutdown")

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
        """Setup Firefox WebDriver"""
        print("Setup WebDriver...")
        
        # User agent of an Android smartphone device
        header = "Mozilla/5.0 (Linux; Android 11; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.5414.87 Mobile Safari/537.36"
        
        browser_option = FirefoxOptions()
        browser_option.add_argument("--no-sandbox")
        browser_option.add_argument("--disable-dev-shm-usage")
        browser_option.add_argument("--ignore-certificate-errors")
        browser_option.add_argument("--disable-gpu")
        browser_option.add_argument("--log-level=3")
        browser_option.add_argument("--disable-notifications")
        browser_option.add_argument("--disable-popup-blocking")
        browser_option.add_argument("--user-agent={}".format(header))
        
        # Option to hide browser or not
        if self.headless_state == 'yes':
            browser_option.add_argument("--headless")
        
        try:
            print("Initializing FirefoxDriver...")
            driver = webdriver.Firefox(options=browser_option)
            print("WebDriver Setup Complete")
            return driver
        except WebDriverException:
            try:
                print("Downloading FirefoxDriver...")
                firefoxdriver_path = GeckoDriverManager().install()
                firefox_service = FirefoxService(executable_path=firefoxdriver_path)
                
                print("Initializing FirefoxDriver...")
                driver = webdriver.Firefox(
                    service=firefox_service,
                    options=browser_option,
                )
                print("WebDriver Setup Complete")
                return driver
            except Exception as e:
                print(f"Error setting up WebDriver: {e}")
                raise e

    def login(self):
        """Login to Twitter"""
        print("Logging in to Twitter...")
        
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
            
            print("Login Successful")
            return True
            
        except Exception as e:
            print(f"Login Failed: {e}")
            if self.driver:
                self.driver.quit()
            raise e

    def _input_username(self):
        """Input username during login"""
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
                    print("Re-attempting to input username...")
                    sleep(2)

    def _input_unusual_activity(self):
        """Handle unusual activity check if it appears"""
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
        """Input password during login"""
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
                    print("Re-attempting to input password...")
                    sleep(2)

    def scrape_keyword_tweets(self, keyword: str, max_tweets: int = 20) -> List[Dict[str, Any]]:
        """Scrape tweets for a specific keyword"""
        print(f"Scraping tweets for keyword: {keyword}")
        
        # Calculate date range (last 3 days)
        today = date.today()
        three_days_ago = today - timedelta(days=3)
        
        # Navigate to search
        search_query = f"{keyword} since:{three_days_ago} until:{today}"
        search_url = f"https://twitter.com/search?q={search_query.replace(' ', '%20')}&src=typed_query&f=live"
        
        print(f"Navigating to: {search_url}")
        self.driver.get(search_url)
        sleep(5)
        
        # Accept cookies if banner appears
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
                # Get tweet cards
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
                        
                        # Extract tweet data
                        tweet_data = self._extract_tweet_data(card)
                        if tweet_data and not tweet_data.get('is_ad', False):
                            tweets_data.append(tweet_data)
                            added_tweets += 1
                            print(f"Extracted tweet {len(tweets_data)}/{max_tweets}")
                            
                    except Exception as e:
                        print(f"Error extracting tweet: {e}")
                        continue
                
                if added_tweets == 0:
                    scroll_attempts += 1
                    print(f"No new tweets found, scroll attempt {scroll_attempts}")
                    
                    # Try to click retry button if it exists
                    try:
                        retry_button = self.driver.find_element("xpath", "//span[text()='Retry']/../../..")
                        retry_button.click()
                        sleep(3)
                    except NoSuchElementException:
                        pass
                    
                    # Scroll down
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    sleep(3)
                else:
                    scroll_attempts = 0
                    
            except Exception as e:
                print(f"Error during scraping: {e}")
                scroll_attempts += 1
                continue
        
        print(f"Scraped {len(tweets_data)} tweets for {keyword}")
        return tweets_data

    def _extract_tweet_data(self, card) -> Dict[str, Any]:
        """Extract data from a tweet card"""
        try:
            # Check if it's an ad
            try:
                card.find_element("xpath", ".//time")
                is_ad = False
            except NoSuchElementException:
                is_ad = True
                
            if is_ad:
                return {"is_ad": True}
            
            # Extract user handle
            user_handle = ""
            try:
                user_handle = card.find_element("xpath", './/span[contains(text(), "@")]').text
            except NoSuchElementException:
                user_handle = "unknown"
            
            # Extract content
            content = ""
            try:
                content_elements = card.find_elements("xpath", '(.//div[@data-testid="tweetText"])[1]/span | (.//div[@data-testid="tweetText"])[1]/a')
                content = "".join([elem.text for elem in content_elements])
            except NoSuchElementException:
                content = ""
            
            # Extract engagement metrics
            replies = 0
            retweets = 0
            likes = 0
            
            try:
                reply_element = card.find_element("xpath", './/button[@data-testid="reply"]//span')
                reply_text = reply_element.text.strip()
                replies = int(reply_text.replace(",", "")) if reply_text.isdigit() else 0
            except (NoSuchElementException, ValueError):
                replies = 0
            
            try:
                retweet_element = card.find_element("xpath", './/button[@data-testid="retweet"]//span')
                retweet_text = retweet_element.text.strip()
                retweets = int(retweet_text.replace(",", "")) if retweet_text.isdigit() else 0
            except (NoSuchElementException, ValueError):
                retweets = 0
            
            try:
                like_element = card.find_element("xpath", './/button[@data-testid="like"]//span')
                like_text = like_element.text.strip()
                likes = int(like_text.replace(",", "")) if like_text.isdigit() else 0
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
            print(f"Error extracting tweet data: {e}")
            return None

    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()


def get_keyword_data(keyword: str) -> Dict[str, Any]:
    """Get tweet data for a specific keyword and perform sentiment analysis"""
    try:
        # Initialize scraper
        scraper = TwitterScraperForAnalytics(TWITTER_MAIL, TWITTER_USERNAME, TWITTER_PASSWORD, HEADLESS_MODE)
        scraper.login()
        
        # Scrape tweets
        tweets = scraper.scrape_keyword_tweets(keyword, max_tweets=50)
        
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
        
        # Close browser
        scraper.close()
        
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
        print(f"Error analyzing {keyword}: {str(e)}")
        return {"keyword": keyword, "error": str(e)}

def run_analytics_and_store():
    """Run analytics and store results in MongoDB"""
    print("Running scheduled analytics job...")
    
    if not all([TWITTER_MAIL, TWITTER_USERNAME, TWITTER_PASSWORD]):
        print("Twitter credentials not configured. Skipping scheduled job.")
        return
    
    try:
        results = []
        for keyword in KEYWORDS:
            print(f"Processing keyword: {keyword}")
            result = get_keyword_data(keyword)
            results.append(result)
        
        document = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "data": results
        }
        
        # Insert into MongoDB
        collection.insert_one(document)
        print(f"Inserted analytics data at {document['timestamp']}")
        
    except Exception as e:
        print(f"Error in scheduled job: {str(e)}")

@app.get("/analytics", response_class=JSONResponse)
async def get_analytics():
    """Endpoint to manually trigger analytics"""
    if not all([TWITTER_MAIL, TWITTER_USERNAME, TWITTER_PASSWORD]):
        return {
            "error": "Twitter credentials not configured. Please set environment variables."
        }
    
    try:
        results = []
        for keyword in KEYWORDS:
            result = get_keyword_data(keyword)
            results.append(result)
            
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
        
        # Remove MongoDB ID
        last_entry.pop("_id", None)
        return last_entry
        
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "keywords": KEYWORDS}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
