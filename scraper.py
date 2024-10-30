import scrapy
from scrapy.crawler import CrawlerProcess
import json
import logging
import smtplib
from email.mime.text import MIMEText
import sqlite3
import time
import random
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv
import re
import requests
import signal
from jinja2 import Template
from lxml import html as lxml_html
from price_parser import Price
import yaml
from urllib.parse import urljoin

# ==============================
# Configuration Constants
# ==============================
DEFAULT_CONFIG = {
    'email_settings': {
        'EMAIL_SENDER': 'default@example.com',
        'EMAIL_PASSWORD': 'password',
        'SMTP_SERVER': 'smtp.example.com',
        'EMAIL_RECIPIENT': 'recipient@example.com'
    },
    'search_config': {
        'price_range': {'min': 1000, 'max': 2000}
    },
    'scrapy_settings': {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 16,
        'COOKIES_ENABLED': True,
        'USER_AGENT': 'my_scraper'
    }
}

# ==============================
# Load Environment Variables
# ==============================
load_dotenv()

# ==============================
# Configure Logging
# ==============================
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("scraper.log"),
                        logging.StreamHandler()
                    ])

# ==============================
# Load Configuration
# ==============================
def load_config(file='config.json'):
    logging.info("Loading configuration...")
    try:
        with open(file, 'r') as json_file:
            config = json.load(json_file)
            logging.info("Configuration loaded successfully.")
            return config
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Configuration file error: {e}. Using defaults.")
        return DEFAULT_CONFIG

config = load_config()

# Extract settings
EMAIL_SENDER = config['email_settings']['EMAIL_SENDER']
EMAIL_PASSWORD = config['email_settings']['EMAIL_PASSWORD']
SMTP_SERVER = config['email_settings']['SMTP_SERVER']
EMAIL_RECIPIENT = config['email_settings']['EMAIL_RECIPIENT']
SEARCH_CONFIG = config['search_config']
SELECTORS = config['selectors']
SCRAPY_SETTINGS = config['scrapy_settings']
URLS = config['urls']

# Validate configuration
logging.info("Validating configuration...")
required_keys = ['email_settings', 'search_config', 'selectors', 'urls']
for key in required_keys:
    if key not in config:
        logging.error(f"Missing required configuration key: {key}")
        exit(1)

# Validate Price Range
min_price = SEARCH_CONFIG['price_range']['min']
max_price = SEARCH_CONFIG['price_range']['max']

if min_price >= max_price:
    logging.error("Invalid price range: Min price should be less than Max price. Exiting.")
    exit(1)

logging.info(f"Price range validated: Min={min_price}, Max={max_price}")

def load_yaml_config(file='adresy.yaml'):
    logging.info("Loading YAML configuration...")
    try:
        with open(file, 'r') as yaml_file:
            config = yaml.safe_load(yaml_file)
            logging.info("YAML configuration loaded successfully.")
            return config
    except (FileNotFoundError, yaml.YAMLError) as e:
        logging.error(f"YAML file error: {e}. Exiting.")
        exit(1)

# ==============================
# Database Management
# ==============================
class DatabaseManager:
    def __init__(self, db_path='listings.db'):
        self.db_path = Path(db_path)
        logging.info(f"Connecting to database at {self.db_path}...")
        self.connection = self.get_database_connection()
        self.create_table()
        self.new_listings = []

    def get_database_connection(self):
        try:
            return sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            logging.error(f"Failed to insert listing '{title}': {e}")

    def create_table(self):
        with self.connection:
            self.connection.execute(''' 
                CREATE TABLE IF NOT EXISTS listings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    UNIQUE(title, link)
                )
            ''')
            logging.info("Listings table created or already exists.")

    def insert_listing(self, title, link, price):
        if title and link and price is not None:
            title, link = self.sanitize_input(title), self.sanitize_input(link)
            try:
                with self.connection:
                    cursor = self.connection.cursor()
                    cursor.execute('INSERT OR IGNORE INTO listings (title, link, price) VALUES (?, ?, ?)', (title, link, price))
                    if cursor.rowcount > 0:
                        self.new_listings.append((title, link, price))
                        logging.info(f"New listing added: {title}, {price}, {link}")  # Log each new listing
            except sqlite3.Error as e:
                logging.error(f"Failed to insert listing '{title}': {e}")


    @staticmethod
    def sanitize_input(data):
        return re.sub(r'[^\w\s-]', '', data).strip().lower()

    def close(self):
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed.")
    
    def get_new_listings(self):
        return self.new_listings

    def fetch_all_listings(self):
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute('SELECT title, link, price FROM listings')
            return cursor.fetchall()  # Fetch all listings
# ==============================
# Email Notification Function
# ==============================
last_notification_time = time.time()

def send_notification(new_listings):
    global last_notification_time
    if time.time() - last_notification_time < 3600:
        logging.warning("Email notification throttled. Try again later.")
        return

    last_notification_time = time.time()
    if not new_listings:
        logging.warning("No new listings to notify.")
        return

    template = Template("<h3>New Listings Found:</h3><ul>{% for listing in listings %}<li>Title: {{ listing.title }}<br>Link: <a href='{{ listing.link }}'>{{ listing.link }}</a><br>Price: {{ listing.price }}</li>{% endfor %}</ul>")
    message = template.render(listings=new_listings)

    msg = MIMEText(message, 'html')
    msg['Subject'] = 'Scraper Notification - New Listings Found'
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECIPIENT

    try:
        with smtplib.SMTP(SMTP_SERVER) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        logging.info("Notification sent successfully.")
    except smtplib.SMTPException as e:
        logging.error(f"Failed to send notification: {e}")

# ==============================
#             Love Pásr
# ==============================
def parse_price(price_str):
    # Strip whitespace and handle currency symbols
    dykmore = Price.fromstring(price_str)
    if dykmore.amount is not None:
        return int(dykmore.amount)  # Convert Decimal to int
    logging.error(f"Could not convert price '{price_str}' to an integer.")
    return None

# ==============================
# Krappy Spider Class
# ==============================
class Pavouk(scrapy.Spider):
    name = "real_estate"
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        **SCRAPY_SETTINGS  # Keep your existing settings
    }

    def __init__(self, urls, db_manager, selectors, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.urls = urls
        self.db_manager = db_manager
        self.selectors = selectors
        self.current_url_index = 0  # Initialize index to keep track of current URL

    def make_request(self):
        if self.current_url_index < len(self.urls):
            current_url = self.urls[self.current_url_index]
            logging.info(f"Starting request for URL: {current_url}")
            return scrapy.Request(url=current_url, callback=self.parse)
        return None  # Return None if there are no more URLs

    def start_requests(self):  # sourcery skip: use-named-expression
        # Start with the first URL
        request = self.make_request()
        if request:
            yield request

    def parse(self, response):  # sourcery skip: use-named-expression
        logging.info(f"Parsing response from {response.url}, status: {response.status}")

        # Process the listings based on the selectors
        if url_selectors := self.selectors.get(response.url):
            listings = response.css(url_selectors['listing_item'])
            logging.info(f"Number of listings found: {len(listings)}")

            if not listings:
                logging.warning(f"No listings found for URL: {response.url}")
            else:
                listings_found = 0

                for listing in listings:
                    title_element = listing.css(self.selectors[response.url]['title']).get()
                    title = re.sub(r'<.*?>', '', title_element).strip() if title_element else "No title found"

                    price_str_element = listing.css(self.selectors[response.url]['price']).get()
                    price_str = re.sub(r'<.*?>', '', price_str_element).strip() if price_str_element else "No price found"
                    logging.info(f"Found title: {title}, price: {price_str}")

                    price = parse_price(price_str)

                    if price is not None and min_price <= price <= max_price:
                        link = urljoin(response.url, listing.css('a::attr(href)').get())
                        self.db_manager.insert_listing(title, link, price)
                        listings_found += 1
                    else:
                        logging.info(f"Skipping '{title}' with price {price} (not in range or invalid).")

                logging.info(f"Total listings processed in this response: {listings_found}")

        # Prepare the next URL for processing
        self.current_url_index += 1  # Increment the index for the next URL
        next_request = self.make_request()  # Get the next request
        if next_request:
            yield next_request  # Yield the next request

        # Optional: Wait a random time before the next request to mimic natural browsing behavior
        time.sleep(random.uniform(2, 5))



# ==============================
# URL Validation Function
# ==============================
def validate_urls(urls):
    logging.info("Validating URLs...")
    valid_urls = set()
    user_agent = SCRAPY_SETTINGS['USER_AGENT']
    url_pattern = re.compile(r'^(?:http|https)://[^\s/$.?#].[^\s]*$', re.IGNORECASE)

    for url in urls:
        if url_pattern.match(url):
            domain = urlparse(url).netloc
            robots_url = f'https://{domain}/robots.txt'
            try:
                response = requests.get(robots_url)
                if response.status_code == 200:
                    disallowed_paths = [line.split(' ')[-1] for line in response.text.splitlines() if line.startswith('Disallow:') and user_agent in line]
                    if not any(url.startswith(f"https://{domain}{path}") for path in disallowed_paths):
                        valid_urls.add(url)
                        logging.info(f"Valid URL added: {url}")
                    else:
                        logging.warning(f"URL is disallowed by robots.txt: {url}")
                else:
                    logging.warning(f"Failed to access robots.txt for {domain}, status code: {response.status_code}")
            except requests.RequestException as e:
                logging.error(f"Failed to retrieve {robots_url}: {e}")
        else:
            logging.warning(f"Invalid URL format: {url}")
    return list(valid_urls)
# ==============================
# Signal Handling for Graceful Shutdown
# ==============================
def signal_handler(sig, frame):
    logging.info("Graceful shutdown initiated.")
    db_manager.close()
    exit(0)

# sourcery skip: use-named-expression
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================== 
#     Main Execution Block 
# ============================== 


if __name__ == "__main__": 
    logging.info("Starting the main execution block...") 

    config = load_yaml_config('adresy.yaml')  # Load YAML configuration
    urls_with_selectors = config.get('urls', [])
    selectors = {url['url']: url['selectors'] for url in urls_with_selectors if 'selectors' in url}
    urls = [url['url'] for url in urls_with_selectors]
    valid_urls = validate_urls([entry['url'] for entry in urls_with_selectors if 'url' in entry])
 
    if not valid_urls:
        logging.error("No valid URLs provided or scraping disallowed. Exiting.") 
        exit(1) 

    logging.info(f"Valid URLs: {valid_urls}, Price range: Min={min_price}, Max={max_price}") 
    
    db_manager = DatabaseManager() 

    # Initialize the CrawlerProcess
    process = CrawlerProcess()

    # Start the spider with valid URLs
    process.crawl(Pavouk, urls=valid_urls, db_manager=db_manager, selectors=selectors)
    
    # Start the crawling process
    process.start()  # This will block until all crawlers are finished

    # The following code will run after the spider completes
    logging.info("Fetching all listings from the database...") 
    all_listings = db_manager.fetch_all_listings()  # Fetch all listings from the database 

    if all_listings: 
        logging.info(f"Total listings fetched: {len(all_listings)}")  # Log the count
        for title, link, price in all_listings: 
            print(f"Title: {title}, Link: {link}, Price: {price}")  # Print each listing 
    else:   
        logging.info("No listings found in the database.")
    
    new_listings = db_manager.get_new_listings()
    
    if new_listings:
        logging.info("New listings detected in this session:")
        for title, link, price in new_listings:
            logging.info(f"Title: {title}, Price: {price}, Link: <a href='{link}'>{title}</a>")
    else:
        logging.info("No new listings detected in this session.")

    db_manager.close()  # Close the database connection after processing the listings 
