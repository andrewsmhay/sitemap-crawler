#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import time
import random
import argparse
import logging
from urllib.parse import urljoin, urlparse
import gzip
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as ET

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sitemap_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Languages to crawl with their Accept-Language header values
LANGUAGES = {
    "French": "fr,fr-FR;q=0.9",
    "German": "de,de-DE;q=0.9",
    "Dutch": "nl,nl-NL;q=0.9",
    "Polish": "pl,pl-PL;q=0.9",
    "Swedish": "sv,sv-SE;q=0.9",
    "Finnish": "fi,fi-FI;q=0.9"
}

class SitemapCrawler:
    def __init__(self, sitemap_url, delay=1, timeout=30, max_workers=5):
        self.sitemap_url = sitemap_url
        self.delay = delay  # Delay between requests in seconds
        self.timeout = timeout  # Request timeout in seconds
        self.max_workers = max_workers
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.9999.99 Safari/537.36"
        self.visited_urls = {}  # Track visited URLs per language
        self.all_urls = set()
    
    def fetch_url(self, url):
        """Fetch a URL and return its content."""
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            
            # Check if the content is gzipped
            if response.headers.get('Content-Encoding') == 'gzip' or url.endswith('.gz'):
                return gzip.GzipFile(fileobj=BytesIO(response.content)).read()
            
            return response.content
            
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
    
    def parse_sitemap(self, content):
        """Parse sitemap XML content and extract URLs."""
        if not content:
            return []
        
        try:
            # Remove XML namespace to simplify parsing
            content = content.replace(b'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"', b'')
            
            root = ET.fromstring(content)
            
            # Check if this is a sitemap index file
            if root.tag == 'sitemapindex':
                urls = []
                for sitemap in root.findall('.//sitemap'):
                    loc = sitemap.find('loc')
                    if loc is not None and loc.text:
                        urls.append(loc.text)
                return urls, True
            
            # Regular sitemap
            urls = []
            for url in root.findall('.//url'):
                loc = url.find('loc')
                if loc is not None and loc.text:
                    urls.append(loc.text)
            return urls, False
            
        except Exception as e:
            logger.error(f"Failed to parse sitemap: {e}")
            return [], False
    
    def get_urls_from_sitemap(self):
        """Process sitemap and extract all URLs recursively."""
        urls_to_process = [self.sitemap_url]
        all_page_urls = []
        
        while urls_to_process:
            sitemap_url = urls_to_process.pop(0)
            logger.info(f"Processing sitemap: {sitemap_url}")
            
            content = self.fetch_url(sitemap_url)
            if not content:
                continue
                
            urls, is_index = self.parse_sitemap(content)
            
            if is_index:
                # If this is a sitemap index, add the child sitemaps to the processing queue
                logger.info(f"Found sitemap index with {len(urls)} child sitemaps")
                urls_to_process.extend(urls)
            else:
                # If this is a regular sitemap, add the URLs to our result list
                logger.info(f"Found {len(urls)} URLs in sitemap {sitemap_url}")
                all_page_urls.extend(urls)
            
            # Add a small delay
            time.sleep(self.delay)
        
        # Remove duplicates
        unique_urls = list(set(all_page_urls))
        logger.info(f"Found {len(unique_urls)} unique URLs across all sitemaps")
        return unique_urls
    
    def visit_url(self, url, language_name, language_header):
        """Visit a URL with the specified language header."""
        # Track visited URLs per language
        if language_name not in self.visited_urls:
            self.visited_urls[language_name] = set()
            
        if url in self.visited_urls[language_name]:
            logger.debug(f"Already visited {url} with {language_name}")
            return
        
        self.visited_urls[language_name].add(url)
        
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Language": language_header,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        
        try:
            logger.info(f"Visiting {url} with language: {language_name}")
            response = requests.get(url, headers=headers, timeout=self.timeout)
            
            if response.status_code == 200:
                logger.info(f"Successfully visited {url} with {language_name}")
                
                # Optional: Extract and log the page title
                soup = BeautifulSoup(response.text, 'html.parser')
                title = soup.title.string if soup.title else "No title"
                logger.info(f"Page title ({language_name}): {title}")
                
                # Add a random delay to avoid overwhelming the server
                time.sleep(self.delay + random.uniform(0, 1))
            else:
                logger.warning(f"Failed to visit {url} with {language_name}. Status code: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error visiting {url} with {language_name}: {e}")
    
    def process_url_with_all_languages(self, url):
        """Process a single URL with all languages."""
        for language_name, language_header in LANGUAGES.items():
            self.visit_url(url, language_name, language_header)
            # Add explicit logging to confirm language processing
            logger.info(f"Completed processing {url} with {language_name}")
    
    def crawl(self):
        """Crawl all URLs from the sitemap with all specified languages."""
        urls = self.get_urls_from_sitemap()
        if not urls:
            logger.error("No URLs found to crawl")
            return
        
        total_requests = len(urls) * len(LANGUAGES)
        logger.info(f"Starting crawl of {total_requests} total requests ({len(urls)} URLs × {len(LANGUAGES)} languages)")
        
        # Use ThreadPoolExecutor to process URLs in parallel, but process all languages for each URL
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(self.process_url_with_all_languages, urls)
        
        # Log statistics for each language
        logger.info("Crawl completed. Summary by language:")
        total_visited = 0
        for language, visited_urls in self.visited_urls.items():
            count = len(visited_urls)
            total_visited += count
            logger.info(f"  - {language}: {count} URLs")
        logger.info(f"Total: {total_visited} URL-language combinations processed")

def main():
    parser = argparse.ArgumentParser(description="Crawl a website's sitemap with different language settings")
    parser.add_argument("sitemap_url", help="URL of the sitemap.xml file")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests in seconds (default: 1.0)")
    parser.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds (default: 30)")
    parser.add_argument("--max-workers", type=int, default=5, help="Maximum number of concurrent requests (default: 5)")
    
    args = parser.parse_args()
    
    crawler = SitemapCrawler(
        sitemap_url=args.sitemap_url,
        delay=args.delay,
        timeout=args.timeout,
        max_workers=args.max_workers
    )
    
    crawler.crawl()

if __name__ == "__main__":
    main()
