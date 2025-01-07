import urllib.request
import os
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote, urljoin
import time
from bs4 import BeautifulSoup
import json
import argparse
from wiki_parser import parse_wiki_page
import xml.etree.ElementTree as ET
import hashlib
import requests
import gzip
import re
import multiprocessing
from multiprocessing import Pool
from itertools import islice
from datetime import datetime, timedelta
import random
from collections import deque
import threading
from threading import Lock

class WikiCrawler:
    def __init__(self, output_dir="wiki_pages", max_pages=None, threads=16, delay=0.5):
        self.output_dir = output_dir
        self.html_dir = os.path.join(output_dir, "html")
        self.markdown_dir = os.path.join(output_dir, "markdown")
        self.dataset_dir = "dataset"
        self.sitemap_file = os.path.join(self.dataset_dir, "NS_0-0.xml.gz")
        self.visited_file = os.path.join(output_dir, "visited_pages.json")
        
        # Initialize thread-safe primitives
        self.error_lock = threading.Lock()
        self.visited_lock = threading.Lock()
        
        # Initialize shared state
        self.visited_pages = set()
        self._load_visited_pages()
        print(f"Number of visited pages: {len(self.visited_pages)}")
        
        self.base_url = "https://oldschool.runescape.wiki"
        self.sitemap_index_url = "https://oldschool.runescape.wiki/images/sitemaps/index.xml"
        self.max_pages = max_pages
        self.threads = threads
        self.delay = delay
        self.pages_downloaded = 0
        self._counter_lock = Lock()
        
        # Create output directories if they don't exist
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.markdown_dir, exist_ok=True)
        os.makedirs(self.dataset_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.visited_file), exist_ok=True)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Add rate limiting parameters
        self.request_window = 60  # 1 minute window
        self.max_requests = 300   # Max requests per window
        self.request_times = deque(maxlen=self.max_requests)
        self.last_request_time = time.time()
        self.min_request_interval = self.request_window / self.max_requests  # Minimum time between requests
        
        # Add backoff parameters
        self.min_delay = delay
        self.max_delay = 30  # Maximum delay between requests
        self.backoff_factor = 2
        self.error_counts = {}
        
        self.batch_size = 10  # Number of URLs to process per batch
        
        self._thread_local = threading.local()
        self._last_save_time = time.time()
        self._save_interval = 60  # Save every minute

    def _load_visited_pages(self):
        """Load the set of visited pages from disk"""
        try:
            if os.path.exists(self.visited_file):
                with self.visited_lock, open(self.visited_file, 'r') as f:
                    self.visited_pages.update(json.load(f))
        except Exception as e:
            print(f"Error loading visited pages: {str(e)}")

    def save_visited_pages(self):
        """Save the visited pages to disk periodically"""
        current_time = time.time()
        if current_time - self._last_save_time < self._save_interval:
            return
        
        try:
            with self.visited_lock:
                temp_file = self.visited_file + '.tmp'
                with open(temp_file, 'w') as f:
                    json.dump(list(self.visited_pages), f)
                os.replace(temp_file, self.visited_file)
                self._last_save_time = current_time
        except Exception as e:
            print(f"Error saving visited pages: {str(e)}")

    def check_and_download_latest_sitemap(self):
        """Check if we have the latest sitemap and download if needed"""
        try:
            # Download the sitemap index
            response = requests.get(self.sitemap_index_url, headers=self.headers)
            index_content = response.content
            
            # Parse the index XML
            root = ET.fromstring(index_content)
            # Find the most recent sitemap URL (usually the first one)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            latest_sitemap = root.find('.//sm:loc', ns).text
            
            # Download the latest sitemap (gzipped)
            response = requests.get(latest_sitemap, headers=self.headers)
            sitemap_content = response.content
            
            # Check if we need to update our local copy
            if not os.path.exists(self.sitemap_file):
                with open(self.sitemap_file, 'wb') as f:
                    f.write(sitemap_content)
                return True
            
            # Compare with existing file
            with open(self.sitemap_file, 'rb') as f:
                existing_content = f.read()
                
            if hashlib.md5(existing_content).hexdigest() != hashlib.md5(sitemap_content).hexdigest():
                with open(self.sitemap_file, 'wb') as f:
                    f.write(sitemap_content)
                return True
                
            return False
            
        except Exception as e:
            print(f"Error checking/downloading sitemap: {str(e)}")
            return False
    
    def extract_urls_from_sitemap(self):
        """Extract all wiki page URLs from the sitemap XML"""
        try:
            # Read and decompress the gzipped file
            with gzip.open(self.sitemap_file, 'rb') as gz_file:
                xml_content = gz_file.read()
            
            # Parse the XML content
            root = ET.fromstring(xml_content)
            
            # Define the namespace
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            # Extract all URLs
            urls = set()
            for url in root.findall('.//sm:url/sm:loc', ns):
                page_url = url.text
                # Only include wiki pages, exclude special pages, etc.
                if '/w/' in page_url and not any(x in page_url.lower() for x in [
                    'special:', 'file:', 'template:', 'category:', 'talk:', 'user:'
                ]):
                    urls.add(page_url)
            
            return urls
            
        except Exception as e:
            print(f"Error parsing sitemap: {str(e)}")
            return set()
    
    def wait_for_rate_limit(self):
        """Implement rate limiting per thread"""
        try:
            current_time = time.time()
            
            # Initialize thread-local last_request_time if not exists
            if not hasattr(self._thread_local, 'last_request_time'):
                self._thread_local.last_request_time = 0
            
            time_since_last = current_time - self._thread_local.last_request_time
            
            if time_since_last < self.min_request_interval:
                sleep_time = max(0.1, self.min_request_interval - time_since_last)
                time.sleep(sleep_time)
            
            self._thread_local.last_request_time = time.time()
            return True
        except Exception as e:
            print(f"Error in rate limiting: {str(e)}")
            return False

    def get_backoff_delay(self, url):
        """Calculate exponential backoff delay based on error count"""
        with self.error_lock:
            error_count = self.error_counts.get(url, 0)
            delay = min(self.min_delay * (self.backoff_factor ** error_count), self.max_delay)
            # Add jitter to prevent thundering herd
            return delay * (0.5 + random.random())

    def download_with_retry(self, url, max_retries=5):
        """Download a URL with retry logic and exponential backoff"""
        retries = 0
        while retries < max_retries:
            try:
                print(f"Attempting download of {url}")
                # Wait for rate limit
                if not self.wait_for_rate_limit():
                    print("Rate limiting failed, retrying...")
                    time.sleep(1)
                    continue
                
                # Make the request
                req = urllib.request.Request(url, headers=self.headers)
                with urllib.request.urlopen(req) as response:
                    print(f"Download successful for {url}")
                    content = response.read().decode('utf-8')
                
                # Reset error count on success
                with self.error_lock:
                    self.error_counts[url] = 0
                
                return content

            except urllib.error.HTTPError as e:
                if e.code == 429:  # Too Many Requests
                    print(f"Rate limited on {url}, backing off...")
                    time.sleep(5 + random.random() * 5)
                elif e.code >= 500:
                    print(f"Server error {e.code} on {url}, retrying...")
                else:
                    print(f"HTTP error {e.code} on {url}")
                    raise
            except Exception as e:
                print(f"Error downloading {url}: {str(e)}")
            
            # Increment error count and calculate backoff
            with self.error_lock:
                self.error_counts[url] = self.error_counts.get(url, 0) + 1
            
            delay = self.get_backoff_delay(url)
            print(f"Retrying {url} in {delay:.2f} seconds (attempt {retries + 1}/{max_retries})")
            time.sleep(delay)
            retries += 1
        
        raise Exception(f"Failed to download {url} after {max_retries} attempts")

    def download_page(self, url):
        """Download a single page and save it to the output directory"""
        try:
            # Quick check without lock first
            if url in self.visited_pages:
                return
            
            # Create filenames
            page_name = unquote(url.split('/w/')[-1])
            safe_name = re.sub(r'[\\/:*?"<>|]', '_', page_name)
            html_filename = os.path.join(self.html_dir, f"{safe_name}.html")
            md_filename = os.path.join(self.markdown_dir, f"{safe_name}.md")
            
            # Skip if files exist
            if os.path.exists(html_filename) and os.path.exists(md_filename):
                with self.visited_lock:
                    self.visited_pages.add(url)
                return
            
            # Double-check with lock
            with self.visited_lock:
                if url in self.visited_pages:
                    return
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting download: {page_name}")
            
            # Download and process content
            content = self.download_with_retry(url)
            soup = BeautifulSoup(content, 'html.parser')
            main_content = soup.find('div', id='content')
            
            if main_content:
                # Write files without holding locks
                tmp_html = html_filename + '.tmp'
                tmp_md = md_filename + '.tmp'
                
                try:
                    with open(tmp_html, 'w', encoding='utf-8') as f:
                        f.write(str(main_content))
                    os.replace(tmp_html, html_filename)
                    
                    markdown = parse_wiki_page(str(main_content))
                    with open(tmp_md, 'w', encoding='utf-8') as f:
                        f.write(markdown)
                    os.replace(tmp_md, md_filename)
                    
                    # Update counters and visited pages at the end
                    with self._counter_lock:
                        self.pages_downloaded += 1
                        downloaded = self.pages_downloaded
                    
                    with self.visited_lock:
                        self.visited_pages.add(url)
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Successfully downloaded ({downloaded} total): {page_name}")
                    
                finally:
                    for tmp_file in (tmp_html, tmp_md):
                        try:
                            if os.path.exists(tmp_file):
                                os.remove(tmp_file)
                        except:
                            pass
                        
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error downloading {url}: {str(e)}")
            raise
    
    def get_page_content(self, url):
        """Get the content of a page"""
        req = urllib.request.Request(url, headers=self.headers)
        with urllib.request.urlopen(req) as response:
            return response.read().decode('utf-8')
    
    def extract_links(self, html_content):
        """Extract all wiki page links from the chunk list"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the chunk list
        chunk_list = soup.find('ul', class_='mw-allpages-chunk')
        
        if not chunk_list:
            print("Warning: Could not find the page list")
            return []
        
        # Extract all links from the chunk list
        links = [
            a.get('href') 
            for a in chunk_list.find_all('a')
        ]
        
        # Convert relative URLs to absolute URLs
        return [urljoin(self.base_url, link) for link in links]
    
    def get_next_page_url(self, html_content):
        """Extract the next page URL from the navigation"""
        soup = BeautifulSoup(html_content, 'html.parser')
        nav = soup.find('div', class_='mw-allpages-nav')
        
        if nav:
            # Look for the "Next page" link
            links = nav.find_all('a')
            for link in links:
                if 'Next page' in link.text:
                    return urljoin(self.base_url, link.get('href'))
        return None
    
    def process_url_batch(self, urls):
        """Process a batch of URLs using threading within a process"""
        try:
            with ThreadPoolExecutor(max_workers=self.threads) as executor:
                futures = [
                    executor.submit(self.download_page, url)
                    for url in urls
                ]
                for future in futures:
                    future.result()
        except Exception as e:
            print(f"Error processing batch: {str(e)}")

    def crawl(self):
        """Start the crawling process using batched multithreading"""
        start_time = time.time()
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting crawler...")
        
        print("Checking for sitemap updates...")
        if self.check_and_download_latest_sitemap():
            print("Downloaded new sitemap file")
        else:
            print("Using existing sitemap file")
        
        # Extract URLs from sitemap
        urls = self.extract_urls_from_sitemap()
        print(f"Found {len(urls)} URLs in sitemap")
        
        if not urls:
            print("No URLs found in sitemap, aborting")
            return
        
        # Filter out already visited URLs
        with self.visited_lock:
            urls = [url for url in urls if url not in self.visited_pages]
        print(f"{len(urls)} URLs remaining after filtering visited pages")
        
        if self.max_pages:
            with self._counter_lock:
                urls = list(islice(urls, self.max_pages - self.pages_downloaded))
                print(f"Limited to {len(urls)} URLs due to max_pages setting")
        
        # Split URLs into batches
        url_batches = [urls[i:i + self.batch_size] for i in range(0, len(urls), self.batch_size)]
        total_batches = len(url_batches)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
              f"Split {len(urls)} URLs into {total_batches} batches of {self.batch_size}")
        
        completed_batches = 0
        
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"Starting downloads with {self.threads} threads...")
            
            with ThreadPoolExecutor(max_workers=self.threads) as executor:
                futures = [executor.submit(self.process_batch, batch) for batch in url_batches]
                
                for i, future in enumerate(futures):
                    try:
                        future.result(timeout=120)  # 2 minute timeout per batch
                        completed_batches += 1
                        elapsed = time.time() - start_time
                        avg_time = elapsed / completed_batches
                        remaining = (total_batches - completed_batches) * avg_time
                        
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                              f"Completed batch {completed_batches}/{total_batches} "
                              f"(Avg: {avg_time:.1f}s/batch, "
                              f"Est. remaining: {remaining/60:.1f}min)")
                        
                    except TimeoutError:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch {i+1} timed out")
                    except Exception as e:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                              f"Error processing batch {i+1}: {str(e)}")
            
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error during crawling: {str(e)}")
            import traceback
            traceback.print_exc()
            
        finally:
            elapsed = time.time() - start_time
            self.save_visited_pages()
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] "
                  f"Crawling completed in {elapsed/60:.1f} minutes. "
                  f"Downloaded {self.pages_downloaded} pages.")

    def process_batch(self, urls):
        """Process a batch of URLs in parallel"""
        total = len(urls)
        completed = 0
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting batch of {total} URLs")
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = [executor.submit(self.download_page, url) for url in urls]
            
            for future in futures:
                try:
                    future.result()
                    completed += 1
                    
                    if completed % 5 == 0 or completed == total:
                        elapsed = time.time() - start_time
                        avg_time = elapsed / completed
                        remaining = (total - completed) * avg_time
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                              f"Batch progress: {completed}/{total} URLs "
                              f"(Avg: {avg_time:.1f}s/URL, "
                              f"Est. remaining: {remaining:.1f}s)")
                        
                except Exception as e:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                          f"Error processing URL in batch ({completed}/{total}): {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='OSRS Wiki Crawler')
    parser.add_argument('--max-pages', type=int, help='Maximum number of pages to download (default: no limit)')
    parser.add_argument('--threads', type=int, default=16, help='Number of download threads (default: 16)')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between downloads in seconds (default: 0.5)')
    parser.add_argument('--output-dir', default='wiki_pages', help='Output directory (default: wiki_pages)')
    
    args = parser.parse_args()
    
    print(f"Starting crawler with settings:")
    print(f"- Max pages: {args.max_pages if args.max_pages else 'No limit'}")
    print(f"- Threads: {args.threads}")
    print(f"- Delay: {args.delay} seconds")
    print(f"- Output directory: {args.output_dir}")
    
    crawler = WikiCrawler(
        output_dir=args.output_dir,
        max_pages=args.max_pages,
        threads=args.threads,
        delay=args.delay
    )
    crawler.crawl()

if __name__ == "__main__":
    main()
