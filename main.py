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

class WikiCrawler:
    def __init__(self, output_dir="wiki_pages", max_pages=None, threads=16, delay=0.5):
        self.output_dir = output_dir
        self.html_dir = os.path.join(output_dir, "html")
        self.markdown_dir = os.path.join(output_dir, "markdown")
        self.dataset_dir = "dataset"
        self.sitemap_file = os.path.join(self.dataset_dir, "NS_0-0.xml.gz")
        self.visited_file = os.path.join(output_dir, "visited_pages.json")
        self.visited_pages = self.load_visited_pages()
        self.base_url = "https://oldschool.runescape.wiki"
        self.sitemap_index_url = "https://oldschool.runescape.wiki/images/sitemaps/index.xml"
        self.max_pages = max_pages
        self.threads = threads
        self.delay = delay
        self.pages_downloaded = 0
        
        # Create output directories if they don't exist
        os.makedirs(self.html_dir, exist_ok=True)
        os.makedirs(self.markdown_dir, exist_ok=True)
        os.makedirs(self.dataset_dir, exist_ok=True)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Add rate limiting parameters
        self.request_window = 60  # 1 minute window
        self.max_requests = 300   # Max requests per window
        self.request_times = deque(maxlen=self.max_requests)
        self.request_lock = threading.Lock()
        
        # Add backoff parameters
        self.min_delay = delay
        self.max_delay = 30  # Maximum delay between requests
        self.backoff_factor = 2
        self.error_counts = {}
        self.error_lock = threading.Lock()
    
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
            urls = []
            for url in root.findall('.//sm:url/sm:loc', ns):
                page_url = url.text
                # Only include wiki pages, exclude special pages, etc.
                if '/w/' in page_url and not any(x in page_url.lower() for x in [
                    'special:', 'file:', 'template:', 'category:', 'talk:', 'user:'
                ]):
                    urls.append(page_url)
            
            return urls
            
        except Exception as e:
            print(f"Error parsing sitemap: {str(e)}")
            return []
    
    def load_visited_pages(self):
        """Load the set of visited pages from disk"""
        if os.path.exists(self.visited_file):
            with open(self.visited_file, 'r') as f:
                return set(json.load(f))
        return set()
    
    def save_visited_pages(self):
        """Save the set of visited pages to disk"""
        with open(self.visited_file, 'w') as f:
            json.dump(list(self.visited_pages), f)
    
    def wait_for_rate_limit(self):
        """Implement rate limiting using sliding window"""
        with self.request_lock:
            now = datetime.now()
            
            # Remove old requests from the window
            while self.request_times and (now - self.request_times[0]).total_seconds() > self.request_window:
                self.request_times.popleft()
            
            # If we've hit the limit, wait until we can make another request
            if len(self.request_times) >= self.max_requests:
                sleep_time = (self.request_times[0] + timedelta(seconds=self.request_window) - now).total_seconds()
                time.sleep(max(0, sleep_time))
            
            # Add current request to the window
            self.request_times.append(now)

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
                # Wait for rate limit
                self.wait_for_rate_limit()
                
                # Make the request
                req = urllib.request.Request(url, headers=self.headers)
                with urllib.request.urlopen(req) as response:
                    content = response.read().decode('utf-8')
                
                # Reset error count on success
                with self.error_lock:
                    self.error_counts[url] = 0
                
                return content

            except urllib.error.HTTPError as e:
                if e.code == 429:  # Too Many Requests
                    print(f"Rate limited on {url}, backing off...")
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
            if self.max_pages and self.pages_downloaded >= self.max_pages:
                return
            
            # Skip if URL was already visited
            if url in self.visited_pages:
                print(f"Skipping already visited: {url}")
                return
            
            # Create filenames
            page_name = unquote(url.split('/w/')[-1])
            safe_name = re.sub(r'[\\/:*?"<>|]', '_', page_name)
            html_filename = os.path.join(self.html_dir, f"{safe_name}.html")
            md_filename = os.path.join(self.markdown_dir, f"{safe_name}.md")
            
            # Skip if both files already exist
            if os.path.exists(html_filename) and os.path.exists(md_filename):
                self.visited_pages.add(url)
                return
            
            # Download with retry logic
            content = self.download_with_retry(url)
            
            # Process the content
            soup = BeautifulSoup(content, 'html.parser')
            main_content = soup.find('div', id='content')
            
            if main_content:
                # Save files with atomic writes
                tmp_html = html_filename + '.tmp'
                tmp_md = md_filename + '.tmp'
                
                try:
                    # Write HTML
                    with open(tmp_html, 'w', encoding='utf-8') as f:
                        f.write(str(main_content))
                    os.replace(tmp_html, html_filename)
                    
                    # Convert and write markdown
                    markdown = parse_wiki_page(str(main_content))
                    with open(tmp_md, 'w', encoding='utf-8') as f:
                        f.write(markdown)
                    os.replace(tmp_md, md_filename)
                    
                    print(f"Downloaded: {page_name}")
                    self.pages_downloaded += 1
                    
                finally:
                    # Clean up temp files if they exist
                    for tmp_file in (tmp_html, tmp_md):
                        try:
                            if os.path.exists(tmp_file):
                                os.remove(tmp_file)
                        except:
                            pass
            
            # Mark as visited and save periodically
            self.visited_pages.add(url)
            if len(self.visited_pages) % 10 == 0:
                self.save_visited_pages()
            
        except Exception as e:
            print(f"Error processing {url}: {str(e)}")
    
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
        """Start the crawling process using multiprocessing and multithreading"""
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
        urls = [url for url in urls if url not in self.visited_pages]
        print(f"{len(urls)} URLs remaining after filtering visited pages")
        
        if self.max_pages:
            urls = urls[:self.max_pages - self.pages_downloaded]

        try:
            # Calculate number of processes and batch size
            num_processes = multiprocessing.cpu_count()
            batch_size = max(1, len(urls) // (num_processes * 4))  # Divide URLs into smaller batches
            
            # Create batches of URLs
            url_batches = [
                urls[i:i + batch_size] 
                for i in range(0, len(urls), batch_size)
            ]
            
            print(f"Using {num_processes} processes with {self.threads} threads each")
            print(f"Split {len(urls)} URLs into {len(url_batches)} batches")

            # Process URL batches using multiple processes
            with Pool(processes=num_processes) as pool:
                pool.map(self.process_url_batch, url_batches)
        
        except Exception as e:
            print(f"Error during crawling: {str(e)}")
            
        finally:
            # Save visited pages after completion
            self.save_visited_pages()
            print(f"\nCrawling completed. Downloaded {self.pages_downloaded} pages.")

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
