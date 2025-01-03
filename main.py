import urllib.request
import os
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote, urljoin
import time
from bs4 import BeautifulSoup
import json
import argparse

class WikiCrawler:
    def __init__(self, output_dir="wiki_pages", max_pages=None, threads=16, delay=0.5):
        self.output_dir = output_dir
        self.visited_file = os.path.join(output_dir, "visited_pages.json")
        self.visited_pages = self.load_visited_pages()
        self.base_url = "https://oldschool.runescape.wiki"
        self.max_pages = max_pages
        self.threads = threads
        self.delay = delay
        self.pages_downloaded = 0
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
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
    
    def download_page(self, url):
        """Download a single page and save it to the output directory"""
        try:
            if self.max_pages and self.pages_downloaded >= self.max_pages:
                return
            
            # Skip if URL was already visited
            if url in self.visited_pages:
                print(f"Skipping already visited: {url}")
                return
            
            # Create a proper filename from the URL
            page_name = unquote(url.split('/w/')[-1])
            filename = os.path.join(self.output_dir, f"{page_name}.html")
            
            # Skip if file already exists
            if os.path.exists(filename):
                self.visited_pages.add(url)
                return
            
            # Create the request with headers
            req = urllib.request.Request(url, headers=self.headers)
            
            # Download the page
            with urllib.request.urlopen(req) as response:
                content = response.read().decode('utf-8')
            
            # Parse the content and extract only the main content div
            soup = BeautifulSoup(content, 'html.parser')
            main_content = soup.find('div', id='content')
            
            if main_content:
                # Save only the main content
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(str(main_content))
                print(f"Downloaded: {page_name}")
                self.pages_downloaded += 1
            else:
                print(f"Warning: No main content found for {page_name}")
            
            # Mark as visited
            self.visited_pages.add(url)
            
            # Save visited pages periodically
            if len(self.visited_pages) % 10 == 0:
                self.save_visited_pages()
                
            # Be nice to the server
            time.sleep(self.delay)
            
        except Exception as e:
            print(f"Error downloading {url}: {str(e)}")
    
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
    
    def crawl(self):
        """Start the crawling process"""
        current_url = f"{self.base_url}/w/Special:AllPages?from=&to=&namespace=0&hideredirects=1"
        
        while current_url:
            if self.max_pages and self.pages_downloaded >= self.max_pages:
                print(f"\nReached maximum pages limit ({self.max_pages})")
                break
                
            print(f"\nProcessing page list: {current_url}")
            print(f"Pages downloaded so far: {self.pages_downloaded}")
            
            # Get the current page content
            content = self.get_page_content(current_url)
            
            # Extract all unique links from the current page
            links = list(set(self.extract_links(content)))
            print(f"Found {len(links)} unique links to process")
            
            try:
                # Use ThreadPoolExecutor for parallel downloads
                with ThreadPoolExecutor(max_workers=self.threads) as executor:
                    # Submit download tasks for each link
                    futures = [
                        executor.submit(self.download_page, link)
                        for link in links
                    ]
                    
                    # Wait for all downloads to complete
                    for future in futures:
                        future.result()
            
            except Exception as e:
                print(f"Error processing page {current_url}: {str(e)}")
                
            finally:
                # Save visited pages after each batch
                self.save_visited_pages()
            
            # Get the next page URL
            current_url = self.get_next_page_url(content)
            if current_url:
                print(f"Moving to next page...")
                time.sleep(1)  # Extra delay between page lists

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
