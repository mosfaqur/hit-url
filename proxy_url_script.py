import requests
import random
import time
from typing import List
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Proxy configuration
USE_GITHUB_PROXY_LIST = True
GITHUB_PROXY_URL = 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt'
USE_PROXY_FILE = False
PROXY_FILE = 'proxies.txt'
PUBLIC_PROXIES = []
USE_NO_PROXY = False

# Proxy validation settings
VALIDATE_PROXIES = True  # Test proxies before using them
VALIDATION_URL = 'http://httpbin.org/ip'  # URL to test proxies
VALIDATION_TIMEOUT = 5  # Timeout for proxy validation
MAX_PROXIES_TO_TEST = 100  # Max proxies to download and test

# Performance settings
NUM_THREADS = 10  # Number of parallel threads
NUM_REQUESTS_PER_URL = 50  # How many times to hit each URL
DELAY_BETWEEN_REQUESTS = 0.1  # Delay in seconds (lower for parallel)

# Redirect handling
FOLLOW_REDIRECTS = True  # Follow redirects like curl -L

# Thread-safe counter
class Stats:
    def __init__(self):
        self.lock = Lock()
        self.total = 0
        self.successful = 0
        self.failed = 0
    
    def increment_success(self):
        with self.lock:
            self.total += 1
            self.successful += 1
    
    def increment_fail(self):
        with self.lock:
            self.total += 1
            self.failed += 1
    
    def get_stats(self):
        with self.lock:
            return self.total, self.successful, self.failed

def download_proxies_from_github(url: str, max_proxies: int = None) -> List[str]:
    """Download proxy list from GitHub URL."""
    try:
        logger.info(f"Downloading proxies from {url}...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        proxies = []
        for line in response.text.split('\n'):
            proxy = line.strip()
            if proxy and not proxy.startswith('#'):
                if not proxy.startswith(('http://', 'https://', 'socks5://')):
                    proxy = 'http://' + proxy
                proxies.append(proxy)
                
                if max_proxies and len(proxies) >= max_proxies:
                    break
        
        logger.info(f"Downloaded {len(proxies)} proxies from GitHub")
        return proxies
    except Exception as e:
        logger.error(f"Error downloading proxies: {e}")
        return []

def read_proxies_from_file(filename: str) -> List[str]:
    """Read proxies from a file."""
    try:
        with open(filename, 'r') as f:
            proxies = []
            for line in f:
                proxy = line.strip()
                if proxy and not proxy.startswith('#'):
                    if not proxy.startswith(('http://', 'https://', 'socks5://')):
                        proxy = 'http://' + proxy
                    proxies.append(proxy)
        logger.info(f"Loaded {len(proxies)} proxies from {filename}")
        return proxies
    except FileNotFoundError:
        logger.warning(f"File {filename} not found")
        return []

def read_urls_from_file(filename: str) -> List[str]:
    """Read URLs from a file."""
    try:
        with open(filename, 'r') as f:
            urls = []
            for line in f:
                url = line.strip()
                if url:
                    if not url.startswith(('http://', 'https://')):
                        url = 'http://' + url
                    urls.append(url)
        logger.info(f"Loaded {len(urls)} URLs from {filename}")
        return urls
    except FileNotFoundError:
        logger.error(f"File {filename} not found!")
        return []

def validate_proxy(proxy: str, test_url: str = VALIDATION_URL, timeout: int = VALIDATION_TIMEOUT) -> bool:
    """Test if a proxy is working."""
    proxies = {'http': proxy, 'https': proxy}
    try:
        response = requests.get(
            test_url,
            proxies=proxies,
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        return response.status_code == 200
    except:
        return False

def validate_proxies_parallel(proxy_list: List[str], max_workers: int = 20) -> List[str]:
    """Validate proxies in parallel and return working ones."""
    logger.info(f"Validating {len(proxy_list)} proxies (this may take a minute)...")
    working_proxies = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_proxy = {executor.submit(validate_proxy, proxy): proxy for proxy in proxy_list}
        
        completed = 0
        for future in as_completed(future_to_proxy):
            proxy = future_to_proxy[future]
            completed += 1
            
            if completed % 10 == 0:
                logger.info(f"  Validated {completed}/{len(proxy_list)} proxies...")
            
            try:
                if future.result():
                    working_proxies.append(proxy)
                    logger.info(f"  ✓ Working proxy found: {proxy}")
            except:
                pass
    
    logger.info(f"Validation complete: {len(working_proxies)}/{len(proxy_list)} proxies are working")
    return working_proxies

def hit_url_with_proxy(url: str, proxy: str = None, timeout: int = 10) -> dict:
    """Hit a URL using a proxy."""
    proxies = None
    if proxy:
        proxies = {'http': proxy, 'https': proxy}

    try:
        response = requests.get(
            url,
            proxies=proxies,
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            allow_redirects=FOLLOW_REDIRECTS
        )
        final_url = response.url if FOLLOW_REDIRECTS else url
        redirect_count = len(response.history) if FOLLOW_REDIRECTS else 0
        return {
            'success': True,
            'status_code': response.status_code,
            'requested_url': url,
            'proxy': proxy if proxy else 'Direct',
            'response_time': response.elapsed.total_seconds(),
            'final_url': final_url,
            'redirects': redirect_count
        }
    except requests.exceptions.ProxyError:
        return {'success': False, 'error': 'Proxy Error', 'requested_url': url, 'proxy': proxy}
    except requests.exceptions.Timeout:
        return {'success': False, 'error': 'Timeout', 'requested_url': url, 'proxy': proxy}
    except requests.exceptions.TooManyRedirects:
        return {'success': False, 'error': 'Too Many Redirects', 'requested_url': url, 'proxy': proxy}
    except requests.exceptions.RequestException as e:
        return {'success': False, 'error': str(e)[:50], 'requested_url': url, 'proxy': proxy}

def process_url_batch(url: str, proxies: List[str], num_requests: int, stats: Stats):
    """Process multiple requests for a single URL."""
    target_url = url
    resolved_notice_logged = False

    for i in range(num_requests):
        proxy = random.choice(proxies)
        result = hit_url_with_proxy(target_url, proxy)

        if result['success']:
            final_url = result.get('final_url', target_url)
            redirect_info = ''
            if FOLLOW_REDIRECTS and final_url != result['requested_url']:
                redirect_info = f" -> {final_url}"
                if not resolved_notice_logged:
                    logger.info(
                        f"Resolved redirect for {url} -> {final_url}. "
                        "Using resolved destination for future requests."
                    )
                    resolved_notice_logged = True
                target_url = final_url
            stats.increment_success()
            logger.info(
                f"✓ {result['requested_url'][:30]}...{redirect_info} | "
                f"Status: {result['status_code']} | "
                f"Time: {result['response_time']:.2f}s | Proxy: {proxy}"
            )
        else:
            stats.increment_fail()
            logger.warning(
                f"✗ {result['requested_url'][:30]}... | Error: {result['error']} | Proxy: {proxy}"
            )

        time.sleep(DELAY_BETWEEN_REQUESTS)

def main():
    URL_FILE = 'urls.txt'
    
    # Read URLs
    urls = read_urls_from_file(URL_FILE)
    if not urls:
        logger.error("No URLs to process. Exiting.")
        return
    
    # Load proxies
    proxies = []
    if USE_NO_PROXY:
        logger.info("Running in NO PROXY mode")
        proxies = [None]
    elif USE_GITHUB_PROXY_LIST:
        proxies = download_proxies_from_github(GITHUB_PROXY_URL, MAX_PROXIES_TO_TEST)
        if not proxies and USE_PROXY_FILE:
            proxies = read_proxies_from_file(PROXY_FILE)
    elif USE_PROXY_FILE:
        proxies = read_proxies_from_file(PROXY_FILE)
    
    if not proxies:
        logger.error("No proxies available. Exiting.")
        return
    
    # Validate proxies
    if VALIDATE_PROXIES and proxies != [None]:
        working_proxies = validate_proxies_parallel(proxies)
        if not working_proxies:
            logger.error("No working proxies found! Exiting.")
            return
        proxies = working_proxies
        
        # Save working proxies to file
        with open('working_proxies.txt', 'w') as f:
            for proxy in working_proxies:
                f.write(f"{proxy}\n")
        logger.info(f"Saved {len(working_proxies)} working proxies to working_proxies.txt")
    
    # Statistics
    stats = Stats()
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Starting multi-threaded URL hitting")
    logger.info(f"URLs: {len(urls)} | Requests per URL: {NUM_REQUESTS_PER_URL}")
    logger.info(f"Threads: {NUM_THREADS} | Working Proxies: {len(proxies)}")
    logger.info(f"{'='*60}\n")
    
    try:
        # Process URLs in parallel
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = []
            for url in urls:
                future = executor.submit(process_url_batch, url, proxies, NUM_REQUESTS_PER_URL, stats)
                futures.append(future)
            
            # Wait for all to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing URL: {e}")
    
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user. Stopping gracefully...")
    
    # Final statistics
    total, successful, failed = stats.get_stats()
    logger.info(f"\n{'='*60}")
    logger.info("FINAL STATISTICS")
    logger.info(f"{'='*60}")
    logger.info(f"Total Requests: {total}")
    if total > 0:
        logger.info(f"Successful: {successful} ({successful/total*100:.1f}%)")
        logger.info(f"Failed: {failed} ({failed/total*100:.1f}%)")
    logger.info(f"{'='*60}")

if __name__ == "__main__":
    main()
