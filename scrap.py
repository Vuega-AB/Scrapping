import os
import requests
import logging
import time
from urllib.parse import urljoin, urlparse, urldefrag, urlunparse
from bs4 import BeautifulSoup

RETRIES = 3
BACKOFF_FACTOR = 1
POLITE_DELAY_SECONDS = 0.5
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def setup_logging(log_file):
    """Configures logging to write to a file and the console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler()
        ]
    )

def make_request_with_retries(session, url, **kwargs):
    for attempt in range(RETRIES):
        try:
            response = session.get(url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1} of {RETRIES} failed for {url}. Error: {e}")
            if attempt + 1 == RETRIES:
                logging.error(f"All {RETRIES} retries failed for {url}. Giving up.")
                return None # Failed all retries
            
            # Exponential backoff
            sleep_time = BACKOFF_FACTOR * (2 ** attempt)
            logging.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
    return None

# --- NEW: Intelligent URL builder to fix the pagination issue ---
def build_absolute_url(current_url, href):
    """
    Intelligently joins a base URL and a relative link (href),
    correctly handling pagination query strings.
    """
    # Standard joining for most cases
    joined_url = urljoin(current_url, href)
    
    # Parse the potentially malformed URL
    parsed_join = urlparse(joined_url)
    
    # Check for the error condition: a '?' within the query string
    if '?' in parsed_join.query:
        # The query is malformed, e.g., "page=1?page=2"
        # We want the last part, which is the correct new query
        correct_query = parsed_join.query.split('?')[-1]
        
        # Reconstruct the URL with the corrected query
        # urlunparse takes a 6-part tuple and builds a URL string
        corrected_url_parts = (
            parsed_join.scheme,
            parsed_join.netloc,
            parsed_join.path,
            parsed_join.params,
            correct_query, # Use the corrected query string
            parsed_join.fragment
        )
        return urlunparse(corrected_url_parts)
    
    # If no '?' in query, the URL is fine as is
    return joined_url

def download_pdf(session, pdf_url, download_folder):
    """Downloads a single PDF file from a given URL using a session."""
    try:
        response = make_request_with_retries(session, pdf_url, stream=True, timeout=30)
        if response is None:
            return False

        filename = os.path.basename(urlparse(pdf_url).path)
        if not filename:
            filename = "downloaded_file.pdf"
        file_path = os.path.join(download_folder, filename)

        logging.info(f"      -> Downloading as: {file_path}")
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"      -> Successfully downloaded.")
        return True
    except Exception as e:
        logging.error(f"      -> UNEXPECTED ERROR during download of {pdf_url}. Reason: {e}")
        return False

def crawl_and_download_pdfs(start_url, download_folder, log_file):
    setup_logging(log_file)
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
        logging.info(f"Created download directory: {download_folder}")
    
    with requests.Session() as session:
        session.headers.update(HEADERS)
        urls_to_visit = [start_url]
        visited_urls = set()
        downloaded_pdf_urls = set()

        logging.info(f"Starting crawl at: {start_url}")
        logging.info(f"Will only crawl pages that start with the above URL.")

        while urls_to_visit:
            current_url = urls_to_visit.pop(0)
            normalized_url, _ = urldefrag(current_url)
            if normalized_url in visited_urls:
                continue

            logging.info(f"\nCrawling page: {current_url}")
            visited_urls.add(normalized_url)
            time.sleep(POLITE_DELAY_SECONDS)

            response = make_request_with_retries(session, current_url, timeout=15)
            if response is None:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # --- CHANGE: Use the new intelligent URL builder ---
                absolute_url = build_absolute_url(current_url, href)
                
                normalized_absolute_url, _ = urldefrag(absolute_url)

                if absolute_url.lower().endswith('.pdf'):
                    if absolute_url not in downloaded_pdf_urls:
                        logging.info(f"  [+] Found PDF on '{current_url}' -> {absolute_url}")
                        if download_pdf(session, absolute_url, download_folder):
                            downloaded_pdf_urls.add(absolute_url)
                
                elif normalized_absolute_url.startswith(start_url) and normalized_absolute_url not in visited_urls:
                    if absolute_url not in urls_to_visit:
                         urls_to_visit.append(absolute_url)

    logging.info("\n-----------------------------------------")
    logging.info("Crawling finished.")
    # (Rest of the function is unchanged)
    logging.info(f"Total unique pages visited: {len(visited_urls)}")
    logging.info(f"Total unique PDFs downloaded: {len(downloaded_pdf_urls)}")
    logging.info(f"Log file saved to: {log_file}")
    logging.info("-----------------------------------------")

if __name__ == "__main__":
    START_URL = "https://www.imy.se/tillsyner/"
    DOWNLOAD_FOLDER = "pdf_downloads"
    LOG_FILE = "crawl_log.txt"
    crawl_and_download_pdfs(START_URL, DOWNLOAD_FOLDER, LOG_FILE)