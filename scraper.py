# scraper.py
"""
Robust scraper:
- Uses requests+BeautifulSoup first; falls back to Selenium for JS-rendered pages.
- If urls.txt exists (one URL per line) it will parse those pages directly.
- Writes OUTPUT_CSV with columns:
  title,url,date,author,category,excerpt,content_length,tags,notes
Config via .env or environment variables.
"""

import os
import time
import csv
import logging
from pathlib import Path
from urllib.parse import urljoin

from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Load env
load_dotenv()

START_URL = os.getenv("START_URL", "https://www.worldhistory.org/articles/")
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "100"))
DELAY = float(os.getenv("DELAY", "1.0"))
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "scraped_raw.csv")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MyScraper/1.0)"}

# Listing selector tuned for worldhistory.org; change if scraping another site.
LISTING_LINK_SELECTOR = os.getenv("LISTING_LINK_SELECTOR", "a.title")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [scraper] %(message)s")
logger = logging.getLogger("scraper")


def requests_get(url, timeout=15):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning("requests failed for %s: %s", url, e)
        return None


def fetch_soup_requests(url):
    r = requests_get(url)
    if not r:
        return None
    return BeautifulSoup(r.text, "html.parser")


def fetch_soup_selenium(url, wait_seconds=3):
    logger.info("Using Selenium to fetch: %s", url)
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(url)
        time.sleep(wait_seconds)
        html = driver.page_source
        return BeautifulSoup(html, "html.parser")
    finally:
        driver.quit()


def extract_links_from_listing(url):
    """Try requests then selenium to extract article links."""
    soup = fetch_soup_requests(url)
    links = []
    if soup:
        for a in soup.select(LISTING_LINK_SELECTOR):
            href = a.get("href")
            if href:
                links.append(urljoin(url, href))
    if not links:
        soup = fetch_soup_selenium(url)
        if soup:
            for a in soup.select(LISTING_LINK_SELECTOR):
                href = a.get("href")
                if href:
                    links.append(urljoin(url, href))
    links = list(dict.fromkeys(links))  # dedupe
    logger.info("Found %d candidate links", len(links))
    return links


def extract_text_with_selectors(soup, selectors):
    for sel in selectors:
        if sel.startswith("meta"):
            el = soup.select_one(sel)
            if el and el.get("content"):
                return el.get("content").strip()
            continue
        el = soup.select_one(sel)
        if el:
            if el.name == "meta" and el.get("content"):
                return el.get("content").strip()
            return el.get_text(" ", strip=True)
    return ""


def parse_article(url):
    logger.info("Parsing %s", url)
    soup = fetch_soup_requests(url)
    if not soup:
        soup = fetch_soup_selenium(url)
    if not soup:
        logger.warning("Could not fetch %s", url)
        return None

    title = extract_text_with_selectors(soup, ["h1", ".entry-title", ".article-title"]) or ""
    excerpt = extract_text_with_selectors(soup, ["meta[name='description']", ".summary", ".excerpt"]) or ""
    author = extract_text_with_selectors(soup, [".author a", ".byline", "meta[name='author']"]) or ""
    date = extract_text_with_selectors(soup, ["time", ".date", ".published"]) or ""
    tags = ", ".join([t.get_text(strip=True) for t in soup.select(".tags a")]) if soup.select(".tags a") else ""
    content_length = len(soup.get_text(" ", strip=True))

    return {
        "title": title,
        "url": url,
        "date": date,
        "author": author,
        "category": "History",
        "excerpt": excerpt,
        "content_length": content_length,
        "tags": tags,
        "notes": ""
    }


def main():
    project = Path.cwd()
    urls_txt = project / "urls.txt"
    if urls_txt.exists():
        logger.info("Found urls.txt — will parse listed URLs")
        with urls_txt.open("r", encoding="utf-8") as fh:
            links = [l.strip() for l in fh if l.strip()]
    else:
        links = extract_links_from_listing(START_URL)

    if not links:
        logger.error("No links discovered. If scraping a private / JS site, provide urls.txt with targets.")
        # write header-only CSV to indicate file exists
        header = ["title", "url", "date", "author", "category", "excerpt", "content_length", "tags", "notes"]
        Path(OUTPUT_CSV).write_text(",".join(header) + "\n", encoding="utf-8")
        return

    links = links[:MAX_RECORDS]
    rows = []
    for i, link in enumerate(links, 1):
        logger.info("(%d/%d) %s", i, len(links), link)
        item = parse_article(link)
        if item:
            rows.append(item)
        time.sleep(DELAY)

    header = ["title", "url", "date", "author", "category", "excerpt", "content_length", "tags", "notes"]
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    logger.info("Saved %d rows to %s", len(rows), OUTPUT_CSV)


if __name__ == "__main__":
    main()
