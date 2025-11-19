# scraper_crawl.py
"""
Crawl a site (BFS) to discover article pages and scrape them.
Writes/append to scraped_raw.csv. Config via .env or env vars.
"""

import os
import time
import csv
import logging
from collections import deque
from urllib.parse import urljoin, urlparse
from pathlib import Path

from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

# Selenium fallback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()

START_URL = os.getenv("START_URL", "https://www.worldhistory.org/")
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "100"))
DELAY = float(os.getenv("DELAY", "1.0"))
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "scraped_raw.csv")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MyCrawler/1.0; +https://example.com)"}
MAX_PAGES_TO_VISIT = int(os.getenv("MAX_PAGES_TO_VISIT", "2000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [crawler] %(message)s")
logger = logging.getLogger("crawler")

def requests_get(url, timeout=15):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.debug("requests failed for %s : %s", url, e)
        return None

def fetch_with_selenium(url, wait=3):
    logger.info("Selenium fallback for %s", url)
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    try:
        driver.get(url)
        time.sleep(wait)
        return driver.page_source
    finally:
        driver.quit()

def is_internal(url, base_netloc):
    try:
        return urlparse(url).netloc == "" or urlparse(url).netloc == base_netloc
    except Exception:
        return False

def normalize_url(href, base):
    if not href:
        return None
    return urljoin(base, href.split("#")[0]).rstrip("/")

def extract_links(html, base):
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href").strip()
        full = normalize_url(href, base)
        if full:
            links.append(full)
    return links

def parse_article(html, url):
    soup = BeautifulSoup(html, "html.parser")
    title = ""
    if soup.select_one("h1"):
        title = soup.select_one("h1").get_text(" ", strip=True)
    elif soup.title:
        title = soup.title.string.strip()
    excerpt = ""
    meta = soup.select_one("meta[name='description']")
    if meta and meta.get("content"):
        excerpt = meta.get("content").strip()
    author = ""
    a = soup.select_one(".author a") or soup.select_one(".byline")
    if a:
        author = a.get_text(" ", strip=True)
    date = ""
    t = soup.select_one("time")
    if t:
        date = t.get_text(" ", strip=True)
    tags = ", ".join([x.get_text(" ", strip=True) for x in soup.select(".tags a")]) if soup.select(".tags a") else ""
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

def append_csv(rows, path: Path):
    header = ["title","url","date","author","category","excerpt","content_length","tags","notes"]
    exist = path.exists()
    with path.open("a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header)
        if not exist:
            w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    base = START_URL
    base_netloc = urlparse(base).netloc
    out_path = Path(OUTPUT_CSV)
    visited = set()
    articles = set()

    # load existing scraped urls to avoid duplicates
    if out_path.exists():
        import pandas as pd
        try:
            df = pd.read_csv(out_path, dtype=str).fillna("")
            for u in df["url"].astype(str).tolist():
                articles.add(u.rstrip("/"))
        except Exception:
            pass

    q = deque([base])
    pages_visited = 0
    scraped_rows = []

    while q and len(articles) < MAX_RECORDS and pages_visited < MAX_PAGES_TO_VISIT:
        url = q.popleft()
        if url in visited:
            continue
        visited.add(url)
        pages_visited += 1
        logger.info("Visiting (%d) %s", pages_visited, url)

        r = requests_get(url)
        html = None
        if r:
            html = r.text
        else:
            try:
                html = fetch_with_selenium(url)
            except Exception as e:
                logger.warning("Selenium fetch failed for %s : %s", url, e)
                continue

        if not html:
            continue

        # find links and queue internal ones
        for link in extract_links(html, url):
            if not is_internal(link, base_netloc):
                continue
            normalized = link.rstrip("/")
            if normalized not in visited:
                q.append(normalized)

        # If URL looks like an article page, parse & save
        lower = url.lower()
        if "/article" in lower or "/articles" in lower or "/article-" in lower:
            norm = url.rstrip("/")
            if norm not in articles:
                try:
                    row = parse_article(html, url)
                    scraped_rows.append(row)
                    articles.add(norm)
                    logger.info("Collected article: %s", row["title"][:120])
                except Exception as e:
                    logger.warning("Failed parse article %s : %s", url, e)
        time.sleep(DELAY)

        # Save periodically to disk (every 10 found)
        if len(scraped_rows) >= 10:
            append_csv(scraped_rows, out_path)
            scraped_rows = []

    # final flush
    if scraped_rows:
        append_csv(scraped_rows, out_path)

    logger.info("Crawl complete. Total articles collected (including existing): %d", len(articles))
    logger.info("Saved/updated CSV: %s", out_path)

if __name__ == "__main__":
    main()
