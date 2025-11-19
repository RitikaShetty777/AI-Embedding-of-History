# scraper_sitemap.py
"""
Scrape up to MAX_RECORDS article pages by reading the site's sitemap.
Writes scraped_raw.csv with required columns.
Polite delays and simple resume logic.
"""

import os
import time
import csv
import logging
from pathlib import Path
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

# Selenium fallback imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

load_dotenv()

# config (env or defaults)
SITEMAP_URL = os.getenv("SITEMAP_URL", "https://www.worldhistory.org/sitemap.xml")
START_URL = os.getenv("START_URL", "https://www.worldhistory.org/")  # fallback base
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "100"))
DELAY = float(os.getenv("DELAY", "1.0"))
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "scraped_raw.csv")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MyScraper/1.0)"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [sitemap-scraper] %(message)s")
logger = logging.getLogger("sitemap-scraper")

def requests_get(url, timeout=20):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning("requests failed for %s: %s", url, e)
        return None

def fetch_with_selenium(url, wait=3):
    logger.info("Selenium fetching: %s", url)
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(url)
        time.sleep(wait)
        return driver.page_source
    finally:
        driver.quit()

def parse_sitemap(sitemap_url):
    logger.info("Downloading sitemap: %s", sitemap_url)
    r = requests_get(sitemap_url)
    if not r:
        logger.error("Could not download sitemap")
        return []
    try:
        root = ET.fromstring(r.content)
    except Exception as e:
        logger.error("Failed to parse sitemap xml: %s", e)
        return []
    ns = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
    urls = []
    for url in root.findall('.//{*}loc'):
        loc = url.text.strip()
        # keep article pages (pattern may vary); adjust filter if needed
        if "/article/" in loc or "/articles/" in loc or "/article-" in loc:
            urls.append(loc)
    logger.info("Found %d candidate article URLs in sitemap", len(urls))
    return urls

def extract_article_fields(html, url):
    soup = BeautifulSoup(html, "html.parser")
    title = ""
    if soup.select_one("h1"):
        title = soup.select_one("h1").get_text(" ", strip=True)
    elif soup.title:
        title = soup.title.string.strip()
    excerpt = ""
    meta_desc = soup.select_one("meta[name='description']")
    if meta_desc and meta_desc.get("content"):
        excerpt = meta_desc["content"].strip()
    author = ""
    a = soup.select_one(".author a")
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

def write_csv(rows, path: Path):
    header = ["title","url","date","author","category","excerpt","content_length","tags","notes"]
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    out_path = Path(OUTPUT_CSV)
    # if CSV exists, load existing urls to avoid duplicates
    existing_urls = set()
    if out_path.exists():
        import pandas as pd
        try:
            df = pd.read_csv(out_path, dtype=str).fillna("")
            existing_urls = set(df["url"].astype(str).tolist())
        except Exception:
            existing_urls = set()
    # gather sitemap urls
    sitemap_urls = parse_sitemap(SITEMAP_URL)
    if not sitemap_urls:
        # fallback: try start URL (not ideal)
        sitemap_urls = [START_URL]

    # iterate and collect up to MAX_RECORDS new ones
    collected = []
    for u in sitemap_urls:
        if len(collected) >= MAX_RECORDS:
            break
        if u in existing_urls:
            continue
        # fetch page
        r = requests_get(u)
        html = None
        if r:
            html = r.text
        else:
            try:
                html = fetch_with_selenium(u)
            except Exception as e:
                logger.warning("Selenium fetch failed for %s : %s", u, e)
                continue
        if not html:
            continue
        try:
            rec = extract_article_fields(html, u)
            collected.append(rec)
            existing_urls.add(u)
            logger.info("Collected: %s", rec["title"][:80])
        except Exception as e:
            logger.warning("Failed parse for %s : %s", u, e)
        time.sleep(DELAY)

    if collected:
        write_csv(collected, out_path)
        logger.info("Saved %d new records to %s", len(collected), out_path)
    else:
        logger.info("No new records collected.")

if __name__ == "__main__":
    main()
