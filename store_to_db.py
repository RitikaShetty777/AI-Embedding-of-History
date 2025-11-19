#!/usr/bin/env python3
"""
store_to_db_debug.py
Verbose / debug variant of store_to_db.py.
Usage:
  # normal (uses MONGO_URI from env or .env)
  python store_to_db_debug.py

  # to allow invalid certs (TESTING ONLY)
  setx ALLOW_INVALID_TLS 1
  python store_to_db_debug.py
"""
import os
import csv
import json
import logging
from pathlib import Path
import certifi
import pymongo
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MONGO_URI = os.environ.get("MONGO_URI")
CSV_PATH = Path("output_with_summaries.csv")
FALLBACK = Path("db_fallback.jsonl")

def show_tls_info():
    import ssl, sys
    logging.info("Python: %s", sys.version.splitlines()[0])
    logging.info("OpenSSL: %s", ssl.OPENSSL_VERSION)
    logging.info("certifi bundle: %s", certifi.where())

def load_rows(csv_path):
    if not csv_path.exists():
        logging.error("CSV not found: %s", csv_path)
        return []
    rows = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    logging.info("Loaded %d records from %s", len(rows), csv_path)
    return rows

def save_fallback(rows, fallback_path=FALLBACK):
    logging.warning("Saving %d records to fallback file: %s", len(rows), fallback_path)
    with fallback_path.open("w", encoding="utf-8") as out:
        for r in rows:
            out.write(json.dumps(r, ensure_ascii=False) + "\n")
    logging.info("Fallback save complete.")

def upsert_to_mongo(rows, client):
    db = client.get_database("scrape_db")
    col = db["history_pages"]
    count = 0
    for r in rows:
        # you can adjust the upsert key
        filter_doc = {"url": r.get("url")}
        update_doc = {"$set": r}
        col.update_one(filter_doc, update_doc, upsert=True)
        count += 1
    logging.info("Upserted %d records to %s.%s", count, db.name, col.name)

def main():
    if MONGO_URI is None:
        logging.error("MONGO_URI not set. Export it as an env var before running.")
        return

    show_tls_info()

    rows = load_rows(CSV_PATH)
    if not rows:
        return

    allow_invalid = os.environ.get("ALLOW_INVALID_TLS", "") in ("1", "true", "True")

    logging.info("Trying to connect to Atlas with tlsCAFile (certifi). allow_invalid=%s", allow_invalid)
    try:
        client_opts = {
            "tlsCAFile": certifi.where(),
            "serverSelectionTimeoutMS": 15000,
            "connectTimeoutMS": 15000,
        }
        if allow_invalid:
            # For debugging only — disables certificate validation
            client_opts["tlsAllowInvalidCertificates"] = True
            client_opts["tlsAllowInvalidHostnames"] = True

        client = MongoClient(MONGO_URI, **client_opts)
        logging.info("Pinging server...")
        client.admin.command("ping")
        logging.info("Ping succeeded — proceeding to upsert.")
        upsert_to_mongo(rows, client)
        logging.info("All done.")
    except Exception as e:
        logging.exception("MongoDB connection failed (exception): %s", e)
        save_fallback(rows)
        logging.info("Records saved locally. Fix TLS / network and re-run importer (import_fallback.py).")

if __name__ == "__main__":
    main()
