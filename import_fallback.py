# import_fallback.py
"""
Robust import helper for db_fallback.jsonl

Behavior:
1) Read records from db_fallback.jsonl
2) Try to connect to MONGO_URI (uses certifi for Atlas)
3) If that fails, try local Mongo at mongodb://localhost:27017
4) If import succeeds, optionally rename the fallback file to mark it imported.
5) If both attempts fail, print clear instructions for installing MongoDB on Windows.

Set environment variables in .env or in your shell:
  MONGO_URI (optional) - e.g. mongodb+srv://user:pass@cluster...
  DB_NAME (optional) - defaults to 'scrape_db'
  DB_COLLECTION (optional) - defaults to 'history_pages'
  DB_BATCH_SIZE (optional) - defaults to 200
"""

import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
import certifi
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError

load_dotenv()

FALLBACK_PATH = Path(os.getenv("DB_FALLBACK_FILE", "db_fallback.jsonl"))
MONGO_URI = os.getenv("MONGO_URI", "").strip()
DB_NAME = os.getenv("DB_NAME", "scrape_db")
COLLECTION = os.getenv("DB_COLLECTION", "history_pages")
BATCH_SIZE = int(os.getenv("DB_BATCH_SIZE", "200"))
TIMEOUT_MS = int(os.getenv("DB_TIMEOUT_MS", "10000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("import_fallback")

def load_fallback():
    if not FALLBACK_PATH.exists():
        logger.error("Fallback file not found: %s", FALLBACK_PATH)
        return []
    lines = [line for line in FALLBACK_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]
    records = [json.loads(line) for line in lines]
    logger.info("Loaded %d fallback records from %s", len(records), FALLBACK_PATH)
    return records

def client_for_uri(uri, use_certifi=True):
    if not uri:
        return None
    if uri.startswith("mongodb+srv") or ("mongodb.net" in uri and not uri.startswith("mongodb://localhost")):
        # Atlas style - use certifi
        return MongoClient(uri, tls=True, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=TIMEOUT_MS)
    else:
        # normal mongodb URI
        return MongoClient(uri, serverSelectionTimeoutMS=TIMEOUT_MS)

def try_import(client, records):
    db = client[DB_NAME]
    col = db[COLLECTION]
    col.create_index("url", unique=True, background=True)
    ops_applied = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        ops = []
        for rec in batch:
            key = {"url": rec.get("url")} if rec.get("url") else {"title": rec.get("title")}
            ops.append(UpdateOne(key, {"$set": rec}, upsert=True))
        if not ops:
            continue
        result = col.bulk_write(ops, ordered=False)
        ops_applied += (getattr(result, "upserted_count", 0) + getattr(result, "modified_count", 0))
        logger.info("Imported batch %d-%d", i, i + len(batch) - 1)
    return ops_applied

def main():
    records = load_fallback()
    if not records:
        logger.info("No fallback records to import. Exiting.")
        return

    # 1) Try configured URI (Atlas or custom)
    if MONGO_URI:
        logger.info("Attempting import using MONGO_URI (masked): %s...", (MONGO_URI[:50] + "...") if len(MONGO_URI) > 50 else MONGO_URI)
        try:
            client = client_for_uri(MONGO_URI)
            client.admin.command("ping")
            applied = try_import(client, records)
            logger.info("Import successful to MONGO_URI. Approx operations applied: %d", applied)
            # rename fallback file to mark imported
            FALLBACK_PATH.rename(FALLBACK_PATH.with_suffix(".imported.jsonl"))
            logger.info("Renamed fallback file to %s", FALLBACK_PATH.with_suffix(".imported.jsonl"))
            return
        except ServerSelectionTimeoutError as e:
            logger.warning("Could not connect to MONGO_URI: %s", e)
        except PyMongoError as e:
            logger.warning("PyMongo error with MONGO_URI: %s", e)
        except Exception as e:
            logger.exception("Unexpected error while using MONGO_URI: %s", e)

    # 2) Try local MongoDB
    local_uri = "mongodb://localhost:27017"
    logger.info("Attempting import using local MongoDB at %s", local_uri)
    try:
        client_local = client_for_uri(local_uri)
        client_local.admin.command("ping")
        applied = try_import(client_local, records)
        logger.info("Import successful to local MongoDB. Approx operations applied: %d", applied)
        FALLBACK_PATH.rename(FALLBACK_PATH.with_suffix(".imported.jsonl"))
        logger.info("Renamed fallback file to %s", FALLBACK_PATH.with_suffix(".imported.jsonl"))
        return
    except Exception as e:
        logger.warning("Local MongoDB import failed: %s", e)

    # 3) If we reach here, both attempts failed
    logger.error("Both Atlas (MONGO_URI) and local MongoDB attempts failed.")
    logger.info("Your fallback file is still available at: %s", FALLBACK_PATH.resolve())

    # Provide actionable next steps
    print("\n--- Next steps to get the import working ---\n")
    print("1) If you want to use local MongoDB (recommended for offline development), install MongoDB Community:")
    print("   - Download the MSI from https://www.mongodb.com/try/download/community")
    print("   - Run the installer (choose Complete, install as a service).")
    print("   - Start the service (Admin PowerShell):  net start MongoDB")
    print("   - Then re-run this script:\n       .venv\\Scripts\\Activate.ps1")
    print("       python import_fallback.py\n")
    print("2) If you want to use Atlas, try these first:")
    print("   - Test from a different network (mobile hotspot) to see if your network blocks Atlas TLS.")
    print("   - Ensure your IP is whitelisted in Atlas (Network Access -> Add IP).")
    print("   - If your organization intercepts TLS, you may need to add the intercepting CA to Python certs or ask IT for an exception.")
    print("3) If you need help, paste the output of running this script and I will give the exact next command.\n")

if __name__ == "__main__":
    main()
