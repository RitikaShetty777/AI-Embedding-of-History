"""
cohere_generate_embeddings_and_index.py

Uses direct HTTP requests to Cohere's embed endpoint (requests library) to generate embeddings,
stores them into MongoDB (scrape_db.history_pages -> ai_summary_embedding), and creates a
MongoDB Atlas vectorSearch (knnVector) index.

This avoids client-library signature issues by calling the API directly.

Requirements:
  - pymongo
  - requests
  - tqdm

Environment variables required in the same shell:
  - COHERE_API_KEY
  - MONGODB_URI
"""

import os
import sys
import time
import json
import argparse
from typing import List, Optional
import requests
from pymongo import MongoClient
from tqdm import tqdm

# -----------------------------
# Config
# -----------------------------
DB_NAME = "scrape_db"
COLLECTION_NAME = "history_pages"
SUMMARY_FIELD = "ai_summary"
EMBED_FIELD = "ai_summary_embedding"

COHERE_MODEL = "embed-english-v3.0"   # or "embed-multilingual-v3.0"
BATCH_SIZE = 50
MAX_RETRIES = 3
RETRY_WAIT_BASE = 1.0

# -----------------------------
# Env
# -----------------------------
MONGODB_URI = os.environ.get("MONGODB_URI")
COHERE_API_KEY = os.environ.get("COHERE_API_KEY")

if not MONGODB_URI:
    raise SystemExit("Set MONGODB_URI environment variable.")
if not COHERE_API_KEY:
    raise SystemExit("Set COHERE_API_KEY environment variable.")

# -----------------------------
# Mongo client
# -----------------------------
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client[DB_NAME]
coll = db[COLLECTION_NAME]

# -----------------------------
# Requests-based Cohere embed
# -----------------------------
COHERE_EMBED_URL = "https://api.cohere.com/v1/embed"

def cohere_embed_http(texts: List[str]) -> List[List[float]]:
    """Call Cohere embed endpoint via requests. Sends multiple possible payload keys
    ('texts', 'inputs', 'input') to be robust to server variations. Returns list of vectors.
    """
    headers = {
        "Authorization": f"Bearer {COHERE_API_KEY}",
        "Content-Type": "application/json",
    }

    # include multiple names to satisfy different Cohere API variants
    payload = {
        "model": COHERE_MODEL,
        "texts": texts,
        "inputs": texts,
        "input": texts,
        "input_type": "text",
    }

    resp = requests.post(COHERE_EMBED_URL, headers=headers, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Cohere embed HTTP error {resp.status_code}: {resp.text}")

    j = resp.json()

    # preferred: top-level 'embeddings'
    if "embeddings" in j and isinstance(j["embeddings"], list):
        return j["embeddings"]

    # some variants return { "data": [{"embedding": [...]}, ...] }
    if "data" in j and isinstance(j["data"], list):
        out = []
        for item in j["data"]:
            if isinstance(item, dict):
                emb = item.get("embedding") or item.get("embeddings") or item.get("vector") or item.get("value")
                out.append(emb)
            else:
                out.append(item)
        return out

    # older variant: { "result": { "embeddings": [...] } }
    if "result" in j and isinstance(j["result"], dict) and "embeddings" in j["result"]:
        return j["result"]["embeddings"]

    # fallback — maybe response is already a list of vectors
    if isinstance(j, list) and len(j) > 0 and isinstance(j[0], (list, tuple)):
        return j

    raise RuntimeError("Unexpected Cohere embed HTTP response shape: " + json.dumps(j)[:300])

def get_embeddings_with_retries(texts: List[str]) -> List[List[float]]:
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return cohere_embed_http(texts)
        except Exception as e:
            last_exc = e
            wait = RETRY_WAIT_BASE ** (attempt - 1)
            print(f"Cohere HTTP embed error (attempt {attempt}/{MAX_RETRIES}): {e}. Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Cohere embed failed after retries") from last_exc

# -----------------------------
# Mongo helpers
# -----------------------------
def fetch_docs_to_embed(limit: Optional[int] = None):
    q = {SUMMARY_FIELD: {"$exists": True, "$ne": None}, EMBED_FIELD: {"$exists": False}}
    cursor = coll.find(q, {SUMMARY_FIELD: 1})
    if limit:
        cursor = cursor.limit(limit)
    return list(cursor)

def chunkify(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

def create_vector_index(dim: int):
    index_name = "vector_idx_ai_summary_cohere_http"
    search_index = {
        "name": index_name,
        "database": DB_NAME,
        "collection": COLLECTION_NAME,
        "type": "vectorSearch",
        "mappings": {
            "dynamic": False,
            "fields": {
                EMBED_FIELD: {"type": "knnVector", "dimensions": dim, "similarity": "cosine"}
            }
        }
    }
    try:
        res = db.command({"createSearchIndexes": COLLECTION_NAME, "indexes": [search_index]})
        print("createSearchIndexes response:", res)
        print(f"Vector index '{index_name}' requested/created.")
    except Exception as e:
        print("Could not create vector index programmatically:", e)
        print("If this fails, create the index manually in Atlas UI -> Search -> Create Search Index -> JSON and paste:")
        print(json.dumps({"mappings": {"dynamic": False, "fields": {EMBED_FIELD: {"type":"knnVector","dimensions":dim,"similarity":"cosine"}}}}, indent=2))

# -----------------------------
# Main pipeline
# -----------------------------
def main(limit: Optional[int] = None):
    docs = fetch_docs_to_embed(limit=limit)
    if not docs:
        print("No documents found needing embeddings. Exiting.")
        return

    n_docs = len(docs)
    n_batches = (n_docs + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Found {n_docs} docs needing embeddings. Processing in {n_batches} batches (batch size {BATCH_SIZE})...")

    first = True
    # iterate with tqdm so Pylance sees tqdm is used and user sees progress
    for batch in tqdm(chunkify(docs, BATCH_SIZE), total=n_batches, desc="Batches"):
        texts = [doc[SUMMARY_FIELD] for doc in batch]
        ids = [doc["_id"] for doc in batch]

        vectors = get_embeddings_with_retries(texts)

        # store embeddings back to MongoDB (small inner progress for the batch)
        for _id, vec in zip(ids, vectors):
            coll.update_one({"_id": _id}, {"$set": {EMBED_FIELD: vec}})

        if first:
            # prints which client path was used (requests or client)
            print("Using requests-based Cohere embedding.")
            first = False

        # small pause to avoid bursting the API/DB
        time.sleep(0.1)

    # verify embedding dimension
    sample = coll.find_one({EMBED_FIELD: {"$exists": True}}, {EMBED_FIELD: 1})
    if not sample:
        raise RuntimeError("No embeddings saved - something went wrong.")
    dim = len(sample[EMBED_FIELD])
    print(f"Embedding dimension detected: {dim}")

    create_vector_index(dim)
    print("Done. You can now run $vectorSearch queries against the collection.")

# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Cohere embeddings (HTTP) and create vector index in MongoDB Atlas")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of documents for testing")
    args = parser.parse_args()
    main(limit=args.limit)
