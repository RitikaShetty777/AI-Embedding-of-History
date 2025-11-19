# upsert_via_data_api.py
import json, pathlib, requests

# adjust:
API_URL = "https://data.mongodb-api.com/app/myapp-abcde/endpoint/data/v1/action/insertOne"
API_KEY = "TpqAKQgvhZE4r6AOzpVydJ9a3tB1BLMrgDzLlBLbihKNDzSJWTAHMVbsMoIOpnM6"
DATABASE = "scrape_db"
COLLECTION = "history_pages"
CSV_JSONL = "db_fallback.jsonl"   # use your fallback JSONL (each line = JSON doc)

headers = {
    "Content-Type": "application/json",
    "Access-Control-Request-Headers": "*",
    "api-key": API_KEY,
}

# read fallback jsonl lines
docs = [json.loads(line) for line in open(CSV_JSONL, "r", encoding="utf-8") if line.strip()]

# build bulkWrite request body with upsert operations
writes = []
for d in docs:
    writes.append({"updateOne": {
        "filter": {"url": d.get("url")},
        "update": {"$set": d},
        "upsert": True
    }})

body = {
    "dataSource": "Cluster0",   # typical default name; replace if different in your App config
    "database": DATABASE,
    "collection": COLLECTION,
    "operations": writes
}

resp = requests.post(API_URL, headers=headers, json=body, timeout=120)
print(resp.status_code)
print(resp.text)
