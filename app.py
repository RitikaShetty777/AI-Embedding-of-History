# app.py
import os
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from fastapi.middleware.cors import CORSMiddleware

MONGODB_URI = os.environ.get("MONGODB_URI")
if not MONGODB_URI:
    raise SystemExit("Set MONGODB_URI env var in this shell")

MODEL_NAME = "all-MiniLM-L6-v2"
INDEX_NAME = "vector_index"
PATH = "ai_summary_embedding"
NUM_CANDIDATES = 100

client = MongoClient(MONGODB_URI)
db = client["scrape_db"]
coll = db["history_pages"]

model = SentenceTransformer(MODEL_NAME)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change in production
    allow_methods=["*"],
    allow_headers=["*"],
)

class SearchRequest(BaseModel):
    query: str
    limit: int = 10
    show_embedding: bool = False

@app.post("/search")
def search(req: SearchRequest):
    if not req.query or not req.query.strip():
        raise HTTPException(status_code=400, detail="Empty query")

    q_vec = model.encode(req.query).tolist()

    pipeline = [
        {
            "$vectorSearch": {
                "index": INDEX_NAME,
                "queryVector": q_vec,
                "path": PATH,
                "numCandidates": NUM_CANDIDATES,
                "limit": req.limit,
            }
        },
        {
            "$project": {
                "title": 1,
                "ai_summary": 1,
                "vectorScore": {"$meta": "vectorSearchScore"},
                "ai_summary_embedding": 1
            }
        }
    ]

    docs = list(coll.aggregate(pipeline))
    results = []
    for d in docs:
        item = {
            "title": d.get("title"),
            "ai_summary": d.get("ai_summary"),
            "score": d.get("vectorScore"),
        }
        if req.show_embedding:
            item["embedding"] = d.get("ai_summary_embedding")
        results.append(item)

    return {"results": results}
