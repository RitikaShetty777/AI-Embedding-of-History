# example_vector_search.py
from pymongo import MongoClient
import os

MONGODB_URI = os.environ["MONGODB_URI"]
COHERE_API_KEY = os.environ.get("COHERE_API_KEY")
DB_NAME = "scrape_db"
COLLECTION_NAME = "history_pages"
EMBED_FIELD = "ai_summary_embedding"
COHERE_MODEL = "embed-english-v3.0"

# create clients
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]

# get a query embedding (use same cohere client code as before)
try:
    from cohere import ClientV2 as CohereClient
    co = CohereClient(api_key=COHERE_API_KEY)
    def cohere_embed(texts): return co.embed(texts=texts, model=COHERE_MODEL).embeddings
except Exception:
    import cohere
    co = cohere.Client(api_key=COHERE_API_KEY)
    def cohere_embed(texts): return getattr(co.embed(texts=texts, model=COHERE_MODEL), "embeddings", None)

query = "Who invented the printing press?"
q_vec = cohere_embed([query])[0]

pipeline = [
    {
        "$vectorSearch": {
            "vector": {"value": q_vec, "path": EMBED_FIELD},
            "k": 5,
            "score": {"path": "score"}
        }
    },
    {"$project": {"ai_summary": 1, "score": 1}}
]

results = list(db[COLLECTION_NAME].aggregate(pipeline))
for r in results:
    print(f"{r.get('score'):.4f}  {r.get('ai_summary')[:200]}")
