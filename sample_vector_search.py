# sample_vector_search.py
import os
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L6-v2"
INDEX_NAME = "vector_index"       # your Atlas index name
PATH = "ai_summary_embedding"     # field path
LIMIT = 5                         # number of results to return
NUM_CANDIDATES = 100              # tuned for approximate search (index built earlier)

def make_query_vector(q_text):
    model = SentenceTransformer(MODEL_NAME)
    return model.encode(q_text).tolist()

def main():
    if "MONGODB_URI" not in os.environ:
        raise SystemExit("Set MONGODB_URI in this shell.")
    client = MongoClient(os.environ["MONGODB_URI"])
    db = client["scrape_db"]
    coll = db["history_pages"]

    query = "Who invented the printing press?"
    q_vec = make_query_vector(query)

    pipeline = [
        {
            "$vectorSearch": {
                "index": INDEX_NAME,
                "queryVector": q_vec,
                "path": PATH,
                "numCandidates": NUM_CANDIDATES,
                "limit": LIMIT
            }
        },
        # Project the vectorSearchScore into a field using $meta
        {
            "$project": {
                "title": 1,
                "ai_summary": 1,
                "vectorScore": {"$meta": "vectorSearchScore"}
            }
        }
    ]

    results = list(coll.aggregate(pipeline))
    if not results:
        print("No results returned. Check embeddings, dims, and index path.")
        return

    for r in results:
        score = r.get("vectorScore")
        # Mongo may return score as None if driver/cluster version behaves differently,
        # so guard and print nicely
        print(f"\nscore={score:.6f}" if score is not None else "\nscore=(none)")
        print("title:", r.get("title", "N/A"))
        print("summary:", r.get("ai_summary", "")[:400])
        print("-" * 60)

if __name__ == "__main__":
    main()
