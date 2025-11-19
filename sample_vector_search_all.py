# sample_vector_search_all.py
import os
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L6-v2"
INDEX_NAME = "vector_index"
PATH = "ai_summary_embedding"
LIMIT = 100
NUM_CANDIDATES = 100

def make_query_vector(q_text):
    model = SentenceTransformer(MODEL_NAME)
    return model.encode(q_text).tolist()

def main():
    if "MONGODB_URI" not in os.environ:
        raise SystemExit("Set MONGODB_URI env var in this shell")
    client = MongoClient(os.environ["MONGODB_URI"])
    coll = client["scrape_db"]["history_pages"]

    query = input("Enter query: ").strip() or "Who invented the printing press?"
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
        {"$project": {"title": 1, "ai_summary": 1, "vectorScore": {"$meta": "vectorSearchScore"}}}
    ]

    results = list(coll.aggregate(pipeline))
    print(f"\nReturned {len(results)} docs (limit={LIMIT}):\n")
    for i, r in enumerate(results, 1):
        score = r.get("vectorScore")
        print(f"{i}. score={score:.6f}" if score is not None else f"{i}. score=(none)")
        print("Title:", r.get("title", "N/A"))
        print("Summary:", r.get("ai_summary", "")[:400].replace("\n"," "))
        print("-" * 70)

if __name__ == "__main__":
    main()
