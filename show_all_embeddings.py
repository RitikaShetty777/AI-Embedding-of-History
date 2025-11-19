# show_all_embeddings.py
import os
from pymongo import MongoClient

PREVIEW_DIMS = 10  # number of initial vector dims to show

def main():
    if "MONGODB_URI" not in os.environ:
        raise SystemExit("Set MONGODB_URI env var in this shell")
    client = MongoClient(os.environ["MONGODB_URI"])
    coll = client["scrape_db"]["history_pages"]

    cursor = coll.find({}, {"title":1, "ai_summary_embedding":1})
    count = 0
    for d in cursor:
        title = d.get("title","<no title>")
        emb = d.get("ai_summary_embedding")
        if emb is None:
            print(f"{title} -> no embedding")
            continue
        print(f"Title: {title}")
        print("Embedding length:", len(emb))
        preview = emb[:PREVIEW_DIMS]
        print("First", PREVIEW_DIMS, "dims:", preview)
        print("-"*70)
        count += 1
    print(f"Displayed {count} documents.")

if __name__ == "__main__":
    main()
