from sentence_transformers import SentenceTransformer
from pymongo import MongoClient
import os

# Load local embedding model (no API needed)
model = SentenceTransformer("all-MiniLM-L6-v2")  # produces 384-dim vectors

# Connect to MongoDB Atlas
client = MongoClient(os.environ["MONGODB_URI"])
db = client["scrape_db"]
coll = db["history_pages"]

# Process documents that have ai_summary but no embedding
for doc in coll.find(
    {"ai_summary": {"$exists": True}, "ai_summary_embedding": {"$exists": False}}
).limit(100):

    vec = model.encode(doc["ai_summary"]).tolist()  # embedding
    coll.update_one({"_id": doc["_id"]}, {"$set": {"ai_summary_embedding": vec}})

    print("Updated:", doc["_id"])

