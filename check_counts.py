import os
from pymongo import MongoClient

client = MongoClient(os.environ["MONGODB_URI"])
coll = client["scrape_db"]["history_pages"]

total = coll.count_documents({})
with_summary = coll.count_documents({"ai_summary": {"$exists": True, "$ne": None}})
with_embed = coll.count_documents({"ai_summary_embedding": {"$exists": True}})

print("total:", total)
print("with ai_summary:", with_summary)
print("with ai_summary_embedding:", with_embed)
