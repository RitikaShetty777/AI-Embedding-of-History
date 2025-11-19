# test_mongo_conn.py
import os, certifi, traceback
from pymongo import MongoClient
uri = os.getenv("MONGO_URI")
print("Using MONGO_URI (masked):", (uri[:60] + "...") if uri else None)
try:
    client = MongoClient(uri, tls=True, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=10000)
    print("Pinging...")
    print(client.admin.command("ping"))
    print("Connection successful!")
except Exception:
    traceback.print_exc()
