import os
import csv
import json
from pymongo import MongoClient

OUTPUT_FILE = "output_full_with_embeddings.csv"

def main():
    if "MONGODB_URI" not in os.environ:
        raise SystemExit("Set MONGODB_URI environment variable first.")

    client = MongoClient(os.environ["MONGODB_URI"])
    coll = client["scrape_db"]["history_pages"]

    # Fetch full documents
    docs = list(coll.find({}))

    if not docs:
        print("No documents found.")
        return

    # Extract all keys across all documents
    all_keys = set()
    for d in docs:
        all_keys.update(d.keys())

    # Remove MongoDB internal _id if you don't want it
    # comment this out if you want _id included
    # all_keys.discard("_id")

    # Convert set to sorted list for consistent column order
    all_keys = sorted(all_keys)

    # Ensure ai_summary_embedding is last column (optional)
    if "ai_summary_embedding" in all_keys:
        all_keys.remove("ai_summary_embedding")
        all_keys.append("ai_summary_embedding")

    print("Exporting columns:\n", all_keys)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(all_keys)

        count = 0
        for doc in docs:
            row = []
            for key in all_keys:
                value = doc.get(key, "")

                # Convert embedding to JSON string
                if key == "ai_summary_embedding" and isinstance(value, list):
                    value = json.dumps(value)

                # Avoid dicts/lists being written raw
                if isinstance(value, (dict, list)):
                    value = json.dumps(value)

                row.append(value)

            writer.writerow(row)
            count += 1

    print(f"\nExport complete! Wrote {count} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
