import os, requests, json
COHERE_API_KEY = os.environ.get("COHERE_API_KEY")
if not COHERE_API_KEY:
    raise SystemExit("Set COHERE_API_KEY in this shell")

url = "https://api.cohere.com/v1/embed"
headers = {"Authorization": f"Bearer {COHERE_API_KEY}", "Content-Type": "application/json"}

payload = {
    "model": "embed-english-v3.0",
    "inputs": ["hello world"],
    "input_type": "search_document"
}

print("REQUEST PAYLOAD:", json.dumps(payload))
r = requests.post(url, headers=headers, json=payload, timeout=60)
print("HTTP STATUS:", r.status_code)
print("BODY:", r.text)
try:
    print("JSON keys:", r.json().keys())
except Exception:
    pass
