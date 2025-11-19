📘 AI Embedding of History — Vector Search Engine

A lightweight end-to-end project that scrapes historical articles, generates AI summaries, converts them into vector embeddings, stores them in MongoDB Atlas, and provides a FastAPI + frontend UI to perform semantic search over 100+ history documents.

🚀 Features
✔️ Web Scraping

Extracts 100+ history articles from a target website.

Stores title, body, categories, metadata, etc.

✔️ AI Summaries

Generates concise summaries using LLMs.

✔️ Vector Embeddings

Uses Sentence Transformers (384-dim) for text embeddings.

Saves embeddings to MongoDB under ai_summary_embedding.

✔️ MongoDB Atlas Vector Search

A dedicated knnVector index enables fast similarity search.

Supports cosine similarity.

✔️ FastAPI Backend

/search endpoint accepts user queries.

Converts query → embedding → returns top 10 relevant documents.

✔️ Simple Frontend UI

User enters a search query.

Shows most relevant historical articles with similarity scores.

🧠 Tech Stack
Component	Technology
Scraping	Python, Requests, BeautifulSoup / custom parser
Summaries	LLM (OpenAI / fallback)
Embeddings	SentenceTransformers – all-MiniLM-L6-v2
Database	MongoDB Atlas
Search	Vector Index (knnVector, cosine)
Backend	FastAPI + Uvicorn
Frontend	HTML + JS fetch API
