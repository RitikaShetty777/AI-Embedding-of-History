from cohere_generate_embeddings_and_index import cohere_embed, COHERE_CLIENT_VARIANT

vecs = cohere_embed(["hello world"])
print("Got", len(vecs), "embedding(s)")
print("Embedding length:", len(vecs[0]))
print("First 5 dims:", vecs[0][:5])
print("Cohere client variant:", COHERE_CLIENT_VARIANT)
