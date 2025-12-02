import json
import math
from nltk.stem import PorterStemmer

# list of common stopwords to ignore
STOPWORDS = {
    "the","is","in","and","a","to","of","for","on","with",
    "at","by","an","be","this","that","it","as","from"
}

stemmer = PorterStemmer()  # initialize Porter Stemmer for word stemming

class SearchEngine:
    def __init__(self, index_file="index_output/index.json"):
        print("[INFO] Loading index...")

        # Load the inverted index saved in JSON
        with open(index_file, "r", encoding="utf-8") as f:
            self.index = json.load(f)

        # Build a mapping from document IDs to URLs, need this later to show URLs instead of numeric IDs
        self.doc_id_to_url = {}
        for token, postings in self.index.items():
            for p in postings:
                self.doc_id_to_url[p["doc_id"]] = p["url"]

        # Keep track of how many documents are in total
        self.total_docs = len(self.doc_id_to_url)
        print(f"[INFO] Loaded {self.total_docs} documents.\n")

        # Precompute IDF for each token and saves computation later when scoring documents
        self.idf_cache = {}
        for token, postings in self.index.items():
            df = len(postings)  # number of docs containing this token
            if df > 0:
                self.idf_cache[token] = math.log(self.total_docs / df)
            else:
                self.idf_cache[token] = 0.0

    # Break query into words, lowercase everything
    def tokenize(self, query):
        return [w.lower() for w in query.split()]

    # Remove common stopwords
    def remove_stopwords(self, tokens):
        return [t for t in tokens if t not in STOPWORDS]

    # Apply stemming to reduce words to their root form
    def stem_tokens(self, tokens):
        return [stemmer.stem(t) for t in tokens]

    # Boolean AND search and return only docs that contain all query terms
    def boolean_and(self, tokens):
        posting_sets = []
        for token in tokens:
            postings = self.index.get(token)
            if not postings:
                return set()  # if any token is missing, no document matches
            posting_sets.append({p["doc_id"] for p in postings})

        # intersect all sets to find docs containing every term
        docs = posting_sets[0]
        for s in posting_sets[1:]:
            docs &= s
        return docs

    # Boolean OR search and return docs containing at least one of the query terms
    def boolean_or(self, tokens):
        docs = set()
        for token in tokens:
            postings = self.index.get(token)
            if postings:
                docs |= {p["doc_id"] for p in postings}
        return docs

    # Compute TF-IDF score for a single document
    def score_doc(self, doc_id, tokens):
        score = 0.0
        for token in tokens:
            postings = self.index.get(token)
            if not postings:
                continue

            # find this document in the postings list
            for p in postings:
                if p["doc_id"] == doc_id:
                    tf = p["frequency"]   # term frequency
                    idf = self.idf_cache.get(token, 0.0)  # precomputed IDF
                    score += tf * idf
                    break
        return score

    # Main search function, takes a query string, returns a ranked list of (URL, score)
    def search(self, query):
        # 1) tokenize
        tokens = self.tokenize(query)

        # 2) remove stopwords
        tokens = self.remove_stopwords(tokens)
        if not tokens:
            return []

        # 3) stem each token
        tokens = self.stem_tokens(tokens)

        # 4) Try Boolean AND first
        doc_ids = self.boolean_and(tokens)

        # 5) If no results, fallback to OR search
        if not doc_ids:
            doc_ids = self.boolean_or(tokens)

        # 6) Score each document using TF-IDF
        results = []
        for doc_id in doc_ids:
            score = self.score_doc(doc_id, tokens)
            results.append((doc_id, score))

        # 7) Sort results by score descending
        results.sort(key=lambda x: x[1], reverse=True)

        # 8) Convert document IDs to URLs for display
        return [(self.doc_id_to_url[doc_id], score) for doc_id, score in results]


def main():
    engine = SearchEngine("index_output/index.json")

    print("TF-IDF Ranked Search Engine")
    print("Type a query or 'quit' to exit.\n")

    while True:
        q = input("Query: ").strip()
        if q.lower() == "quit":
            break

        results = engine.search(q)

        if not results:
            print("No results.\n")
            continue

        print("\nTop 5 results:")
        for i, (url, score) in enumerate(results[:5], 1):
            print(f"{i}. {url}   (score={score:.4f})")
        print()


if __name__ == "__main__":
    main()
