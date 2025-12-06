import json
import math
import os
from nltk.stem import PorterStemmer

stemmer = PorterStemmer()  # initialize Porter Stemmer for word stemming

class SearchEngine:
    def __init__(self, doc_map_file="index_output/doc_id_map.json", merged_index_file="index_output/index.json"):

        # Create doc_map if it doesn't exist
        self.create_doc_map_if_needed(doc_map_file, merged_index_file)

        print("[INFO] Loading document map...")

        # Load doc_id → URL mapping (small JSON, safe to load fully)
        with open(doc_map_file, "r", encoding="utf-8") as f:
            self.doc_id_to_url = json.load(f)
        self.total_docs = len(self.doc_id_to_url)

        print(f"[INFO] Loaded {self.total_docs} documents.\n")

        # Split merged index into shards (A-F, G-M, N-T, U-Z)
        self.shard_files = {
            "A": "index_output/A_F.json",
            "G": "index_output/G_M.json",
            "N": "index_output/N_T.json",
            "U": "index_output/U_Z.json"
        }

        #check if shard_files exist so we dont run it every instantiation 
        if not all(os.path.exists(f) for f in self.shard_files.values()):
            self.split_into_ranges(merged_index_file)

        # Add a cache for loaded shards
        self.shard_cache = {}

    def create_doc_map_if_needed(self, doc_map_file, merged_index_file):
        #Create doc_id_map.json if it doesn't exist        
        if os.path.exists(doc_map_file):
            print(f"[INFO] {doc_map_file} already exists, skipping creation.")
            return
        
        print(f"[INFO] Creating {doc_map_file} from {merged_index_file}...")
        
        with open(merged_index_file, "r") as f:
            index = json.load(f)
        
        doc_map = {}
        for term, postings_list in index.items():
            for posting in postings_list:
                doc_id = str(posting["doc_id"])
                url = posting["url"]
                if doc_id not in doc_map:
                    doc_map[doc_id] = url
        
        with open(doc_map_file, "w") as f:
            json.dump(doc_map, f, indent=2)
        
        print(f"[INFO] Created {doc_map_file} with {len(doc_map)} documents")

    #split one big json into multiple small one in alphabetical order
    def split_into_ranges(self, final_index_path):
        with open(final_index_path, "r") as f:
            full_index = json.load(f)

        ranges = {
            "A_F.json": {},
            "G_M.json": {},
            "N_T.json": {},
            "U_Z.json": {}
        }

        for term, postings in full_index.items():
            first = term[0].lower()

            if "a" <= first <= "f":
                ranges["A_F.json"][term] = postings
            elif "g" <= first <= "m":
                ranges["G_M.json"][term] = postings
            elif "n" <= first <= "t":
                ranges["N_T.json"][term] = postings
            else:
                ranges["U_Z.json"][term] = postings

        for filename, data in ranges.items():
            with open(f"index_output/{filename}", "w") as f:
                json.dump(data, f)

        print("[INFO] Split full index into 4 range files.")

    #load posting/index depend on what we need
    def load_postings(self, token):
        first = token[0].lower()
        
        if "a" <= first <= "f":
            filename = self.shard_files["A"]
        elif "g" <= first <= "m":
            filename = self.shard_files["G"]
        elif "n" <= first <= "t":
            filename = self.shard_files["N"]
        else:
            filename = self.shard_files["U"]
        
        # Check cache first!
        if filename not in self.shard_cache:
            print(f"[DEBUG] Loading shard: {filename}")  # Optional: see when shards load
            with open(filename, "r") as f:
                self.shard_cache[filename] = json.load(f)
        
        index = self.shard_cache[filename]
        
        # Get postings list for this token
        postings_list = index.get(token, [])
        
        # Convert list format to dict format
        # From: [{"doc_id": 0, "frequency": 15, "url": "..."}, ...]
        # To: {"0": {"frequency": 15, "weighted_frequency": 15}, ...}
        postings_dict = {}
        for posting in postings_list:
            doc_id = str(posting["doc_id"])
            postings_dict[doc_id] = {
                "frequency": posting["frequency"],
                "weighted_frequency": posting.get("weighted_frequency", posting["frequency"])
            }
        
        return postings_dict
            

    # Break query into words, lowercase everything
    def tokenize(self, query):
        return [w.lower() for w in query.split()]

    # Apply stemming to reduce words to their root form
    def stem_tokens(self, tokens):
        return [stemmer.stem(t) for t in tokens]

    # Boolean AND search and return only docs that contain all query terms
    def boolean_and(self, tokens):
        posting_sets = []
        for token in tokens:
            postings = self.load_postings(token) #load posting for selective index
            if not postings:
                return set()  # if any token is missing, no document matches
            posting_sets.append(set(postings.keys()))

        # intersect all sets to find docs containing every term
        docs = posting_sets[0]
        for s in posting_sets[1:]:
            docs &= s
        return docs

    # Boolean OR search and return docs containing at least one of the query terms
    def boolean_or(self, tokens):
        docs = set()
        for token in tokens:
            postings = self.load_postings(token) #load posting for selective index
            if postings:
                docs |= set(postings.keys())
        return docs
    
    #get IDF for the IF-IDF scoring
    def get_idf(self, token):
        postings = self.load_postings(token)
        df = len(postings)
        if df > 0:
            return math.log(self.total_docs / df)
        return 0.0
    
    # Compute TF-IDF score for a single document
    def score_doc(self, doc_id, tokens):
        score = 0.0
        for token in tokens:
            postings = self.load_postings(token) #load posting for selective index
            if not postings:
                continue

            # find this document in the postings list
            p = postings.get(str(doc_id))  # doc_id stored as string in JSON
            if p:
                tf = p.get("weighted_frequency", p["frequency"]) # term frequency and weighted
                idf = self.get_idf(token)  # precomputed IDF
                score += tf * idf
        return score

    # Main search function, takes a query string, returns a ranked list of (URL, score)
    def search(self, query):
        # 1) tokenize
        tokens = self.tokenize(query)

        if not tokens:
            return []

        # 2) stem each token

        # bigrams
        tokens = self.stem_tokens(tokens)

        query_bgrams = [tokens[i] + "_" + tokens[i+1] for i in range(len(tokens)-1)]

        allTerms = tokens+query_bgrams
        print("debug unigrams:", tokens)
        print("debug bigrams:", query_bgrams)

        # 3) Try Boolean AND first
        doc_ids = self.boolean_and(tokens)

        # 4) If no results, fallback to OR search
        if not doc_ids:
            doc_ids = self.boolean_or(tokens)

        # 5) Score each document using TF-IDF
        results = []
        for doc_id in doc_ids:
            score = self.score_doc(doc_id, allTerms)
            # score = self.score_doc(doc_id, tokens)
            results.append((doc_id, score))

        # 6) Sort results by score descending
        results.sort(key=lambda x: x[1], reverse=True)

        # 7) Convert document IDs to URLs for display
        return [
            (self.doc_id_to_url[doc_id], score) 
            for doc_id, score in results 
            # if doc_id in self.doc_id_to_url  # Safety check
        ]


def main():
    engine = SearchEngine()

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