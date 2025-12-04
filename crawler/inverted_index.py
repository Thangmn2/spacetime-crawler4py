import json
import os
import re
from pathlib import Path
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
from collections import Counter
from bs4 import XMLParsedAsHTMLWarning
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

class InvertedIndex:
    def __init__(self, output_dir="index_output"):
        self.inverted_index = {} #Token, postings

        self.output_dir = Path(output_dir) #Where to store data instead of in storage
        self.output_dir.mkdir(exist_ok=True)
    
    def add(self, token, url, n =1):
        doc_id = Posting.doc_to_doc_id(url)
        postings_list = self.inverted_index.get(token)
        if postings_list is None:
            p = Posting(url)
            p.add(n)
            self.inverted_index[token] = {doc_id: p}
            return
        p = postings_list.get(doc_id)
        if p is None:
            p = Posting(url)
            postings_list[doc_id] = p
        p.add(n)

    def add_document_tokens(self, url, tokens):
        # Add all tokens from a document to the inverted index.
        if not tokens:
            return
            
        for token, c in Counter(tokens).items():
            self.add(token, url, n=c)

    def get(self, token):
    # get all postings for a token.
        return self.inverted_index.get(token, [])
    
    def get_statistics(self):
        num_unique_tokens = len(self.inverted_index) # Total Unique tokens
        num_documents = len(Posting.get_all_doc_urls()) # Total num documents
        
        total_postings = sum(len(postings) for postings in self.inverted_index.values()) # Total postings
        
        return {
            "num_unique_tokens": num_unique_tokens,
            "num_documents": num_documents,
            "total_postings": total_postings
        }
    
class Posting:
    # class variables
    doc_id_counter = 0 #Keep track of document ids
    doc_url_map = {} # (d_id, document)
    url_to_doc_id = {} # {url: doc_id}

    def __init__(self, url):
        self.doc_id = Posting.doc_to_doc_id(url)
        self.frequency = 0

    def add(self, n=1): # Add n to frequency of posting
        self.frequency += n

    @staticmethod
    def doc_to_doc_id(url): # convert document to document_id
        #check if document already exists
        doc_id = Posting.url_to_doc_id.get(url)
        if doc_id is not None:
            return doc_id

        # Document does not eixst, add document to doc_url_map & update document ids
        doc_id = Posting.doc_id_counter
        Posting.doc_id_counter += 1
        Posting.doc_url_map[doc_id] = url
        Posting.url_to_doc_id[url] = doc_id
        return doc_id

    @staticmethod
    def get_url_by_doc_id(doc_id):
        return Posting.doc_url_map.get(doc_id)
    
    @staticmethod
    def get_all_doc_urls():
        return dict(Posting.doc_url_map)
    
    def to_dict(self): #convert class data to a dictionary
        return {
            "doc_id": self.doc_id,
            "frequency": self.frequency,
            "url": Posting.get_url_by_doc_id(self.doc_id)
        }
#traverse folders and read JSON
def iter_json(root):
    for p in Path(root).rglob("*.json"):
        try:
            with p.open("r", encoding="utf-8") as f:
                j = json.load(f)

            # ensure a string fallback
            url = j.get("url")
            if isinstance(url, list):
                url = url[0] if url else None
            if not isinstance(url, str) or not url.strip():
                url = str(p)

            # ensure a string
            content = j.get("content", "")
            if isinstance(content, bytes):
                content = content.decode("utf-8", "ignore")
            elif isinstance(content, list):
                content = " ".join(map(str, content))
            elif isinstance(content, dict):
                content = json.dumps(content, ensure_ascii=False)
            elif not isinstance(content, str):
                content = "" if content is None else str(content)

            yield {"url": url, "content": content}
        except Exception as e:
            print(f"[skip] {p}: {e}")

#parse HTML
def html_to_text(html):

    # normalize to str
    if isinstance(html, bytes):
        html = html.decode("utf-8", "ignore")
    elif not isinstance(html, str):
        html = "" if html is None else str(html)

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    return re.sub(r"[ \t\r\f\v]+", " ", text).strip()
    

def tokenize(text, _pat=re.compile(r"[A-Za-z0-9]+")):
    return [m.group(0).lower() for m in _pat.finditer(text or "")]

stemmer = PorterStemmer()
def stem_tokens(tokens):
    stems = []
    if not tokens:
        return stems
        
    for t in tokens:
        stem = stemmer.stem(t)
        stems.append(stem)
    return stems
    
def should_skip(url, content):
    # Skip Apache directory listings (?C= sorting)
    if "?C=" in url and ";O=" in url:
        return True

    # Skip giant text dumps
    if url.endswith(".txt") or url.endswith(".log"):
        if len(content) > 200_000:  # 200 KB raw text
            return True

    # Skip extremely large HTML pages (>2MB inside JSON)
    if len(content) > 2_000_000:  # 2MB
        return True

    return False

    
def main():
    folder = "../ANALYST"

    #initialize index with the index_output file
    idx = InvertedIndex(output_dir="index_output")
    
    for doc in iter_json(folder):
        url = doc.get("url")
        content = doc.get("content", "")
        
        #skip large or useless pages
        if should_skip(url, content):
            print(f"\n[SKIP] {url}")
            continue
            
        text = html_to_text(doc.get("content", ""))
        tokens = stem_tokens(tokenize(text))

        # building 2 grams
        bgrams = [tokens[i] + "_" + tokens[i+1] for i in range(len(tokens)-1)]
        allTerms = tokens + bgrams
        idx.add_document_tokens(url, allTerms)
        # idx.add_document_tokens(doc.get("url"), tokens)
        print(doc["url"])  # See every file being processed
        
    stats = idx.get_statistics()

    # Save the index as JSON
    json_file = idx.output_dir / "index.json"
    with open(json_file, "w", encoding="utf-8") as f:
        f.write("{\n")
        first = True
        for token, postings in idx.inverted_index.items():
            if not first:
                f.write(",\n")
            first = False
    
            posts = [p.to_dict() for p in postings.values()]
            f.write(json.dumps(token))
            f.write(": ")
            f.write(json.dumps(posts))
        f.write("\n}\n")

    #get size in KB
    size_kb = os.path.getsize(json_file) / 1024
    
    print("documents:", stats["num_documents"])
    print("unique tokens:", stats["num_unique_tokens"])
    print("total postings:", stats["total_postings"])
    print(f"index size in (json) KB: {size_kb:.2f}")

if __name__ == "__main__":
    main()
    
