import json
import os
import pickle
import re
from pathlib import Path
from collections import defaultdict
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer

class InvertedIndex:
    def __init__(self, output_dir="index_output"):
        self.inverted_index = {} #Token, postings

        self.output_dir = Path(output_dir) #Where to store data instead of in storage
        self.output_dir.mkdir(exist_ok=True)
    
    def add(self, token, url):
        doc_id = Posting.doc_to_doc_id(url)

        if token not in self.inverted_index: # New token
            posting = Posting(url)
            posting.add()
            self.inverted_index[token] = [posting]
        else: # Token exists, check if posting for this document exists
            postings_list = self.inverted_index[token]
            existing_posting = None
            
            # find the posting
            for posting in postings_list:
                if posting.doc_id == doc_id:
                    existing_posting = posting
                    break
            
            if existing_posting:
                # if posting already exists, add 1 to count
                existing_posting.add()
            else:
                # posting does not exist, create a new one
                new_posting = Posting(url)
                new_posting.add()
                postings_list.append(new_posting)

    def add_document_tokens(self, url, tokens):
        # Add all tokens from a document to the inverted index.
        for token in tokens:
            self.add(token, url)

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

    def __init__(self, url):
        self.doc_id = Posting.doc_to_doc_id(url)
        self.frequency = 0
        self.doc_url_map = {} # doc_id, URL

    def add(self): # Add 1 to frequency of posting
        self.frequency += 1

    @staticmethod
    def doc_to_doc_id(url): # convert document to document_id
        #check if document already exists
        for doc_id, existing_url in Posting.doc_url_map.items():
            if existing_url == url:
                return doc_id

        # Document does not eixst, add document to doc_url_map & update document ids
        doc_id = Posting.doc_id_counter
        Posting.doc_url_map[doc_id] = url
        Posting.doc_id_counter += 1
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
            yield {"url": j.get("url") or p.name, "content": j.get("content", "")}
        except Exception as e:
            print(f"[skip] {p}: {e}")

#parse HTML
def html_to_text(html):
    if not html:
        return ""
        
    soup = BeautifulSoup(html, "html.parser")
    
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()
        
    for br in soup.find_all("br"):
        br.replace_with("\n")
        
    HTML_tags = {
        "address", "article", "div", "footer", "header", "hr", "h1", "h2", "h3", "h4", "h5", "li", "main", "nav", "ol", "p", "section", "table", "thead", "tr", "td", "th", "ul"
    }
    #removes HTML tags
    for tag in soup.find_all(HTML_tags):
        tag.insert_before("\n")
        tag.insert_after("\n")
    
    
    text = soup.get_text(separator=" ", strip=True)
    
    text = re.sub(r"[ \t\r\f\v]+", " ", text) #no spaces/tabs
    text = re.sub(r"\n\s*\n+", "\n", text)    #no blank lines
    return text.strip()

def tokenize(text):
    tokens = []
    for match in re.finditer(r"[A-Za-z0-9]+", text or ""):
        tokens.append(match.group(0).lower())
    return tokens

stemmer = PorterStemmer()
def stem_tokens(tokens):
    stems = []
    if not tokens:
        return stems
        
    for t in tokens:
        stem = stemmer.stem(t)
        stems.append(stem)
    return stems
    
def main():
    folder = "../DEV/xtune_ics_uci_edu"

    #initialize index with the index_output file
    idx = InvertedIndex(output_dir="index_output")
    for doc in iter_json(folder):
        text = html_to_text(doc.get("content", ""))
        tokens = stem_tokens(tokenize(text))
        idx.add_document_tokens(doc.get("url"), tokens)
        
    stats = idx.get_statistics()

   # Save the index as JSON
    json_file = idx.output_dir / "index.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(
            {token: [p.to_dict() for p in postings] 
             for token, postings in idx.inverted_index.items()},
            f,
            indent=2
        )

    #get size in KB
    size_kb = os.path.getsize(json_file) / 1024
    
    print("documents:", stats["num_documents"])
    print("unique tokens:", stats["num_unique_tokens"])
    print("total postings:", stats["total_postings"])
    print(f"index size in KB: {size_kb:.2f}")

if __name__ == "__main__":
    main()
    