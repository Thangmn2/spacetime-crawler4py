import json
import pickle
from pathlib import Path
from collections import defaultdict


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
        self.d_id = Posting.doc_to_doc_id(url)
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
        d_id = Posting.doc_id_counter
        Posting.doc_url_map[d_id] = url
        Posting.doc_id_counter += 1
        return d_id

    @staticmethod
    def get_url_by_doc_id(doc_id):
        return Posting.doc_url_map.get(doc_id)
    
    @staticmethod
    def get_all_doc_urls():
        return dict(Posting.doc_url_map)
    
    def to_dict(self): #convert class data to a dictionary
        return {
            "doc_id": self.d_id,
            "frequency": self.frequency,
            "url": Posting.get_url_by_doc_id(self.d_id)
        }

    