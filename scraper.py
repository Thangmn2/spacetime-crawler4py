import re
from urllib.parse import urlparse, urljoin, urldefrag
from collections import Counter
from bs4 import BeautifulSoup

# Is_Valid variables
STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am",
    "an", "and", "any", "are", "aren't", "as", "at", "be", "because",
    "been", "before", "being", "below", "between", "both", "but", "by",
    "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does",
    "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
    "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
    "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll",
    "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its",
    "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no",
    "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
    "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves"
}

ALLOWED_DOMAINS = (
    ".ics.uci.edu",
    ".cs.uci.edu",
    ".informatics.uci.edu",
    ".stat.uci.edu",
)
BLACKLISTED_DOMAINS = (
    "https://isg.ics.uci.edu/events/*",
    "http://fano.ics.uci.edu/ca/rules/",
)

FILE_EXT_BLACKLIST_RE = re.compile(
    r"\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4|"
    r"wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|ps|eps|tex|ppt|"
    r"pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|"
    r"7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1|thmx|mso|arff|rtf|jar|"
    r"csv|rm|smil|wmv|swf|wma|zip|rar|gz)$"
)

TRAP_PATTERNS = [
    r"/calendar", r"/events", r"/ical", r"/wp-json", r"/feed", r"/rss",
    r"/archives?/\d{4}", r"/\d{4}/\d{2}/",
    r"/print", r"/preview", r"/share", r"/login", r"/logout",
    r"/tag/", r"/author/", r"/category/",
    r"(\?|&)page=\d{2,}", r"(\?|&)offset=\d+", r"(\?|&)p=\d+",
    r"(\?|&)sort=", r"(\?|&)order=", r"(\?|&)dir=",
    r"(\?|&)utm_", r"(\?|&)replytocom=", r"(\?|&)session(id)?=",
    r"(\?|&)fbclid=", r"(\?|&)gclid=",
    r"(\?|&)format=(amp|print)",
    r"(\?|&)do=diff",
    r"(\?|&)rev\d*\[?\d*\]?",
    r"(\?|&)difftype=",
    r"doku\.php\?id=.*&rev=",
]
TRAP_RES = [re.compile(p) for p in TRAP_PATTERNS]

# 
SKIP_REASONS = Counter()

unique_urls = set()
word_freq = Counter()
subdomains = {}
longest_page = ("", 0)

similar = set()
def similar_check(tokens):
    #check for duplicate pages and return T if it is similar to previous pages
    unique_tokens = sorted(set(tokens))[:500]

    # Convert list to tuple to store in the set
    identical = tuple(unique_tokens)

    if identical in similar:
        return True
        
    #add next to compare
    similar.add(identical)
    return False

def tokenize(text):
    # Lowercase and split by non-alphabetic characters
    tokens = re.findall(r"[a-zA-Z]+", text.lower())

    #remove stop words and char
    filtered_tokens = []
    for t in tokens:
        if t not in STOPWORDS:
            if len(t) > 1:
                filtered_tokens.append(t)
                
    return filtered_tokens
    
def debug_stats():
    #print information for debug
    print("Unique URLs:", len(unique_urls))
    print("Top 10 words:", word_freq.most_common(10))
    print("Subdomains:", {k: len(v) for k, v in subdomains.items()})
    print("Longest page:", longest_page)

    
def scraper(url, resp):
    # Check for valid response
    if not resp or resp.status != 200 or not getattr(resp, "raw_response", None):
        return [] # no valid links

    # Check if content is text/html
    content_type = resp.raw_response.headers.get("Content-Type", "")
    if "text/html" not in content_type.lower():
        return [] 

    # Initialize data after checking response is ok
    base_url, _ = urldefrag(url)
    unique_urls.add(base_url)
    links = extract_next_links(url, resp)

    try:
        # read text
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        
        #get tokens & update word_freq
        tokens = tokenize(text)
        word_freq.update(tokens)

        # Prevent scraping pages with too few tokens
        if len(tokens) < 100:
            return []

        # Prevent extremely large pages
        if len(tokens) > 100000:
            return []

        # Prevent too many repeated tokens, compared to 0.2
        unique_token_ratio = len(set(tokens)) / len(tokens)
        if unique_token_ratio < 0.2:
            return []

        # skip very similar pages
        if similar_check(tokens):  
            return []

        # update longest page
        global longest_page
        if len(tokens) > longest_page[1]:
            longest_page = (url, len(tokens))

        # update subdomains
        parsed = urlparse(url)
        hostname = parsed.hostname.lower() if parsed.hostname else ""
        if hostname.endswith(".uci.edu"): 
            subdomains.setdefault(hostname, set()).add(base_url)

    except Exception as e: # If there was an error tokening/parsing the page
        print(f"[scraper] Tokenization or parse error on {url}: {e}")

    #print debug
    if len(unique_urls) % 50 == 0:
        debug_stats()
                
    return list(filter(is_valid, links)) # Return all valid links

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    
    # Return early if bad response
    if not resp or getattr(resp, "status", None) != 200 or not getattr(resp, "raw_response", None):
        return []

    output = set() # Set of hyperlinks

    # Check if we get html content
    try:
        content_type = resp.raw_response.headers.get("Content-Type", "") or ""
    except Exception:
        content_type = ""
    if "text/html" not in content_type.lower():
        return []

    # Parse html content
    try:
        # Read content
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
        base = getattr(resp, "url", None) or url

        # Find all <a> tags (hyperlinks)
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href or href.startswith(("mailto:", "javascript:")):
                continue

            absolute_url = urljoin(base, href)
            absolute_url, _ = urldefrag(absolute_url)
            # Add url to output
            output.add(absolute_url)

    except Exception as e:
        print(f"[scraper] Error processing {url}: {e}")

    print(f"[scraper] Extracted {len(output)} links from {url}")
    return list(output)

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.

    try:
        parsed = urlparse(url)
        # Must be http or https
        if parsed.scheme not in {"http", "https"}:
            return False

        

        # Only crawl valid domains
        host = (parsed.hostname or "").lower()
        if not any(host.endswith(suf) for suf in ALLOWED_DOMAINS):
            return False
            
        #Prevent blacklisted domains
        if host in BLACKLISTED_DOMAINS:
            return False 
        
        # Return false on blacklisted file types
        path = (parsed.path or "").lower()
        if FILE_EXT_BLACKLIST_RE.search(path):
            return False

        # handle traps
        query = (parsed.query or "").lower()
        for rx in TRAP_RES:
            if rx.search(path) or rx.search(query):
                return False

        # Handle large nested & repetitive directories
        if path.count("/") > 15:
            return False
        segments = [s for s in path.split("/") if s]
        if len(segments) > 8 and len(segments) != len(set(segments)):
            return False

        # Handle excessively large queries 
        if len(query) > 200:
            return False

        return True

    except TypeError:
        print ("TypeError for ", parsed)
        raise
