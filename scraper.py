import re
from urllib.parse import urlparse, urljoin, urldefrag
from collections import Counter
from bs4 import BeautifulSoup

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

unique_urls = set()
word_freq = Counter()
subdomains = {}
longest_page = ("", 0)

def scraper(url, resp):
    base_url, _ = urldefrag(url)
    unique_urls.add(base_url)
    
    links = extract_next_links(url, resp)

    if not resp or resp.status != 200 or not getattr(resp, "raw_response", None):
        return [link for link in links if is_valid(link)]

    content_type = resp.raw_response.headers.get("Content-Type", "")
    if "text/html" not in content_type.lower():
        return [link for link in links if is_valid(link)]

    try:
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        tokens = tokenize(text)
        word_freq.update(tokens)

        # longest page
        global longest_page
        if len(tokens) > longest_page[1]:
            longest_page = (url, len(tokens))

        # subdomains
        parsed = urlparse(url)
        hostname = parsed.hostname.lower() if parsed.hostname else ""
        if hostname.endswith(".uci.edu"):
            subdomains.setdefault(hostname, set()).add(base_url)

    except Exception as e:
        print(f"[scraper] Tokenization or parse error on {url}: {e}")
                
    return [link for link in links if is_valid(link)]

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
    output = set()

    if not resp or getattr(resp, "status", None) != 200 or not getattr(resp, "raw_response", None):
        return []

    try:
        content_type = resp.raw_response.headers.get("Content-Type", "") or ""
    except Exception:
        content_type = ""
    if "text/html" not in content_type.lower():
        return []

    try:
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
        base = getattr(resp, "url", None) or url

        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href or href.startswith(("mailto:", "javascript:")):
                continue

            absolute_url = urljoin(base, href)
            absolute_url, _ = urldefrag(absolute_url)
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
        if parsed.scheme not in set(["http", "https"]):
            return False

        domain = parsed.hostname.lower() if parsed.hostname else ""
        if not any(domain.endswith(suffix) for suffix in [
            ".ics.uci.edu", ".cs.uci.edu", ".informatics.uci.edu", ".stat.uci.edu"
        ]):
            return False
            
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

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
