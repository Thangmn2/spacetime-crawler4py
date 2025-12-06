from flask import Flask, render_template, request
from search_engine import SearchEngine  
import time

app = Flask(__name__)

# Initialize search engine once
search_engine = SearchEngine(
    doc_map_file="index_output/doc_id_map.json", 
    merged_index_file="index_output/index.json"
)

@app.route("/", methods=["GET", "POST"])
def home():
    query = ""      # store the query typed by the user
    results = []    # store search results

    if request.method == "POST":
        # Get the query from the HTML form
        query = request.form.get("query", "").strip()
        if query:
            
            start_time = time.time()       # Start timing

            results = search_engine.search(query)   # Run the search and get results

            end_time = time.time()         # End timing
            duration = end_time - start_time

            print(f"[QUERY TIME] '{query}' took {duration:.4f} seconds")  # check time for query to complete

    # Render the template with the query and results
    return render_template("index.html", query=query, results=results)

if __name__ == "__main__":
    app.run(debug=True)