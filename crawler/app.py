from flask import Flask, render_template, request
from search_engine import SearchEngine  

app = Flask(__name__)

# Initialize search engine once
search_engine = SearchEngine("index_output/index.json")

@app.route("/", methods=["GET", "POST"])
def home():
    query = ""      # store the query typed by the user
    results = []    # store search results

    if request.method == "POST":
        # Get the query from the HTML form
        query = request.form.get("query", "").strip()
        if query:
            # Run the search and get results
            results = search_engine.search(query)

    # Render the template with the query and results
    return render_template("index.html", query=query, results=results)

if __name__ == "__main__":
    app.run(debug=True)
