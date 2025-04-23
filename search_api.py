import os
from flask import Flask, request, jsonify
from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser

app = Flask(__name__)
ix = open_dir("indexdir")

@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])

    with ix.searcher() as searcher:
        parser = MultifieldParser(["title","content"], schema=ix.schema)
        query = parser.parse(f"{q}*")
        hits = searcher.search(query, limit=20)
        return jsonify([hit["url"] for hit in hits])

if __name__ == "__main__":
    port = int(os.environ.get("API_PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
