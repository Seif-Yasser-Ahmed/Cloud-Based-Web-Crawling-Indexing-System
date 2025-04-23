import logging
import os
import json
from whoosh.fields import Schema, ID, TEXT
from whoosh.index import create_in, open_dir
from whoosh.writing import AsyncWriter
from whoosh.qparser import MultifieldParser
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [Indexer] - [%(levelname)s] - %(message)s")

# Define schema and create/open index
schema = Schema(url=ID(stored=True, unique=True), title=TEXT(stored=True), content=TEXT(stored=True))
INDEX_DIR = "indexdir"
if not os.path.exists(INDEX_DIR):
    os.mkdir(INDEX_DIR)
    index = create_in(INDEX_DIR, schema)
else:
    index = open_dir(INDEX_DIR)


class Indexer:
    def __init__(self, index):
        self.index = index

    def index_document(self, url, html_content):

        try:
            
            soup = BeautifulSoup(html_content, "html.parser")
            
            
            text = soup.get_text(separator=' ')
            
           
            title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"
            
           
            meta_desc_tag = soup.find("meta", attrs={"name": "description"})
            meta_description = (
                meta_desc_tag["content"].strip()
                if meta_desc_tag and "content" in meta_desc_tag.attrs
                else ""
            )
            
            
            full_content = f"{meta_description} {text}"
            
            
            writer = AsyncWriter(self.index)
            writer.update_document(url=url, title=title, content=full_content)
            writer.commit()
            
            logging.info(f"Indexed: {url}")
        except Exception as e:
            logging.error(f"Error indexing document {url}: {e}")

    def search(self, query_string):

        with self.index.searcher() as searcher:
            query_parser = MultifieldParser(["title", "content"], schema=self.index.schema)
            try:
                query = query_parser.parse(query_string)
                logging.info(f"Parsed query: {query}")
                results = searcher.search(query, limit=None)
                return [hit["url"] for hit in results]
            except Exception as e:
                logging.error(f"Error parsing query: {e}")
                return []

    def build_index_parallel(self, map_path="url_map.json"):

        with open(map_path, encoding='utf-8') as f:
            url_map = json.load(f)

        for url, path in url_map.items():
            try:
                with open(path, encoding='utf-8') as f:
                    html = f.read()
                self.index_document(url=url, html_content=html)
            except Exception as e:
                logging.error(f"Error processing {path}: {e}")

        logging.info("Finished indexing all documents")


# # Example usage
# if __name__ == "__main__":
#     # Initialize Indexer
#     indexer = Indexer(index)

#     # Build index from url_map.json
#     indexer.build_index_parallel("url_map.json")

#     # Perform a search
#     query = "example query"
#     results = indexer.search(query)
#     logging.info(f"Search results: {results}")
