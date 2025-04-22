import json
import pickle
import threading
import re
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from whoosh.fields import Schema, ID, TEXT
from whoosh.index import create_in, open_dir
from whoosh.writing import AsyncWriter 



from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [Indexer] - [%(levelname)s] - %(message)s")
schema = Schema(url=ID(stored=True, unique=True),title=TEXT(stored=True),content=TEXT(stored=True))
if not os.path.exists("indexdir"):
    os.mkdir("indexdir")
    index = create_in("indexdir", schema)
else:
    index = open_dir("indexdir")



def index_document(url, filepath):
    try:
        with open(filepath, encoding='utf-8') as f:
            html = f.read()
    except Exception as e:
        logging.error(f"Error reading {filepath}: {e}")
        return

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=' ')
    title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"
    meta_desc_tag = soup.find("meta", attrs={"name": "description"})
    meta_description = meta_desc_tag["content"].strip() if meta_desc_tag and "content" in meta_desc_tag.attrs else ""
    full_content = f"{meta_description} {text}"

  
    writer = AsyncWriter(index)
    writer.update_document(url=url, title=title, content=full_content)
    writer.commit()

    logging.info(f"Indexed: {url}")




def build_index_parallel(map_path="url_map.json"):
    with open(map_path, encoding='utf-8') as f:
        url_map = json.load(f)

    writer = AsyncWriter(index)
    for url, path in url_map.items():
        try:
            with open(path, encoding='utf-8') as f:
                html = f.read()
        except Exception as e:
            logging.error(f"Error reading {path}: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=' ')
        title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"
        meta_desc_tag = soup.find("meta", attrs={"name": "description"})
        meta_description = meta_desc_tag["content"].strip() if meta_desc_tag and "content" in meta_desc_tag.attrs else ""
        full_content = f"{meta_description} {text}"

        writer.update_document(url=url, title=title, content=full_content)

    writer.commit() 
    logging.info("Finished indexing all documents")
