import tkinter as tk
from tkinter import font, ttk, messagebox
import pickle
import webbrowser
import logging
import time
# Removed unused import
import json
from pathlib import Path
import os

INDEX_FILE = "inverted_index.pkl"
URL_QUEUE_FILE = "dynamic_urls.json"
HEARTBEAT_FILE = "heartbeat_log.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [GUI] - [%(levelname)s] - %(message)s")

class SearchApp(tk.Tk):
    def __init__(self, index):
        super().__init__()
        self.title("Distributed Crawler GUI")
        self.geometry("900x600")
        self.configure(bg="#f7f9fb")
        self.index = index
        self._create_styles()
        self._build_ui()
        self.current_query = ""
        self.last_results = []
        self.after(1000, self._auto_refresh_search)
        self.after(2000, self._update_node_status)

    def _create_styles(self):
        self.header_font = font.Font(family="Helvetica", size=18, weight="bold")
        self.normal_font = font.Font(family="Helvetica", size=12)
        style = ttk.Style(self)
        style.configure("Search.TButton", font=self.normal_font, background="#4CAF50", foreground="white")
        style.map("Search.TButton", background=[("active", "#45a049")])

    def _build_ui(self):
        frame = tk.Frame(self, bg="#f7f9fb")
        frame.pack(fill="x", padx=20, pady=10)

        self.search_var = tk.StringVar()
        self.search_var.trace_add('write', lambda *_: self._on_search(manual=True))
        entry = ttk.Entry(frame, textvariable=self.search_var, font=self.normal_font)
        entry.pack(side="left", fill="x", expand=True, ipady=4)

        btn = ttk.Button(frame, text="Search", style="Search.TButton", command=self._on_search)
        btn.pack(side="left", padx=(10, 0))

        self.result_label = tk.Label(self, text="", bg="#f7f9fb", fg="#555555", font=self.normal_font)
        self.result_label.pack(anchor="w", padx=20)

        body = tk.Frame(self, bg="#f7f9fb")
        body.pack(fill="both", expand=True, padx=20, pady=(5, 20))

        self.listbox = tk.Listbox(body, font=self.normal_font, bg="white", fg="#333333",selectbackground="#cce5ff", activestyle="none", borderwidth=0)
        self.listbox.pack(side="left", fill="both", expand=True)
        self.listbox.bind("<Double-Button-1>", self._on_open)

        scrollbar = ttk.Scrollbar(body, orient="vertical", command=self.listbox.yview)
        scrollbar.pack(side="left", fill="y")
        self.listbox.config(yscrollcommand=scrollbar.set)

        right_panel = tk.Frame(body, bg="#e8f1ff", padx=10)
        right_panel.pack(side="left", fill="y")

        ttk.Label(right_panel, text="Node Tracker", font=self.normal_font).pack(pady=(5, 0))
        self.node_box = tk.Text(right_panel, width=30, height=15, font=("Courier", 10))
        self.node_box.pack(pady=5)

        ttk.Label(right_panel, text="Add URL to Crawl:", font=self.normal_font).pack(pady=(10, 0))
        self.new_url_var = tk.StringVar()
        new_url_entry = ttk.Entry(right_panel, textvariable=self.new_url_var, width=30)
        new_url_entry.pack(pady=(0, 5))

        ttk.Button(right_panel, text="Add URL", command=self._add_url).pack()

    def _on_search(self, manual=True):
        query = self.search_var.get().strip().lower()
        if query == "":
            self.listbox.delete(0, tk.END)
            self.result_label.config(text="Enter search term to see results")
            self.last_results = []
            return

        if manual:
            self.current_query = query

        if not self.current_query:
            return

        results = []
        for _, urls in self.index.items():
            for url in urls:
                if self.current_query in url.lower():
                    results.append(url)

        tokens = self.current_query.split()
        scores = {}
        for tok in tokens:
            for url in self.index.get(tok, []):
                scores[url] = scores.get(url, 0) + 1

        for url in scores:
            if url not in results:
                results.append(url)

        results = sorted(set(results), key=lambda u: -scores.get(u, 0))

        if results != self.last_results:
            self.result_label.config(text=f"Results for “{self.current_query}” — {len(results)} found")
            self.listbox.delete(0, tk.END)
            for url in results:
                self.listbox.insert(tk.END, url)
            self.last_results = results

    def _auto_refresh_search(self):
        self._on_search(manual=False)
        self.after(1000, self._auto_refresh_search)

    def _update_node_status(self):
        self.node_box.delete("1.0", tk.END)
        if Path(HEARTBEAT_FILE).exists():
            with open(HEARTBEAT_FILE) as f:
                try:
                    heartbeats = json.load(f)
                    now = time.time()
                    for node, ts in heartbeats.items():
                        age = now - ts
                        status = "ALIVE" if age < 5 else "DEAD"
                        self.node_box.insert(tk.END, f"{node:10} : {status}\n")
                except Exception:
                    self.node_box.insert(tk.END, "Error loading heartbeat log.\n")
        else:
            self.node_box.insert(tk.END, "No heartbeat log found.\n")
        self.after(3000, self._update_node_status)

    def _add_url(self):
        url = self.new_url_var.get().strip()
        if not url:
            return
        if Path(URL_QUEUE_FILE).exists():
            with open(URL_QUEUE_FILE, "r") as f:
                urls = json.load(f)
        else:
            urls = []
        urls.append(url)
        with open(URL_QUEUE_FILE, "w") as f:
            json.dump(urls, f)
        messagebox.showinfo("URL Added", f"URL {url} has been added to the crawl queue.")
        self.new_url_var.set("")

    def _on_open(self, _):
        sel = self.listbox.curselection()
        if sel:
            url = self.listbox.get(sel[0])
            webbrowser.open(url)

if __name__ == "__main__":
    # Attempt to load JSON index for persistence, else fallback to pickle
    if os.path.exists("index.json"):
        with open("index.json", "r", encoding="utf-8") as f:
            inverted_index = json.load(f)
    else:
        with open(INDEX_FILE, "rb") as f:
            inverted_index = pickle.load(f)
    app = SearchApp(inverted_index)
    app.mainloop()
