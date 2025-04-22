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


        with ix.searcher() as searcher:
            query_parser = MultifieldParser(["title", "content"], schema=ix.schema)
            try:
                if len(self.current_query) == 1:
                    query = query_parser.parse(f'{self.current_query}*')
                else:
                    query = query_parser.parse(self.current_query + "*")
                logging.info(f"Parsed query: {query}")
            except Exception as e:
                logging.error(f"Error parsing query: {e}")
                messagebox.showerror("Search Error", "Invalid query format.")
                return

            search_results = searcher.search(query, limit=None)
            for hit in search_results:
                results.append(hit["url"])


        # for _, urls in self.index.items():

        #     for url in urls:
        #         if self.current_query in url.lower():
        #             results.append(url)

        # tokens = self.current_query.split()
        # scores = {}
        # for tok in tokens:
        #     for url in self.index.get(tok, []):
        #         scores[url] = scores.get(url, 0) + 1

        # for url in scores:
        #     if url not in results:
        #         results.append(url)

        # results = sorted(set(results), key=lambda u: -scores.get(u, 0))

        if results != self.last_results:
            self.result_label.config(text=f"Results for “{self.current_query}” — {len(results)} found")
            self.listbox.delete(0, tk.END)
            for url in results:
                self.listbox.insert(tk.END, url)
            self.last_results = results