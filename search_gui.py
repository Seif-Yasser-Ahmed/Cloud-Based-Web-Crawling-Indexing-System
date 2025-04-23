import os, time, tkinter as tk, logging
from tkinter import ttk, font, messagebox
from aws_adapter import SqsQueue, HeartbeatManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - GUI - %(levelname)s - %(message)s")

class SearchApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Crawler GUI")
        self.geometry("800x500")
        self._build_ui()

        self.crawl_q = SqsQueue(os.environ["CRAWL_QUEUE_URL"])
        self.hb_mgr = HeartbeatManager(os.environ["HEARTBEAT_TABLE"])
        self.poll = int(os.environ.get("GUI_HEARTBEAT_POLL_INTERVAL","5"))*1000

        self.after(1000, self._refresh)

    def _build_ui(self):
        f = font.Font(size=12)
        frm = tk.Frame(self); frm.pack(fill="x", padx=10, pady=5)
        self.url_var = tk.StringVar()
        tk.Entry(frm, textvariable=self.url_var, font=f).pack(side="left", fill="x", expand=True)
        ttk.Button(frm, text="Add URL", command=self._add).pack(side="left", padx=5)
        self.status = tk.Text(self, height=15); self.status.pack(fill="both", expand=True, padx=10, pady=5)

    def _add(self):
        url = self.url_var.get().strip()
        if not url: return
        self.crawl_q.send({"url": url, "depth": 1})
        messagebox.showinfo("Enqueued", url)
        self.url_var.set("")

    def _refresh(self):
        self.status.delete("1.0", tk.END)
        hb = self.hb_mgr.get_all()
        now = int(time.time())
        timeout = int(os.environ.get("HEARTBEAT_TIMEOUT","10"))
        for node, ts in hb.items():
            state = "ALIVE" if now-ts < timeout else "DEAD"
            self.status.insert(tk.END, f"{node:15} : {state}\n")
        self.after(self.poll, self._refresh)

if __name__ == "__main__":
    SearchApp().mainloop()
