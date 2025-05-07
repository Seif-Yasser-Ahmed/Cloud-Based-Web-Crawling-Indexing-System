
import threading
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

class HeartbeatManager:
    def __init__(self, timeout=5):
        self.last_heartbeat = {}
        self.lock = threading.Lock()
        self.timeout = timeout

    def update(self, node_id):
        with self.lock:
            self.last_heartbeat[node_id] = time.time()

    def check_dead_nodes(self):
        now = time.time()
        dead = []
        with self.lock:
            for node, ts in self.last_heartbeat.items():
                if now - ts > self.timeout:
                    dead.append(node)
        return dead

    def remove_node(self, node_id):
        with self.lock:
            if node_id in self.last_heartbeat:
                del self.last_heartbeat[node_id]
