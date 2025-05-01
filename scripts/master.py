# master.py
#!/usr/bin/env python3
"""
master.py — Flask API for distributed crawler/indexer using RDS + SQS.

Endpoints:
  GET    /health           — simple OK check
  POST   /jobs             — start a new crawl job
  GET    /jobs/<job_id>    — get status & progress for a job
  GET    /search?query=…   — search indexed pages by term
  GET    /nodes            — list crawler node statuses (alive/dead)
"""

import os
import json
import uuid
import logging
import threading
import time
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3

from db import get_connection

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
CRAWL_QUEUE_URL = os.environ['CRAWL_QUEUE_URL']
MASTER_PORT = int(os.environ.get('MASTER_PORT', 5000))
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'crawler_heartbeats')
HEARTBEAT_TIMEOUT = int(os.environ.get('HEARTBEAT_TIMEOUT', 60))
HEARTBEAT_POLL_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', 30))

# AWS clients
sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

# Flask setup
app = Flask(__name__)
CORS(app)  # adjust origins as needed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In‐memory node status: { node_id: bool(alive) }
node_status = {}


def heartbeat_monitor():
    """
    Background thread that polls the RDS heartbeats table and
    updates node_status for each crawler node.
    """
    while True:
        time.sleep(HEARTBEAT_POLL_INTERVAL)

        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT node_id, UNIX_TIMESTAMP(last_heartbeat) "
                "FROM " + HEARTBEAT_TABLE
            )
            rows = cur.fetchall()
        conn.close()

        now = time.time()
        for node_id, ts in rows:
            alive = (now - ts) < HEARTBEAT_TIMEOUT
            node_status[node_id] = alive

        logger.info("Node status → %s", node_status)

# ──────────────────────────────────────────────────────────────────────────────


@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200


@app.route('/jobs', methods=['POST'])
def start_job():
    data = request.get_json(force=True)
    seed_url = data.get('seedUrl')
    if not seed_url:
        return jsonify({'error': 'Missing seedUrl'}), 400

    try:
        depth_limit = int(data.get('depthLimit', 2))
    except ValueError:
        return jsonify({'error': 'depthLimit must be an integer'}), 400
    depth_limit = min(max(depth_limit, 1), 5)

    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO jobs
              (job_id, seed_url, depth_limit, created_at)
            VALUES (%s, %s, %s, %s)
        """, (job_id, seed_url, depth_limit, now))
    conn.close()

    sqs.send_message(
        QueueUrl=CRAWL_QUEUE_URL,
        MessageBody=json.dumps({
            'jobId': job_id,
            'url':   seed_url,
            'depth': 0
        })
    )

    node_status.clear()
    logger.info("Started job %s with seed %s", job_id, seed_url)
    return jsonify({'jobId': job_id}), 202


@app.route('/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT job_id AS jobId, seed_url AS seedUrl, depth_limit AS depthLimit, "
            "discovered_count AS discoveredCount, indexed_count AS indexedCount, "
            "status, created_at AS createdAt "
            "FROM jobs WHERE job_id = %s",
            (job_id,)
        )
        row = cur.fetchone()
    conn.close()

    if not row:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(row), 200


@app.route('/search', methods=['GET'])
def search_index():
    term = request.args.get('query', '').strip().lower()
    if not term:
        return jsonify([]), 200

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT page_url AS pageUrl, frequency
              FROM index_entries
             WHERE term = %s
             ORDER BY frequency DESC
        """, (term,))
        items = cur.fetchall()
    conn.close()

    return jsonify(items), 200


@app.route('/nodes', methods=['GET'])
def list_nodes():
    return jsonify({nid: ('alive' if alive else 'dead')
                    for nid, alive in node_status.items()}), 200


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    monitor = threading.Thread(target=heartbeat_monitor, daemon=True)
    monitor.start()

    logger.info("Master starting on port %d", MASTER_PORT)
    app.run(host='0.0.0.0', port=MASTER_PORT, threaded=True)
