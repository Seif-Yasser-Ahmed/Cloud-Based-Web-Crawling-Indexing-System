#!/usr/bin/env python3
"""
master.py — Flask API for distributed crawler/indexer using RDS + SQS.
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
from aws_adapter import HeartbeatManager

# ── Configuration ─────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL = os.environ['CRAWL_QUEUE_URL']
AWS_REGION = os.environ.get('AWS_REGION', 'eu-north-1')
MASTER_PORT = int(os.environ.get('MASTER_PORT', 5000))
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'crawler_heartbeats')
HEARTBEAT_TIMEOUT = int(os.environ.get('HEARTBEAT_TIMEOUT', 60))
HEARTBEAT_POLL_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', 30))

# ── AWS clients ────────────────────────────────────────────────────────────────
sqs = boto3.client('sqs', region_name=AWS_REGION)

# ── Flask setup ───────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)   # ← allow all origins for now

logging.basicConfig(level=logging.INFO,
                    format='[MASTER] %(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ── Heartbeat manager & node status tracking ──────────────────────────────────
heartbeat_mgr = HeartbeatManager(table_name=HEARTBEAT_TABLE,
                                 timeout=HEARTBEAT_TIMEOUT)
node_status = {}


def heartbeat_monitor():
    """Background thread: mark alive/dead crawler nodes."""
    while True:
        time.sleep(HEARTBEAT_POLL_INTERVAL)
        dead = heartbeat_mgr.check_dead()
        for nid in dead:
            node_status[nid] = False
        all_ts = heartbeat_mgr.get_all()
        now = time.time()
        for nid, ts in all_ts.items():
            node_status[nid] = (now - ts) < HEARTBEAT_TIMEOUT
        logger.info(f"Node status updated: {node_status}")

# ── Endpoints ─────────────────────────────────────────────────────────────────


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

    # 1) Insert into RDS & commit
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO jobs
              (job_id, seed_url, depth_limit, created_at)
            VALUES (%s, %s, %s, %s)
        """, (job_id, seed_url, depth_limit, now))
    conn.commit()    # ← make sure to commit!
    conn.close()

    # 2) Enqueue initial crawl message & log it
    try:
        resp = sqs.send_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MessageBody=json.dumps({
                'jobId': job_id,
                'url':   seed_url,
                'depth': 0
            })
        )
        logger.info(f"Enqueued crawl message: {resp.get('MessageId')}")
    except Exception as e:
        logger.exception("Failed to send initial SQS message")

    # Reset node-status map
    node_status.clear()
    logger.info("Started job %s with seed %s", job_id, seed_url)
    return jsonify({'jobId': job_id}), 202


@app.route('/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_id AS jobId,
                   seed_url AS seedUrl,
                   depth_limit AS depthLimit,
                   discovered_count AS discoveredCount,
                   indexed_count AS indexedCount,
                   status, created_at AS createdAt
            FROM jobs
            WHERE job_id = %s
        """, (job_id,))
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
    human = {nid: ('alive' if alive else 'dead')
             for nid, alive in node_status.items()}
    return jsonify(human), 200


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # Launch heartbeat monitor
    threading.Thread(target=heartbeat_monitor, daemon=True).start()
    logger.info("Starting master on port %d (Crawl queue = %s)",
                MASTER_PORT, CRAWL_QUEUE_URL)
    app.run(host='0.0.0.0', port=MASTER_PORT, threaded=True)
