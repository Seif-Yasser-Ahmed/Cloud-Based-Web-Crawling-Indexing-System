# master.py
#!/usr/bin/env python3
"""
master.py — Flask API for distributed crawler/indexer using RDS + SQS.
"""

import os
import json
import uuid
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3

from db import get_connection

# ─── Configuration ────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL   = os.environ['CRAWL_QUEUE_URL']

# ─── AWS Clients ──────────────────────────────────────────────────────────────
sqs = boto3.client('sqs')

# ─── Flask App ────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

@app.route('/jobs', methods=['POST'])
def start_job():
    data     = request.get_json(force=True)
    seed_url = data.get('seedUrl')
    if not seed_url:
        return jsonify({'error': 'Missing seedUrl'}), 400

    depth_limit = int(data.get('depthLimit', 2))
    job_id      = str(uuid.uuid4())
    now         = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    # 1) Insert into RDS jobs table
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO jobs
              (job_id, seed_url, depth_limit, created_at)
            VALUES (%s, %s, %s, %s)
        """, (job_id, seed_url, depth_limit, now))

    # 2) Enqueue initial crawl message
    sqs.send_message(
        QueueUrl=CRAWL_QUEUE_URL,
        MessageBody=json.dumps({
            'jobId': job_id,
            'url':   seed_url,
            'depth': 0
        })
    )

    return jsonify({'jobId': job_id}), 202


@app.route('/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM jobs WHERE job_id = %s", (job_id,))
        row = cur.fetchone()

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
            SELECT page_url, frequency
              FROM index_entries
             WHERE term = %s
             ORDER BY frequency DESC
        """, (term,))
        items = cur.fetchall()

    results = [{'pageUrl': it['page_url'], 'frequency': it['frequency']} for it in items]
    return jsonify(results), 200


if __name__ == '__main__':
    # Listen on all interfaces, port 80
    app.run(host='0.0.0.0', port=80, threaded=True)
