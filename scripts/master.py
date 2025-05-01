#!/usr/bin/env python3
"""
master.py — Flask API for distributed crawler/indexer with unified thread-level heartbeats.
"""

import os, time, uuid, logging
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3

from db import get_connection

# ────────────────────────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL      = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL      = os.environ['INDEX_TASK_QUEUE']
MASTER_PORT          = int(os.environ.get('MASTER_PORT', 5000))

HEARTBEAT_TABLE      = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')
HEARTBEAT_TIMEOUT    = int(os.environ.get('HEARTBEAT_TIMEOUT', 60))

CRAWLER_THREAD_COUNT = int(os.environ.get('CRAWLER_THREAD_COUNT',
                             os.environ.get('THREAD_COUNT', '5')))
INDEXER_THREAD_COUNT = int(os.environ.get('INDEXER_THREAD_COUNT',
                             os.environ.get('THREAD_COUNT', '10')))

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MASTER")


@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200


@app.route('/jobs', methods=['POST'])
def start_job():
    data     = request.get_json(force=True)
    seed_url = data.get('seedUrl')
    if not seed_url:
        return jsonify({'error': 'Missing seedUrl'}), 400

    try:
        depth_limit = int(data.get('depthLimit', 2))
    except ValueError:
        return jsonify({'error': 'depthLimit must be an integer'}), 400
    depth_limit = max(1, min(depth_limit, 5))

    job_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO jobs (job_id, seed_url, depth_limit, created_at)
            VALUES (%s, %s, %s, %s)
        """, (job_id, seed_url, depth_limit, now))
    conn.close()

    sqs.send_message(
        QueueUrl=CRAWL_QUEUE_URL,
        MessageBody=json.dumps({'jobId': job_id, 'url': seed_url, 'depth': 0})
    )

    logger.info("Started job %s (seed=%s, depth=%d)", job_id, seed_url, depth_limit)
    return jsonify({'jobId': job_id}), 202


@app.route('/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              job_id          AS jobId,
              seed_url        AS seedUrl,
              depth_limit     AS depthLimit,
              discovered_count AS discoveredCount,
              indexed_count    AS indexedCount,
              status,
              created_at      AS createdAt
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
        results = cur.fetchall()
    conn.close()

    return jsonify(results), 200


@app.route('/status', methods=['GET'])
def status():
    now = time.time()

    # 1) Heartbeats
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT node_id, role, UNIX_TIMESTAMP(last_heartbeat) AS ts
              FROM {HEARTBEAT_TABLE}
        """)
        rows = cur.fetchall()
    conn.close()

    crawlers = {}
    indexers = {}
    for r in rows:
        ts = float(r['ts'])
        alive = (now - ts) < HEARTBEAT_TIMEOUT
        if r['role'] == 'crawler':
            crawlers[r['node_id']] = 'alive' if alive else 'dead'
        else:
            indexers[r['node_id']] = 'alive' if alive else 'dead'

    # 2) Queue depths
    def stats(url):
        a = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible'
            ]
        )['Attributes']
        return {
            'visible':  int(a.get('ApproximateNumberOfMessages', 0)),
            'inFlight': int(a.get('ApproximateNumberOfMessagesNotVisible', 0))
        }
    queues = {'crawl': stats(CRAWL_QUEUE_URL), 'index': stats(INDEX_QUEUE_URL)}

    return jsonify({
        'crawlers': crawlers,
        'indexers': indexers,
        'queues':   queues,
        'threads':  {
            'crawler': CRAWLER_THREAD_COUNT,
            'indexer': INDEXER_THREAD_COUNT
        }
    }), 200


if __name__ == '__main__':
    logger.info("Starting master on port %d", MASTER_PORT)
    app.run(host='0.0.0.0', port=MASTER_PORT, threaded=True)
