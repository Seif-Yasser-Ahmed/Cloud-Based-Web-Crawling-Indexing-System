# master.py

#!/usr/bin/env python3
"""
master.py — Flask API for distributed crawler/indexer with enhanced
boolean & phrase search, plus real-time monitoring.
"""

import os
import re
import json
import uuid
import logging
import threading
import time
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3

from nltk.stem.porter import PorterStemmer
from db import get_connection

# ──────────────────────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL = os.environ['CRAWL_QUEUE_URL']
MASTER_PORT = int(os.environ.get('MASTER_PORT', 5000))
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')
HEARTBEAT_TIMEOUT = int(os.environ.get('HEARTBEAT_TIMEOUT', 60))
HEARTBEAT_POLL_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', 30))

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))
app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
stemmer = PorterStemmer()

node_status = {}


def heartbeat_monitor():
    while True:
        time.sleep(HEARTBEAT_POLL_INTERVAL)
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT node_id, UNIX_TIMESTAMP(last_heartbeat) AS ts "
                f"FROM {HEARTBEAT_TABLE}"
            )
            rows = cur.fetchall()
        conn.close()
        now = time.time()
        for r in rows:
            node_status[r['node_id']] = (now - r['ts']) < HEARTBEAT_TIMEOUT


@app.route('/health')
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
    except:
        return jsonify({'error': 'depthLimit must be int'}), 400
    depth_limit = max(1, min(depth_limit, 5))
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO jobs
              (job_id, seed_url, depth_limit, created_at)
            VALUES (%s,%s,%s,%s)
        """, (job_id, seed_url, depth_limit, now))
    conn.close()
    sqs.send_message(
        QueueUrl=CRAWL_QUEUE_URL,
        MessageBody=json.dumps({'jobId': job_id, 'url': seed_url, 'depth': 0})
    )
    node_status.clear()
    return jsonify({'jobId': job_id}), 202


@app.route('/jobs/<job_id>')
def get_job_status(job_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_id AS jobId,
                   seed_url AS seedUrl,
                   depth_limit AS depthLimit,
                   discovered_count AS discoveredCount,
                   indexed_count AS indexedCount,
                   status,
                   created_at AS createdAt
              FROM jobs WHERE job_id=%s
        """, (job_id,))
        row = cur.fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(row), 200


@app.route('/search')
def search_index():
    """
    Supports:
     - Boolean OR: 'a OR b'
     - Boolean AND (default): 'a b' or 'a AND b'
     - NOT: 'a NOT b'
     - Phrase search: "foo bar"
    """
    raw = (request.args.get('query') or '').lower()
    if not raw.strip():
        return jsonify([]), 200

    # extract quoted phrases
    phrases = re.findall(r'"([^"]+)"', raw)
    raw = re.sub(r'"[^"]+"', '', raw)

    # tokens and operators
    parts = raw.split()
    include, exclude = [], []
    op = 'AND'
    i = 0
    while i < len(parts):
        p = parts[i]
        if p == 'or':
            op = 'OR'
        elif p == 'not' and i+1 < len(parts):
            exclude.append(parts[i+1])
            i += 1
        elif p != 'and':
            include.append(p)
        i += 1

    # stem all terms & phrasess
    terms = [stemmer.stem(w) for w in include]
    term_phrases = []
    for ph in phrases:
        toks = re.findall(r'\w+', ph)
        st = ' '.join(stemmer.stem(w) for w in toks)
        term_phrases.append(st)
    all_terms = terms + term_phrases

    conn = get_connection()
    with conn.cursor() as cur:
        if not all_terms:
            return jsonify([]), 200

        placeholders = ','.join(['%s']*len(all_terms))
        # base query
        sql = f"""
            SELECT page_url AS pageUrl,
                   SUM(frequency) AS frequency,
                   COUNT(DISTINCT term) AS matches
              FROM index_entries
             WHERE term IN ({placeholders})
        """
        params = all_terms[:]
        sql += " GROUP BY page_url"

        # apply AND/OR
        if op == 'AND':
            sql += " HAVING matches = %s"
            params.append(len(all_terms))
        else:  # OR
            sql += " HAVING matches >= 1"

        # apply exclusion
        if exclude:
            ex_stems = [stemmer.stem(w) for w in exclude]
            ex_ph = ','.join(['%s']*len(ex_stems))
            sql += f" AND page_url NOT IN (SELECT page_url FROM index_entries WHERE term IN ({ex_ph}))"
            params.extend(ex_stems)

        sql += " ORDER BY frequency DESC"

        cur.execute(sql, params)
        rows = cur.fetchall()

    conn.close()
    return jsonify([{'pageUrl': r['pageUrl'], 'frequency': r['frequency']} for r in rows]), 200


@app.route('/nodes')
def list_nodes():
    return jsonify({nid: ('alive' if v else 'dead') for nid, v in node_status.items()}), 200


@app.route('/monitor')
def monitor():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT node_id, role,
                   UNIX_TIMESTAMP(last_heartbeat) AS ts,
                   state, current_url
              FROM {HEARTBEAT_TABLE}
        """)
        rows = cur.fetchall()
    conn.close()
    now = time.time()
    data = []
    for r in rows:
        data.append({
            'nodeId':    r['node_id'],
            'role':      r['role'],
            'alive':     (now-r['ts']) < HEARTBEAT_TIMEOUT,
            'state':     r['state'],
            'currentUrl': r['current_url'] or None
        })
    return jsonify(data), 200


if __name__ == '__main__':
    threading.Thread(target=heartbeat_monitor, daemon=True).start()
    app.run(host='0.0.0.0', port=MASTER_PORT, threaded=True)
