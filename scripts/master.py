# updated master.py with CloudWatch metric endpoints and re-added heartbeat_monitor
#!/usr/bin/env python3
"""
master.py — Flask API for distributed crawler/indexer with enhanced
boolean & phrase search, plus real-time monitoring and AWS metrics.
"""

import os
import re
import json
import uuid
import logging
import threading
import time
from datetime import datetime, timezone, timedelta

from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3

from nltk.stem.porter import PorterStemmer
from db import get_connection

# ──────────────────────────────────────────────────────────────────────────────
# Environment and AWS clients
CRAWL_QUEUE_URL = os.environ['CRAWL_QUEUE_URL']
MASTER_PORT = int(os.environ.get('MASTER_PORT', 5000))
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')
HEARTBEAT_TIMEOUT = int(os.environ.get('HEARTBEAT_TIMEOUT', 60))
HEARTBEAT_POLL_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', 30))

# New: ASG and LB names via env
CRAWLER_ASG = os.environ.get('CRAWLER_ASG', 'crawler-asg')
INDEXER_ASG = os.environ.get('INDEXER_ASG', 'indexer-asg')
DASHBOARD_TG_ARN = os.environ['DASHBOARD_TG_ARN']  # full TG ARN for dashboard

# AWS clients
sqs = boto3.client('sqs', region_name=os.environ['AWS_REGION'])
cw = boto3.client('cloudwatch', region_name=os.environ['AWS_REGION'])
app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
stemmer = PorterStemmer()

node_status = {}

# Re-added heartbeat monitor function


def heartbeat_monitor():
    while True:
        time.sleep(HEARTBEAT_POLL_INTERVAL)
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT node_id, UNIX_TIMESTAMP(last_heartbeat) AS ts FROM {HEARTBEAT_TABLE}"
                )
                rows = cur.fetchall()
            conn.close()
            now = time.time()
            for r in rows:
                node_status[r['node_id']] = (now - r['ts']) < HEARTBEAT_TIMEOUT
        except Exception as e:
            logger.error(f"Heartbeat monitor error: {e}")


@app.route('/')
def home():
    return app.send_static_file('index.html')


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
            INSERT INTO jobs (job_id, seed_url, depth_limit, created_at)
            VALUES (%s,%s,%s,%s)""", (job_id, seed_url, depth_limit, now))
    conn.close()
    sqs.send_message(QueueUrl=CRAWL_QUEUE_URL, MessageBody=json.dumps(
        {'jobId': job_id, 'url': seed_url, 'depth': 0}))
    node_status.clear()
    return jsonify({'jobId': job_id}), 202


@app.route('/jobs/<job_id>')
def get_job_status(job_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_id AS jobId, seed_url AS seedUrl, depth_limit AS depthLimit,
                   discovered_count AS discoveredCount, indexed_count AS indexedCount,
                   status, created_at AS createdAt
              FROM jobs WHERE job_id=%s""", (job_id,))
        row = cur.fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(row), 200


@app.route('/search')
def search_index():
    raw = (request.args.get('query') or '').lower()
    if not raw.strip():
        return jsonify([]), 200
    phrases = re.findall(r'"([^"]+)"', raw)
    raw = re.sub(r'"[^"]+"', '', raw)
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
    terms = [stemmer.stem(w) for w in include]
    term_phrases = []
    for ph in phrases:
        toks = re.findall(r'\w+', ph)
        term_phrases.append(' '.join(stemmer.stem(w) for w in toks))
    all_terms = terms + term_phrases
    conn = get_connection()
    with conn.cursor() as cur:
        if not all_terms:
            return jsonify([]), 200
        placeholders = ','.join(['%s']*len(all_terms))
        sql = f"""
            SELECT page_url AS pageUrl, SUM(frequency) AS frequency,
                   COUNT(DISTINCT term) AS matches
              FROM index_entries WHERE term IN ({placeholders})
        """
        params = list(all_terms)
        sql += " GROUP BY page_url"
        if op == 'AND':
            sql += " HAVING matches = %s"
            params.append(len(all_terms))
        else:
            sql += " HAVING matches >= 1"
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
            SELECT node_id, role, UNIX_TIMESTAMP(last_heartbeat) AS ts,
                   state, current_url
              FROM {HEARTBEAT_TABLE}
        """)
        rows = cur.fetchall()
    conn.close()
    now = time.time()
    data = []
    for r in rows:
        data.append({
            'nodeId': r['node_id'],
            'role':   r['role'],
            'alive':  (now-r['ts']) < HEARTBEAT_TIMEOUT,
            'state':  r['state'],
            'currentUrl': r['current_url'] or None
        })
    return jsonify(data), 200


@app.route('/metrics/asg')
def metrics_asg():
    now = datetime.utcnow()
    start = now - timedelta(minutes=5)
    result = {}
    for asg in [CRAWLER_ASG, INDEXER_ASG]:
        resp = cw.get_metric_statistics(
            Namespace='AWS/AutoScaling', MetricName='GroupInServiceInstances',
            Dimensions=[{'Name': 'AutoScalingGroupName', 'Value': asg}],
            StartTime=start, EndTime=now, Period=60, Statistics=['Average']
        )
        dps = sorted(resp.get('Datapoints', []), key=lambda d: d['Timestamp'])
        result[asg] = [{'timestamp': dp['Timestamp'].isoformat(
        ), 'value': dp['Average']} for dp in dps]
    return jsonify(result), 200


@app.route('/metrics/lb')
def metrics_lb():
    now = datetime.utcnow()
    start = now - timedelta(minutes=5)
    dims = [{'Name': 'TargetGroup', 'Value': DASHBOARD_TG_ARN}]
    metrics = {}
    for name in ['HealthyHostCount', 'RequestCountPerTarget']:
        resp = cw.get_metric_statistics(
            Namespace='AWS/ApplicationELB', MetricName=name,
            Dimensions=dims, StartTime=start, EndTime=now, Period=60, Statistics=['Average']
        )
        dps = sorted(resp.get('Datapoints', []), key=lambda d: d['Timestamp'])
        metrics[name] = [
            {'timestamp': dp['Timestamp'].isoformat(), 'value': dp['Average']} for dp in dps]
    return jsonify(metrics), 200


if __name__ == '__main__':
    threading.Thread(target=heartbeat_monitor, daemon=True).start()
    app.run(host='0.0.0.0', port=MASTER_PORT, threaded=True)
