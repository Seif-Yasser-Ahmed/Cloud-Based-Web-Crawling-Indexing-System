#!/usr/bin/env python3
"""
master.py — Flask API for distributed crawler/indexer system.

Endpoints:
  POST   /jobs          — start a new crawl job
  GET    /jobs/<jobId>  — get status & progress of a crawl job
  GET    /search        — search indexed pages by term
"""

import os
import uuid
import json
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3
from boto3.dynamodb.conditions import Key

# ─── Configuration ────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL    = os.environ['CRAWL_QUEUE_URL']
JOBS_TABLE_NAME    = os.environ.get('JOBS_TABLE_NAME', 'Jobs')
INDEX_TABLE_NAME   = os.environ.get('INDEX_TABLE_NAME', 'Index')

# ─── AWS Clients & Resources ─────────────────────────────────────────────────
sqs       = boto3.client('sqs')
dynamodb  = boto3.resource('dynamodb')
jobs_tbl  = dynamodb.Table(JOBS_TABLE_NAME)
index_tbl = dynamodb.Table(INDEX_TABLE_NAME)

# ─── Flask App ────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route('/jobs', methods=['POST'])
def start_job():
    """Kick off a new crawl job by enqueuing the seed URL."""
    data     = request.get_json(force=True)
    seed_url = data.get('seedUrl')
    if not seed_url:
        return jsonify({'error': 'Missing seedUrl'}), 400

    # Optional depth limit (default = 2)
    depth_limit = data.get('depthLimit', 2)
    try:
        depth_limit = int(depth_limit)
    except ValueError:
        return jsonify({'error': 'depthLimit must be an integer'}), 400

    # Generate jobId and timestamp
    job_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()

    # Initialize job record in DynamoDB
    jobs_tbl.put_item(Item={
        'jobId':           job_id,
        'seedUrl':         seed_url,
        'depthLimit':      depth_limit,
        'discoveredCount': 0,
        'indexedCount':    0,
        'status':          'PENDING',
        'createdAt':       now
    })

    # Enqueue initial crawl message
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
    """Return the current status & counts for a given jobId."""
    resp = jobs_tbl.get_item(Key={'jobId': job_id})
    item = resp.get('Item')
    if not item:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(item), 200


@app.route('/search', methods=['GET'])
def search_index():
    """Search the inverted-index DynamoDB table for a single term."""
    term = request.args.get('query', '').strip().lower()
    if not term:
        return jsonify([]), 200

    # Query index table by partition key = term
    resp  = index_tbl.query(
        KeyConditionExpression=Key('term').eq(term)
    )
    items = resp.get('Items', [])

    # Build a list of {jobId, pageUrl, frequency}
    results = []
    for it in items:
        jp = it.get('jobPage', '')
        if '#' in jp:
            job_id, page_url = jp.split('#', 1)
        else:
            job_id, page_url = '', jp
        results.append({
            'jobId':    job_id,
            'pageUrl':  page_url,
            'frequency': it.get('frequency', 0)
        })

    # Sort by frequency descending
    results.sort(key=lambda x: x['frequency'], reverse=True)
    return jsonify(results), 200


# ─── Entrypoint ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # Listen on all interfaces, port 80
    app.run(host='0.0.0.0', port=80, threaded=True)
