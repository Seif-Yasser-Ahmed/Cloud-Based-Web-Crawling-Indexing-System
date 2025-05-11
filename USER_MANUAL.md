# User Manual: Distributed Crawler & Indexer System

This manual guides you through setting up, deploying, and operating the **Puplic\_Project** distributed crawler/indexer system on AWS.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Directory Structure](#directory-structure)
4. [Configuration](#configuration)
5. [Database Initialization](#database-initialization)
6. [AWS Infrastructure Setup](#aws-infrastructure-setup)
7. [Deploying Services](#deploying-services)

   * [Crawler Worker](#crawler-worker)
   * [Indexer Worker](#indexer-worker)
   * [Master Dashboard](#master-dashboard)
8. [Using the Dashboard UI](#using-the-dashboard-ui)
9. [Cost Management & Cleanup](#cost-management--cleanup)
10. [Troubleshooting](#troubleshooting)

---

## Overview

This system implements a scalable, distributed web crawler and indexer architecture using:

* **AWS SQS** for task queues
* **AWS RDS (MySQL)** for metadata and index storage
* **AWS Auto Scaling Groups (ASG)** for horizontal scaling of crawler and indexer
* **AWS Application Load Balancer (ALB)** for routing to the Flask master dashboard
* **Flask** + **Whoosh**-style search API

Workers coordinate via a **heartbeats** table in RDS, enabling live monitoring of node liveness and activity.

---

## Prerequisites

* AWS account with permissions for: EC2, AutoScaling, SQS, RDS, ALB, CloudWatch, IAM
* An **EC2 Key Pair** for SSH access
* [Python 3.8+](https://www.python.org) installed on development machine
* **MySQL** client (e.g. `mysql` CLI) if you need direct RDS access

---

## Directory Structure

```
omarbayom-puplic_project/
├── requirements.txt           # Python dependencies
├── scripts/                  
│   ├── aws_adapter.py        # SQS, S3, DynamoDB helpers
│   ├── crawler_worker.py     # Multi-threaded crawler logic
│   ├── db.py                 # RDS connection helper
│   ├── indexer_worker.py     # Multi-threaded indexer logic
│   ├── init_db.py            # DDL to create tables
│   ├── migrate_heartbeats.py # Alters heartbeats schema
│   ├── master.py             # Flask API + metrics endpoints
│   └── static/               # Dashboard HTML,
│       └── index.html        # refined UI code
└── web/                      # *deprecated* (no longer used)
    ├── README.md            
    ├── app.js               
    └── index.html           
```

---

## Configuration

1. **Set environment variables** (in your EC2 launch template or via Systemd unit):

   ```ini
   AWS_REGION=<region>
   CRAWL_QUEUE_URL=<SQS crawl queue URL>
   INDEX_TASK_QUEUE=<SQS index queue URL>
   S3_BUCKET=<optional S3 bucket for raw page storage>
   HEARTBEAT_TABLE=heartbeats
   DB_HOST=<RDS endpoint>
   DB_PORT=3306
   DB_USER=<db username>
   DB_PASS=<db password>
   DB_NAME=<db name>
   CRAWLER_ASG=crawler-asg
   INDEXER_ASG=indexer-asg
   DASHBOARD_TG_ARN=<ALB target group ARN>
   ```
2. **Install dependencies** on each instance:

   ```bash
   git clone <repo_url>
   cd puplic_project
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   nltk.download('punkt')  # if prompted
   ```

---

## Database Initialization

On a bastion or management host with RDS access:

```bash
source venv/bin/activate
python scripts/init_db.py        # Creates jobs, index_entries, heartbeats tables
python scripts/migrate_heartbeats.py  # Adds state & current_url columns
```

---

## AWS Infrastructure Setup

1. **Create SQS queues** for `crawlTaskQueue` and `indexTaskQueue`.
2. **Provision RDS MySQL instance** and note its endpoint.
3. **Create Security Groups** allowing:

   * Workers ↔ RDS (port 3306)
   * Dashboard ALB ↔ Master (port 5000)
   * SSH (port 22) from your IP
4. **Set up ASGs**:

   * **crawler-asg** and **indexer-asg** with Launch Templates using your AMI (Ubuntu) and user-data to bootstrap workers.
   * Attach appropriate IAM instance profile to allow SQS, S3, DynamoDB, CloudWatch access.
5. **Configure Application Load Balancer**:

   * Create ALB, listener on port 80 → target group pointing at Master instances on port 5000.
   * Health check: `GET /health` → 200 OK.
6. **Configure CloudWatch metrics** or use the built‑in `/metrics/asg` and `/metrics/lb` endpoints for live counts.

---

## Deploying Services

### Crawler Worker

1. Launch (or ASG) instances with user-data:

   ```bash
   #!/bin/bash
   ```

echo "Bootstrapping crawler..."
cd /home/ubuntu/puplic\_project
git pull
source venv/bin/activate
systemctl restart crawler.service

````
2. Systemd unit (`/etc/systemd/system/crawler.service`):
```ini
[Unit]
Description=Crawler Worker
After=network.target

[Service]
Environment="AWS_REGION=..."
# ... other ENVs as above
ExecStart=/home/ubuntu/puplic_project/venv/bin/python scripts/crawler_worker.py
Restart=on-failure
User=ubuntu
WorkingDirectory=/home/ubuntu/puplic_project/scripts

[Install]
WantedBy=multi-user.target
````

3. `sudo systemctl enable --now crawler.service`

### Indexer Worker

Analogous to crawler, using `indexer_worker.py` and `indexer.service` unit. Ensure `Environment="INDEX_TASK_QUEUE=..."` is set.

### Master Dashboard

1. Systemd unit (`master.service`): similar, with ExecStart pointing to `scripts/master.py`.
2. Ensure `app.send_static_file('index.html')` serves from `scripts/static/index.html`.
3. Enable & start: `sudo systemctl enable --now master.service`.
4. Confirm health: `curl http://localhost:5000/health` → `OK`.

---

## Using the Dashboard UI

* Navigate to your ALB DNS (e.g. `http://dashboard-alb-...`) in browser.
* **Search**, **Start Crawl Jobs**, **Monitor Nodes**, and **View ASG / LB metrics** live.

---

## Cost Management & Cleanup

* **Stop ASGs**: reduce desired capacity to 0.
* **Delete Load Balancer**, **Target Group**, **Launch Templates/ASGs**, **RDS instance** when idle.
* **Purge SQS queues** if desired.

---

## Troubleshooting

* **Service logs**: `sudo journalctl -u <service>.service -f`
* **Health check failures**: check `master.service` logs and security groups.
* **Missing ENV errors**: verify `Environment=` entries in your Systemd unit or user-data.
* **Database errors**: test `mysql -h $DB_HOST -u $DB_USER -p$DB_PASS $DB_NAME` and run DDL scripts.
* **Scaling issues**: inspect ASG console, CloudWatch metrics, and ensure IAM roles allow CloudWatch Describe calls.

---

*End of manual.*

