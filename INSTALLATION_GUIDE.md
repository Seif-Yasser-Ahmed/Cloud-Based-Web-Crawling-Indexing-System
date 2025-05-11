# Installation Guide

This guide walks you through installing, configuring, and running the distributed crawler/indexer system.

---

## Table of Contents

1. [Introduction](#introduction)
2. [Installation Guide](#installation-guide)
3. [Configuration](#configuration)
4. [Deployment](#deployment)
5. [Usage](#usage)
6. [Monitoring & Scaling](#monitoring--scaling)
7. [Troubleshooting](#troubleshooting)
8. [Cleanup & Cost Saving](#cleanup--cost-saving)

---

## Introduction

This project provides a scalable, distributed web crawler and indexer using AWS services (EC2 Auto Scaling groups, SQS, RDS, ELB/ALB, CloudWatch). It includes:

* **Crawler workers**: fetch pages, obey robots.txt, send content to index queue
* **Indexer workers**: tokenize and stem content, insert into RDS
* **Master dashboard**: Flask-based UI & API for jobs, search, monitoring, ASG & LB metrics
* **Infrastructure as code** via launch templates and user-data scripts

---

## Installation Guide

Follow these steps to install locally or on AWS.

### Prerequisites

* **AWS account** with EC2, SQS, RDS, ELB, CloudWatch privileges
* **Python 3.9+**
* **Git**
* Optional: **Docker** (for local testing)

### 1. Clone the Repository

```bash
git clone https://github.com/omarbayom/omarbayom-puplic_project.git
cd omarbayom-puplic_project
```

### 2. Create & Activate Python Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Provision RDS (MySQL) Instance

1. In AWS Console, navigate to RDS > Create database (MySQL).
2. Record endpoint, port, username, password.
3. Ensure security group allows inbound from EC2.

### 5. Initialize Database Schema

Set environment variables and run the initializer:

```bash
export DB_HOST=<your-rds-endpoint>
export DB_PORT=3306
export DB_USER=<username>
export DB_PASS=<password>
export DB_NAME=crawler
```

```bash
python scripts/init_db.py
python scripts/migrate_heartbeats.py
```

### 6. Create SQS Queues & DynamoDB Table

* **Crawler queue** for crawl tasks (names `crawlTaskQueue`)
* **Index queue** for index tasks (names `indexTaskQueue`)
* **Heartbeat table** in DynamoDB (optional, not used by default)

### 7. Configure AWS CLI / IAM

Ensure `AWS_REGION`, credentials, and IAM roles/policies are configured. You may use an EC2 instance profile with:

* SQS read/write
* RDS access
* CloudWatch read
* ELB/ASG read

### 8. Set Up EC2 Auto Scaling & Load Balancer

1. **Launch Templates**:

   * One template for crawler ASG (Ubuntu AMI, user-data to install & start `crawler_worker.py`).
   * One template for indexer ASG (user-data to install & start `indexer_worker.py`).
   * One template for dashboard (master) service.
2. **Auto Scaling Groups**:

   * Create ASG for crawler and indexer using corresponding templates.
   * Set desired/min/max capacity.
3. **Application Load Balancer**:

   * Target group pointing to dashboard instances on port 5000.
   * Health check `/health`.
   * Listener on port 80 forwarding to target group.

### 9. Deploy Services via User Data

In each launch template’s user data, install dependencies and start systemd service:

```bash
#!/bin/bash
cd /home/ubuntu/omarbayom-puplic_project
git pull
source venv/bin/activate
pip install -r requirements.txt
cp scripts/<service>.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable <service>.service
systemctl start <service>.service
```

---

## Configuration

Environment variables used by services:

| Name               | Description                            |
| ------------------ | -------------------------------------- |
| `AWS_REGION`       | AWS region (e.g. `eu-north-1`)         |
| `DB_HOST`          | RDS endpoint                           |
| `DB_PORT`          | RDS port (default `3306`)              |
| `DB_USER`          | RDS username                           |
| `DB_PASS`          | RDS password                           |
| `DB_NAME`          | RDS database name (`crawler`)          |
| `CRAWL_QUEUE_URL`  | SQS URL for crawling                   |
| `INDEX_TASK_QUEUE` | SQS URL for indexing                   |
| `HEARTBEAT_TABLE`  | Name of RDS heartbeat table            |
| `CRAWLER_ASG`      | Name of crawler Auto Scaling group     |
| `INDEXER_ASG`      | Name of indexer Auto Scaling group     |
| `DASHBOARD_TG_ARN` | Target group ARN for dashboard ELB     |
| `MASTER_PORT`      | Port for Flask master (default `5000`) |

---

## Deployment

1. Push code to GitHub (or your repo).
2. Update launch templates with the latest commit tag or branch.
3. Roll out new instances by updating ASG.

---

## Usage

* **Dashboard UI**: browse to `http://<ALB-DNS>/`.
* **Start crawl jobs** via form or `POST /jobs`.
* **Search** via UI or `GET /search?query=<terms>`.
* **Monitor nodes** via UI or `GET /monitor`.
* **Metrics**: `GET /metrics/asg`, `GET /metrics/lb`.

---

## Monitoring & Scaling

* CloudWatch alarms can be set on CPU or queue backlog to scale ASG.
* Use the `/metrics` endpoints in your dashboards.

---

## Troubleshooting

* Check systemd logs: `sudo journalctl -u <service>.service`.
* Verify environment variables in `/etc/systemd/system/<service>.service`.
* Confirm security groups allow necessary ports (5000, 80).
* Inspect CloudWatch metrics for ASG and LB.

---

## Cleanup & Cost Saving

To avoid AWS charges when idle:

1. Set ASG desired capacity to 0 for crawler & indexer.
2. Delete ALB and target groups.
3. Stop or delete RDS instance.
4. Remove SQS queues.
5. Delete EC2 launch templates and ASGs.
6. Tear down DynamoDB / other resources.

---

*End of Installation Guide*

