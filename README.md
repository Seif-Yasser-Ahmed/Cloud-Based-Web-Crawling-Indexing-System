# Web Crawling and Indexing Search Engine
Fully distributed, high-performance web crawler and indexing framework designed to scale effortlessly on AWS. Leveraging SQS for task orchestration, multi-threaded crawler and indexer workers, and a Flask-based master API, this project enables automated crawl jobs, robust text processing (including Porter stemming and n-gram indexing), and real-time cluster monitoring. Whether you need to index thousands of web pages or build a searchable archive, this repository provides the tools and examples to get you up and running quickly.

## Overview  
A distributed, high‐throughput web crawler and search indexer built on AWS and MySQL.  
It supports scalable crawling, robust text indexing with n-grams and stemming, and provides a REST API plus real-time monitoring dashboard.

---

## Team Members  
> - [Omar Ahmed](https://github.com/Omarbayom) 
> - [Seif Yasser](https://github.com/Seif-Yasser-Ahmed)
> - [Lina Sameh](https://github.com/Lina-Elsharkawy)
> - [Malak Mahmoud](https://github.com/Malak-Abdelakher)


---

## Main Functionalities  
- **Scalable Crawler**: Multi-threaded workers with dynamic scaling, `robots.txt` compliance, SQS-based task queuing.  
- **High-Throughput Indexer**: Robust HTML parsing, Porter stemming, unigram & bigram frequency indexing.  
- **Distributed Coordination**: Master node orchestrates jobs via SQS, maintains heartbeat state in RDS/DynamoDB.  
- **REST API**: Endpoints for job submission, status, search queries, node health, and AWS metrics.  
- **Real-Time Dashboard**: Web UI for searching, launching jobs, and viewing cluster & AWS Auto Scaling/ELB metrics.

---

## Project Structure  
```

Search-Engine/
├── README.md                  ← This file
├── INSTALLATION_GUIDE.md      ← Setup instructions
├── USER_MANUAL.md             ← End-user guide
├── requirements.txt           ← Core Python dependencies
├── Cloud/                     ← AWS-based deployment
│   ├── requirements.txt
│   ├── scripts/               ← Python services
│   │   ├── aws_adapter.py
│   │   ├── crawler_worker.py
│   │   ├── db.py
│   │   ├── indexer\_worker.py
│   │   ├── init_db.py
│   │   ├── master.py
│   │   ├── migrate_heartbeats.py
│   │   └── static/            ← Dashboard frontend assets
│   └── web/                 
└── Local/                     ← local-only mode

````

---

## Tools & Technologies  
- **Language & Frameworks**: Python 3.8+, Flask, boto3, BeautifulSoup, NLTK  
- **AWS Services**: SQS, S3, RDS (MySQL), Auto Scaling, Application Load Balancer  
- **Database**: MySQL (RDS or self-hosted)  
- **Others**: git, virtualenv, `.env` for configuration

---

## Installation  

1. **Clone & venv**  
   ```bash
   git clone https://github.com/Omarbayom/Search-Engine.git
   cd omarbayom-search-engine
   python3 -m venv venv
   source venv/bin/activate
````

2. **Install Core Dependencies**

   ```bash
   pip install -r requirements.txt
   ```
3. **Cloud Setup**

   ```bash
   cd Cloud
   pip install -r requirements.txt
   ```
4. **Configure Environment**
   Create `Cloud/.env` with your AWS, DB, and queue settings:

   ```ini
   AWS_REGION=…
   CRAWL_QUEUE_URL=…
   INDEX_TASK_QUEUE=…
   S3_BUCKET=…
   DB_HOST=…
   DB_USER=…
   DB_PASS=…
   DB_NAME=…
   HEARTBEAT_TABLE=heartbeats
   CRAWLER_ASG=…
   INDEXER_ASG=…
   DASHBOARD_TG_ARN=…
   ```
5. **Initialize Database**

   ```bash
   cd Cloud/scripts
   python3 init_db.py
   python3 migrate_heartbeats.py
   ```
6. **Run Services**

   ```bash
   # In separate terminals:
   python3 master.py
   python3 crawler_worker.py
   python3 indexer_worker.py
   ```
7. **Access Dashboard**
   Serve `Cloud/scripts/static/index.html` (e.g. via `python3 -m http.server 8000`) and open in browser.

---

## Usage Examples

* **Health Check**

  ```
  GET http://<HOST>:<PORT>/health → OK
  ```
* **Start a Job**

  ```bash
  curl -X POST http://<HOST>:5000/jobs \
       -F "seedUrls=https://example.com"
  ```
* **Search Index**

  ```
  GET /search?query="machine learning" OR data
  ```

---
````
## Contributing

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/foo`)
3. Commit changes (`git commit -m "Add foo"`)
4. Push and open a Pull Request

---

## License

MIT License © 2025
See [LICENSE](./LICENSE) for details.

---
```

