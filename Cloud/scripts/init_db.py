#!/usr/bin/env python3
"""
init_db.py — Ensure RDS schema exists: create `jobs`, `index_entries`, 
and `crawler_heartbeats` tables if needed.
"""

from db import get_connection

DDL = [
    # existing jobs table
    """
    CREATE TABLE IF NOT EXISTS jobs (
      job_id           CHAR(36)     PRIMARY KEY,
      seed_url         TEXT         NOT NULL,
      depth_limit      INT          NOT NULL DEFAULT 2,
      discovered_count INT          NOT NULL DEFAULT 0,
      indexed_count    INT          NOT NULL DEFAULT 0,
      status           VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
      created_at       DATETIME     NOT NULL
    ) ENGINE=InnoDB CHARSET=utf8mb4;
    """,  # :contentReference[oaicite:0]{index=0}&#8203;:contentReference[oaicite:1]{index=1}

    # existing index_entries table
    """
    CREATE TABLE IF NOT EXISTS index_entries (
      term           VARCHAR(255) NOT NULL,
      job_id         CHAR(36)     NOT NULL,
      page_url       TEXT         NOT NULL,
      page_url_hash  CHAR(32)     NOT NULL,
      frequency      INT          NOT NULL DEFAULT 1,
      PRIMARY KEY (term, job_id, page_url_hash)
    ) ENGINE=InnoDB CHARSET=utf8mb4;
    """,  # :contentReference[oaicite:2]{index=2}&#8203;:contentReference[oaicite:3]{index=3}

    # new crawler_heartbeats table for RDS-based node liveness
    """
    CREATE TABLE IF NOT EXISTS crawler_heartbeats (
      node_id         VARCHAR(128) PRIMARY KEY,
      last_heartbeat  TIMESTAMP    NOT NULL
    ) ENGINE=InnoDB CHARSET=utf8mb4;
    """
]


def main():
    conn = get_connection()
    with conn.cursor() as cur:
        for stmt in DDL:
            # show just first line to keep logs tidy
            print("Executing:", stmt.strip().splitlines()[0], "…")
            cur.execute(stmt)
    conn.close()
    print("✅ All tables are ensured.")


if __name__ == '__main__':
    main()
