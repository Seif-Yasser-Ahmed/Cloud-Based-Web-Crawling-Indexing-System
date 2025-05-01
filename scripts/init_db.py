#!/usr/bin/env python3
"""
init_db.py — Ensure RDS schema exists: create `jobs` and `index_entries` tables if needed.
"""

from db import get_connection

DDL = [
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
    """,
    """
    CREATE TABLE IF NOT EXISTS index_entries (
      term           VARCHAR(255) NOT NULL,
      job_id         CHAR(36)     NOT NULL,
      page_url       TEXT         NOT NULL,
      page_url_hash  CHAR(32)     NOT NULL,
      frequency      INT          NOT NULL DEFAULT 1,
      PRIMARY KEY (term, job_id, page_url_hash)
    ) ENGINE=InnoDB CHARSET=utf8mb4;
    """
]

def main():
    conn = get_connection()
    with conn.cursor() as cur:
        for stmt in DDL:
            print("Executing:", stmt.splitlines()[0], "…")
            cur.execute(stmt)
    conn.close()
    print("✅ Tables are ensured.")

if __name__ == '__main__':
    main()
