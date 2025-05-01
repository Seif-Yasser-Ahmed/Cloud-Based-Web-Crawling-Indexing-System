#!/usr/bin/env python3
"""
migrate_heartbeats.py — Add state and current_url columns to the heartbeats table
"""

import logging
from db import get_connection

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='[MIGRATION] %(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("migrate_heartbeats")


def check_column_exists(cursor, table, column):
    """Check if a column exists in a table"""
    cursor.execute(f"""
        SELECT COUNT(*) as count 
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = '{table}' 
        AND COLUMN_NAME = '{column}'
    """)
    return cursor.fetchone()['count'] > 0


def main():
    logger.info("Starting heartbeats table migration...")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # First check if table exists, if not create it
            cur.execute("""
                CREATE TABLE IF NOT EXISTS heartbeats (
                    node_id         VARCHAR(255) PRIMARY KEY,
                    role            VARCHAR(50)  NOT NULL,
                    last_heartbeat  DATETIME     NOT NULL
                ) ENGINE=InnoDB CHARSET=utf8mb4;
            """)
            logger.info("Ensured heartbeats table exists")

            # Add state column if it doesn't exist
            if not check_column_exists(cur, 'heartbeats', 'state'):
                logger.info("Adding 'state' column to heartbeats table")
                cur.execute("""
                    ALTER TABLE heartbeats 
                    ADD COLUMN state VARCHAR(50) DEFAULT 'waiting'
                """)
                logger.info("Added 'state' column successfully")
            else:
                logger.info("'state' column already exists")

            # Add current_url column if it doesn't exist
            if not check_column_exists(cur, 'heartbeats', 'current_url'):
                logger.info("Adding 'current_url' column to heartbeats table")
                cur.execute("""
                    ALTER TABLE heartbeats 
                    ADD COLUMN current_url TEXT NULL
                """)
                logger.info("Added 'current_url' column successfully")
            else:
                logger.info("'current_url' column already exists")

        conn.commit()
        logger.info("Migration completed successfully")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
