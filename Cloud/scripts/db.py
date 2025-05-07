import os, pymysql
from pymysql.cursors import DictCursor

def get_connection():
    return pymysql.connect(
        host=os.environ['DB_HOST'],
        port=int(os.environ.get('DB_PORT', 3306)),
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        db=os.environ['DB_NAME'],
        charset='utf8mb4',
        cursorclass=DictCursor,
        autocommit=True
    )
