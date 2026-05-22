import pymysql
import pymysql.cursors
import config
from decimal import Decimal


def get_conn():
    return pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        database=config.MYSQL_DATABASE,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def query(sql: str, params=None) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
        return [_clean(r) for r in rows]
    finally:
        conn.close()


def _clean(row: dict) -> dict:
    return {k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()}
