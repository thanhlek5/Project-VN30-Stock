"""
Đẩy dữ liệu từ HDFS Gold layer vào PostgreSQL.
Dùng psycopg2 để upsert (INSERT ... ON CONFLICT DO UPDATE).
"""
import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'
os.environ['HADOOP_USER_NAME'] = 'root'

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Config ---
HDFS_GOLD = 'hdfs://namenode:9000/user/vn30/gold'

PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB   = os.getenv('POSTGRES_DB',       'vn30_db')
PG_USER = os.getenv('POSTGRES_USER',     'admin')
PG_PASS = os.getenv('POSTGRES_PASSWORD', 'admin123')

UPSERT_SQL = """
INSERT INTO vn30_gold_layer (
    time, ticker, open, high, low, close, volume,
    invalid, month, year, day,
    price_diff_pct_1d, price_diff_pct_1w,
    volume_vs_avg_20d,
    ma20, above_ma20, dist_from_ma20
) VALUES %s
ON CONFLICT (ticker, time) DO UPDATE SET
    open              = EXCLUDED.open,
    high              = EXCLUDED.high,
    low               = EXCLUDED.low,
    close             = EXCLUDED.close,
    volume            = EXCLUDED.volume,
    invalid           = EXCLUDED.invalid,
    price_diff_pct_1d = EXCLUDED.price_diff_pct_1d,
    price_diff_pct_1w = EXCLUDED.price_diff_pct_1w,
    volume_vs_avg_20d = EXCLUDED.volume_vs_avg_20d,
    ma20              = EXCLUDED.ma20,
    above_ma20        = EXCLUDED.above_ma20,
    dist_from_ma20    = EXCLUDED.dist_from_ma20,
    created_at        = NOW();
"""

COLS = [
    'time', 'ticker', 'open', 'high', 'low', 'close', 'volume',
    'invalid', 'month', 'year', 'day',
    'price_diff_pct_1d', 'price_diff_pct_1w',
    'volume_vs_avg_20d',
    'ma20', 'above_ma20', 'dist_from_ma20'
]


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )


def upsert_partition(rows, conn):
    """Upsert 1 batch rows vào Postgres."""
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=500)
    conn.commit()


def run():
    logger.info('=== BẮT ĐẦU GOLD → POSTGRES ===')

    # 1. Đọc Gold từ HDFS
    spark = SparkSession.builder.appName('gold_to_pg').getOrCreate()
    df = spark.read.parquet(HDFS_GOLD)
    logger.info(f'Số dòng Gold: {df.count()}')

    # 2. Kết nối Postgres
    conn = get_pg_conn()
    logger.info(f'Kết nối Postgres: {PG_HOST}:{PG_PORT}/{PG_DB}')

    # 3. Upsert theo từng ticker (tránh OOM)
    tickers = [r.ticker for r in df.select('ticker').distinct().collect()]
    logger.info(f'Số ticker: {len(tickers)}')

    total = 0
    for ticker in tickers:
        rows = df.filter(df.ticker == ticker) \
                 .select(*COLS) \
                 .collect()
        batch = [tuple(r[c] for c in COLS) for r in rows]
        upsert_partition(batch, conn)
        total += len(batch)
        logger.info(f'  {ticker}: {len(batch)} dòng')

    conn.close()
    spark.stop()
    logger.info(f'=== HOÀN THÀNH: {total} dòng đã upsert ===')


if __name__ == '__main__':
    run()
