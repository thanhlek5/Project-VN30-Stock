"""
pg_dag.py
Đọc gold_dag/{ticker}/{today}.parquet từ HDFS và upsert vào PostgreSQL.
"""
import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

os.environ['JAVA_HOME']        = '/usr/lib/jvm/java-11-openjdk-amd64'
os.environ['HADOOP_USER_NAME'] = 'root'

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_GOLD_DAG = 'hdfs://namenode:9000/user/vn30/gold_dag'

PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB   = os.getenv('POSTGRES_DB',       'vn30_db')
PG_USER = os.getenv('POSTGRES_USER',     'admin')
PG_PASS = os.getenv('POSTGRES_PASSWORD', 'admin123')

UPSERT_SQL = """
INSERT INTO vn30_gold_layer (
    time, ticker, open, high, low, close, volume,
    invalid, month, year, day,
    price_diff_pct_1d, price_diff_pct_1w,
    volume_vs_avg_20d, ma20, above_ma20, dist_from_ma20
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

# Tạo cột time từ year/month/day vì silver_dag không có cột time
COLS = [
    'time', 'ticker', 'open', 'high', 'low', 'close', 'volume',
    'invalid', 'month', 'year', 'day',
    'price_diff_pct_1d', 'price_diff_pct_1w',
    'volume_vs_avg_20d', 'ma20', 'above_ma20', 'dist_from_ma20'
]


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )


def run(today: str = None):
    today = today or datetime.now().strftime('%Y-%m-%d')
    logger.info(f'=== PG DAG [{today}] ===')

    spark = SparkSession.builder.appName('pg_dag').getOrCreate()

    from vnstock import Listing
    symbols = Listing(source='VCI').symbols_by_group('VN30').tolist()

    conn  = get_pg_conn()
    total = 0

    for symbol in symbols:
        src = f'{HDFS_GOLD_DAG}/{symbol}/{today}.parquet'
        try:
            df = spark.read.parquet(src)

            if df.rdd.isEmpty():
                logger.warning(f'{symbol}: file gold trống, bỏ qua')
                continue

            # Tạo cột time từ year/month/day
            from pyspark.sql import functions as f_sql
            df = df.withColumn('time',
                f_sql.to_date(f_sql.concat_ws('-',
                    f_sql.col('year').cast('string'),
                    f_sql.lpad(f_sql.col('month').cast('string'), 2, '0'),
                    f_sql.lpad(f_sql.col('day').cast('string'),   2, '0')
                ), 'yyyy-MM-dd')
            )

            rows  = df.select(*COLS).collect()
            batch = [tuple(r[c] for c in COLS) for r in rows]

            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, batch, page_size=200)
            conn.commit()

            total += len(batch)
            logger.info(f'{symbol}: upsert {len(batch)} dòng')

        except Exception as e:
            logger.error(f'{symbol}: lỗi - {e}')
            conn.rollback()

    conn.close()
    spark.stop()
    logger.info(f'=== HOÀN THÀNH PG DAG: {total} dòng ===')


if __name__ == '__main__':
    run()
