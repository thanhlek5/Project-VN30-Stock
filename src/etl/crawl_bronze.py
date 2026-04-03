"""
crawl_bronze.py
Lấy dữ liệu VN30 của ngày hôm đó từ vnstock và lưu lên HDFS.
Cấu trúc: bronze_dag/{ticker}/{YYYY-MM-DD}.csv
- Mỗi request cách nhau 1-2 giây để tránh rate limit
- Sau mỗi 19 mã, sleep 60 giây với bộ đếm ngược trên CLI
"""
import os
import sys
import time
import random
import logging
from datetime import datetime

from vnstock import Listing, Vnstock
from hdfs import InsecureClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_URL    = os.getenv('HDFS_URL', 'http://namenode:9870')
HADOOP_USER = os.getenv('HADOOP_USER_NAME', 'root')
BRONZE_DAG  = '/user/vn30/bronze_dag'
BATCH_SIZE  = 19
WAIT_SEC    = 60


def get_hdfs_client():
    return InsecureClient(HDFS_URL, user=HADOOP_USER, root='/')


def get_vn30_symbols():
    listing = Listing(source='VCI')
    symbols = listing.symbols_by_group('VN30').tolist()
    logger.info(f'VN30 symbols ({len(symbols)}): {symbols}')
    return symbols


def crawl_one(symbol: str, today: str, hdfs_client: InsecureClient):
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        df = stock.quote.history(start=today, end=today, interval='1D')

        if df is None or df.empty:
            logger.warning(f'{symbol}: không có dữ liệu ngày {today}')
            return

        if 'ticker' not in df.columns:
            df.insert(0, 'ticker', symbol)

        hdfs_path = f'{BRONZE_DAG}/{symbol}/{today}.csv'
        hdfs_client.makedirs(f'{BRONZE_DAG}/{symbol}')
        with hdfs_client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
            df.to_csv(writer, index=False)

        logger.info(f'{symbol}: OK ({len(df)} dòng) -> {hdfs_path}')
    except Exception as e:
        logger.error(f'{symbol}: lỗi - {e}')


def countdown(seconds: int):
    """Hiển thị bộ đếm ngược trên cùng 1 dòng CLI."""
    for remaining in range(seconds, 0, -1):
        sys.stdout.write(f'\r  ⏳ Chờ rate limit... còn {remaining:3d}s ')
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write('\r  ✅ Tiếp tục crawl...                    \n')
    sys.stdout.flush()


def run(today: str = None):
    today = today or datetime.now().strftime('%Y-%m-%d')
    logger.info(f'=== CRAWL BRONZE DAG [{today}] ===')

    symbols     = get_vn30_symbols()
    hdfs_client = get_hdfs_client()

    for i, symbol in enumerate(symbols):
        crawl_one(symbol, today, hdfs_client)

        # Delay 1-2 giây giữa các request
        delay = random.uniform(1.0, 2.0)
        time.sleep(delay)

        # Sau mỗi BATCH_SIZE mã (không phải mã cuối), sleep 60s
        if (i + 1) % BATCH_SIZE == 0 and (i + 1) < len(symbols):
            logger.info(f'Đã crawl {i + 1} mã, nghỉ {WAIT_SEC}s để tránh rate limit...')
            countdown(WAIT_SEC)

    logger.info('=== HOÀN THÀNH CRAWL BRONZE DAG ===')


if __name__ == '__main__':
    run()
