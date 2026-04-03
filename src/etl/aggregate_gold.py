"""
aggregate_gold.py
Đọc toàn bộ lịch sử silver_dag/{ticker}/*.parquet,
tính các business metrics và lưu gold_dag/{ticker}/{today}.parquet
"""
import os
import time
import logging
from datetime import datetime

os.environ['JAVA_HOME']        = '/usr/lib/jvm/java-11-openjdk-amd64'
os.environ['HADOOP_USER_NAME'] = 'root'

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_SILVER_DAG = 'hdfs://namenode:9000/user/vn30/silver_dag'
HDFS_GOLD_DAG   = 'hdfs://namenode:9000/user/vn30/gold_dag'


def setup_spark():
    spark = SparkSession.builder.appName('aggregate_gold_dag').getOrCreate()
    logger.info('Spark khởi tạo thành công')
    return spark


def add_gold_metrics(df):
    """
    Thêm các cột Gold từ business requirements:
      - price_diff_pct_1d  : % thay đổi so với phiên trước
      - price_diff_pct_1w  : % thay đổi so với 7 phiên trước
      - volume_vs_avg_20d  : volume / avg_volume 20 phiên trước
      - ma20               : trung bình động 20 phiên
      - above_ma20         : giá > MA20
      - dist_from_ma20     : % khoảng cách so với MA20
    Sắp xếp theo year, month, day để đảm bảo thứ tự đúng.
    """
    # Tạo cột date tạm để sort window
    df = df.withColumn('_sort_key',
        f.to_date(f.concat_ws('-',
            f.col('year').cast('string'),
            f.lpad(f.col('month').cast('string'), 2, '0'),
            f.lpad(f.col('day').cast('string'),   2, '0')
        ), 'yyyy-MM-dd')
    )

    w    = Window.partitionBy('ticker').orderBy('_sort_key')
    w_20 = Window.partitionBy('ticker').orderBy('_sort_key').rowsBetween(-20, -1)

    df_gold = df \
        .withColumn('close_1d_ago', f.lag('close', 1).over(w)) \
        .withColumn('close_7d_ago', f.lag('close', 7).over(w)) \
        .withColumn('price_diff_pct_1d',
            f.round((f.col('close') - f.col('close_1d_ago')) / f.col('close_1d_ago') * 100, 2)) \
        .withColumn('price_diff_pct_1w',
            f.round((f.col('close') - f.col('close_7d_ago')) / f.col('close_7d_ago') * 100, 2)) \
        .withColumn('avg_volume_20d', f.avg('volume').over(w_20)) \
        .withColumn('volume_vs_avg_20d',
            f.round(f.col('volume') / f.col('avg_volume_20d'), 2)) \
        .withColumn('ma20', f.round(f.avg('close').over(w_20), 2)) \
        .withColumn('above_ma20',   f.col('close') > f.col('ma20')) \
        .withColumn('dist_from_ma20',
            f.round((f.col('close') - f.col('ma20')) / f.col('ma20') * 100, 2)) \
        .drop('close_1d_ago', 'close_7d_ago', 'avg_volume_20d', '_sort_key')

    return df_gold


def process_one(spark, symbol: str, today: str):
    src_all  = f'{HDFS_SILVER_DAG}/{symbol}/*.parquet'   # toàn bộ lịch sử
    dest     = f'{HDFS_GOLD_DAG}/{symbol}/{today}.parquet'

    try:
        df = spark.read.parquet(src_all)

        if df.rdd.isEmpty():
            logger.warning(f'{symbol}: không có dữ liệu silver, bỏ qua')
            return

        df_gold = add_gold_metrics(df)

        # Chỉ lưu dòng của ngày hôm nay
        df_today = df_gold.filter(
            (f.col('year')  == int(today[:4])) &
            (f.col('month') == int(today[5:7])) &
            (f.col('day')   == int(today[8:10]))
        )

        if df_today.rdd.isEmpty():
            logger.warning(f'{symbol}: không có dữ liệu ngày {today} sau transform')
            return

        for attempt in range(1, 4):
            try:
                df_today.coalesce(1).write.mode('overwrite').parquet(dest)
                logger.info(f'{symbol}: lưu gold -> {dest}')
                return
            except Exception as e:
                if 'SafeMode' in str(e) and attempt < 3:
                    logger.warning('SafeMode, chờ 30s...')
                    time.sleep(30)
                else:
                    raise

    except Exception as e:
        logger.error(f'{symbol}: lỗi aggregate - {e}')


def run(today: str = None):
    today = today or datetime.now().strftime('%Y-%m-%d')
    logger.info(f'=== AGGREGATE GOLD DAG [{today}] ===')

    spark = setup_spark()

    from vnstock import Listing
    symbols = Listing(source='VCI').symbols_by_group('VN30').tolist()

    for symbol in symbols:
        process_one(spark, symbol, today)

    spark.stop()
    logger.info('=== HOÀN THÀNH AGGREGATE GOLD DAG ===')


if __name__ == '__main__':
    run()
