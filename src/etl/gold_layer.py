import os
import time
import logging
from datetime import datetime

os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'
os.environ['HADOOP_USER_NAME'] = 'root'

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_SILVER = 'hdfs://namenode:9000/user/vn30/silver/data_clean.parquet'
HDFS_GOLD   = 'hdfs://namenode:9000/user/vn30/gold'


def setup_spark():
    spark = SparkSession.builder.appName('gold_layer').getOrCreate()
    logger.info('Spark khởi tạo thành công')
    return spark


def transform_gold(df):
    """
    Giữ nguyên tất cả cột Silver, thêm các cột Gold:
      - price_diff_pct_1d  : % thay đổi so với phiên trước
      - price_diff_pct_1w  : % thay đổi so với 7 phiên trước
      - volume_vs_avg_20d  : volume hôm nay / avg volume 20 phiên trước
      - ma20               : trung bình động 20 phiên
      - above_ma20         : giá có trên MA20 không
      - dist_from_ma20     : % khoảng cách so với MA20
    """
    w      = Window.partitionBy('ticker').orderBy('time')
    w_20   = Window.partitionBy('ticker').orderBy('time').rowsBetween(-20, -1)

    df_gold = df.filter(f.col('invalid') == 0) \
        .withColumn('close_1d_ago',  f.lag('close', 1).over(w)) \
        .withColumn('close_7d_ago',  f.lag('close', 7).over(w)) \
        .withColumn('price_diff_pct_1d',
            f.round((f.col('close') - f.col('close_1d_ago')) / f.col('close_1d_ago') * 100, 2)) \
        .withColumn('price_diff_pct_1w',
            f.round((f.col('close') - f.col('close_7d_ago')) / f.col('close_7d_ago') * 100, 2)) \
        .withColumn('avg_volume_20d', f.avg('volume').over(w_20)) \
        .withColumn('volume_vs_avg_20d',
            f.round(f.col('volume') / f.col('avg_volume_20d'), 2)) \
        .withColumn('ma20', f.round(f.avg('close').over(w_20), 2)) \
        .withColumn('above_ma20',    f.col('close') > f.col('ma20')) \
        .withColumn('dist_from_ma20',
            f.round((f.col('close') - f.col('ma20')) / f.col('ma20') * 100, 2)) \
        .drop('close_1d_ago', 'close_7d_ago', 'avg_volume_20d')

    return df_gold


def save_gold(df, retries=3):
    for attempt in range(1, retries + 1):
        try:
            df.write \
                .partitionBy('ticker') \
                .mode('overwrite') \
                .parquet(HDFS_GOLD)
            logger.info(f'Lưu Gold layer thành công: {HDFS_GOLD}')
            return
        except Exception as e:
            if 'SafeMode' in str(e) and attempt < retries:
                logger.warning(f'HDFS SafeMode, chờ 30s... (lần {attempt})')
                time.sleep(30)
            else:
                logger.error(f'Lỗi khi lưu Gold: {e}')
                raise


def run():
    logger.info('=== BẮT ĐẦU GOLD LAYER ===')
    spark = setup_spark()

    logger.info(f'Đọc Silver: {HDFS_SILVER}')
    df_silver = spark.read.parquet(HDFS_SILVER)
    logger.info(f'Số dòng Silver: {df_silver.count()}')

    df_gold = transform_gold(df_silver)
    logger.info('Transform Gold hoàn thành')

    save_gold(df_gold)
    logger.info('=== HOÀN THÀNH GOLD LAYER ===')
    spark.stop()


if __name__ == '__main__':
    run()
