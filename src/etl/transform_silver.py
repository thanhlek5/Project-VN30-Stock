"""
transform_silver.py
Đọc CSV từ bronze_dag/{ticker}/{today}.csv, transform và lưu parquet
vào silver_dag/{ticker}/{today}.parquet
"""
import os
import time
import logging
from datetime import datetime

os.environ['JAVA_HOME']       = '/usr/lib/jvm/java-11-openjdk-amd64'
os.environ['HADOOP_USER_NAME'] = 'root'

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col, to_date, when, lit

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HDFS_BRONZE_DAG = 'hdfs://namenode:9000/user/vn30/bronze_dag'
HDFS_SILVER_DAG = 'hdfs://namenode:9000/user/vn30/silver_dag'

SCHEMA = StructType([
    StructField('ticker', StringType(), True),
    StructField('time',   StringType(), True),
    StructField('open',   StringType(), True),
    StructField('high',   StringType(), True),
    StructField('low',    StringType(), True),
    StructField('close',  StringType(), True),
    StructField('volume', StringType(), True),
])


def setup_spark():
    spark = SparkSession.builder.appName('transform_silver_dag').getOrCreate()
    logger.info('Spark khởi tạo thành công')
    return spark


def transform_one(spark, symbol: str, today: str):
    src  = f'{HDFS_BRONZE_DAG}/{symbol}/{today}.csv'
    dest = f'{HDFS_SILVER_DAG}/{symbol}/{today}.parquet'

    try:
        df = spark.read.schema(SCHEMA).option('header', 'true').csv(src)

        if df.rdd.isEmpty():
            logger.warning(f'{symbol}: file bronze trống, bỏ qua')
            return

        df_clean = df \
            .withColumn('ticker', when(col('ticker').isNull(), lit(symbol)).otherwise(col('ticker'))) \
            .withColumn('open',   col('open').cast(DoubleType())) \
            .withColumn('high',   col('high').cast(DoubleType())) \
            .withColumn('low',    col('low').cast(DoubleType())) \
            .withColumn('close',  col('close').cast(DoubleType())) \
            .withColumn('volume', col('volume').cast(LongType())) \
            .withColumn('invalid', when(
                (col('close') > col('high')) |
                (col('close') < col('low'))  |
                (col('high')  < col('low')), 1).otherwise(0)) \
            .withColumn('time_date', to_date(col('time'), 'yyyy-MM-dd')) \
            .withColumn('month', f.month(col('time_date'))) \
            .withColumn('year',  f.year(col('time_date'))) \
            .withColumn('day',   f.dayofmonth(col('time_date'))) \
            .drop('time', 'time_date')   # bỏ cột time, giữ month/year/day

        for attempt in range(1, 4):
            try:
                df_clean.coalesce(1).write.mode('overwrite').parquet(dest)
                logger.info(f'{symbol}: lưu silver -> {dest}')
                return
            except Exception as e:
                if 'SafeMode' in str(e) and attempt < 3:
                    logger.warning(f'SafeMode, chờ 30s...')
                    time.sleep(30)
                else:
                    raise

    except Exception as e:
        logger.error(f'{symbol}: lỗi transform - {e}')


def run(today: str = None):
    today = today or datetime.now().strftime('%Y-%m-%d')
    logger.info(f'=== TRANSFORM SILVER DAG [{today}] ===')

    spark = setup_spark()

    # Lấy danh sách ticker từ bronze_dag
    from vnstock import Listing
    symbols = Listing(source='VCI').symbols_by_group('VN30').tolist()

    for symbol in symbols:
        transform_one(spark, symbol, today)

    spark.stop()
    logger.info('=== HOÀN THÀNH TRANSFORM SILVER DAG ===')


if __name__ == '__main__':
    run()
