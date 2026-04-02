
import os 
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'
from functools import reduce
from pyspark.sql import SparkSession 
from pyspark.sql import functions as f 
from pyspark.sql.window import Window 
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql.functions import col, sum as spark_sum, isnan, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, StringType
import logging
from vnstock import Listing, Vnstock
from datetime import datetime  


# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
HADOOP_USER = os.getenv('HADOOP_USER_NAME', 'root')
SILVER_PATH = "/user/vn30/silver"
LOCAL_DATA_PATH = os.getenv('LOCAL_DATA_PATH', '/opt/airflow/data/raw')

# --- CẤU HÌNH THỜI GIAN ---
START_DATE = "2020-01-01"
END_DATE = datetime.now().strftime('%Y-%m-%d')


def setup_spark(): 
    """ 
    khởi tạo spark  
    """
    try:
        os.environ['HADOOP_USER_NAME'] = 'root'
        spark = SparkSession.builder.appName('silver_layer').getOrCreate()
        logger.info('Đã khởi tạo thành công spark')
        return spark 
    except Exception as e: 
        logger.error(f"Lỗi khởi tạo spark: {e} ")

def get_symbols(): 
    """
    get all symbols in vn30 group
    """
    try:
        listing = Listing(source='VCI')
        symbols_ser = listing.symbols_by_group('VN30')
        symbols = symbols_ser.tolist()
        logger.info('Lấy mã của nhóm VN30 thành công')
        return symbols
    except Exception as e:
        logger.error(f"Lỗi lấy mã: {e}")

def get_data(spark, symbols): 
    """
    dùng spark để đọc dữ liệu trên hdfs
    """
    try:    
        # tạo schema cho bảng 
        schema = StructType([
            StructField("time",StringType(),True),
            StructField("open",StringType(),True),
            StructField('high',StringType(),True),
            StructField('low',StringType(),True),
            StructField('close',StringType(),True),
            StructField("volume",StringType(),True),
            StructField('ticker',StringType(),True)
        ])
        logger.info("Tạo schema hoàn thành.")
        # nhập dữ liệu vào bảng  
        HDFS_BRONZE = 'hdfs://namenode:9000/user/vn30/bronze/'
        dfs = []
        for symbol in symbols: 
            hdfs_file = f"{symbol}_raw.csv"
            HDFS_URF = f"{HDFS_BRONZE}{hdfs_file}"
            df = spark.read.schema(schema)\
                .option("header", "true") \
                .csv(HDFS_URF)
            df = df.withColumn('ticker',when(col('ticker').isNull(),lit(symbol)).otherwise(col('ticker')))
            logger.info(f"Nạp thành công mã {symbol}")
            dfs.append(df)
        dfs_full = reduce(lambda a,b: a.union(b), dfs)
        logger.info('Gộp thành công tất cả các mã')
        return dfs_full
    except Exception as e: 
        logger.error(f"Lỗi tại {e}")
        

def transform(df): 
    try: 
        # doi kieu du lieu 
        df_clean = df \
            .withColumn("time", to_date(col("time"), "yyyy-MM-dd")) \
            .withColumn("open", col("open").cast(DoubleType())) \
            .withColumn("high", col("high").cast(DoubleType())) \
            .withColumn("low", col("low").cast(DoubleType())) \
            .withColumn("close", col("close").cast(DoubleType())) \
            .withColumn("volume", col("volume").cast(LongType()))

        df_type = df_clean.withColumn('invalid',when((col('close') > col('high'))|
                                                    (col('close') < col('low')) |
                                                    (col('high') < col('low')), 1)\
                                                    .otherwise(0))
        df_final = df_type \
            .withColumn('month', f.month(col('time'))) \
            .withColumn('year', f.year(col('time'))) \
            .withColumn('day', f.dayofmonth(col('time')))
        logger.info("Thêm và chuyển đổi kiểu dữ liệu cột hoàn thành")
        return df_final
    except Exception as e: 
        logger.error(f"Lỗi trong quá trình chuyển đổi: {e}")
        
import time

def save_to_hdfs(df, folder_name): 
    hdfs_path = f"hdfs://namenode:9000{SILVER_PATH}/{folder_name}"
    logger.info(f"Đang ghi dữ liệu vào HDFS: {hdfs_path}")

    for attempt in range(1, 4):
        try:
            df.coalesce(1).write.mode("overwrite").parquet(hdfs_path)
            logger.info("Lưu thành công Silver Layer!")
            return
        except Exception as e:
            if "SafeMode" in str(e) and attempt < 3:
                logger.warning(f"HDFS đang SafeMode, chờ 30s rồi thử lại (lần {attempt})...")
                time.sleep(30)
            else:
                logger.error(f"Lỗi khi lưu: {e}")
                return

def sliver():
    logger.info(f"=== BẮT ĐẦU QUY TRÌNH KÉO DỮ LIỆU VN30 (TỪ {START_DATE}) ===")
    spark = setup_spark()
    if not spark: return
    symbols = get_symbols()
    if not symbols: return
    df = get_data(spark, symbols)
    if not df: return
    parquet_ = transform(df)
    if not parquet_: return
    save_to_hdfs(parquet_, 'data_clean.parquet')



sliver()

