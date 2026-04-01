
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
from hdfs import InsecureClient
from vnstock import Listing, Vnstock

def setup_spark(): 
    """ 
    khởi tạo spark  
    """
    try:
        spark =  SparkSession.builder.appName('bronze layer')\
        .master('local[*]')\
        .config('spark.driver.memory', '4g')\
        .getOrCreate()
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
            StructField("volumn",StringType(),True),
            StructField('ticker',StringType(),True)
        ])
        logger.info("Tạo schema hoàn thành.")
        # nhập dữ liệu vào bảng  
        HDFS_BRONZE = 'hdfs://namenode:9000/user/vn30/bronze/'
        dfs = []
        for symbol in symbols: 
            hdfs_file = f"{symbol}.csv"
            HDFS_URF = os.path.join(HDFS_BRONZE,hdfs_file)
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
        return []

def transfom(df): 
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
        df_month = df_type.withColumn('month',f.month(col('time')))
        df_year = df_month.withColumn('month',f.year(col('time')))
        df_final = df_year.withColumn('month',f.dayofmonth(col('time')))
        df_final = df_final.drop("time")
        logger.info("Thêm và chuyển đổi kiểu dữ liệu cột hoàn thành")
        return df_final
    except Exception as e: 
        logger.error(f"Lỗi trong quá trình chuyển đổi")
        return []
def save_to_hdfs(df): 
    hdfs_client = None
    try: 

def sliver(): 
    spark = setup_spark()
    symbols = get_symbols()



