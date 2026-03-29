from vnstock import Listing, Vnstock
from hdfs import InsecureClient
import pandas as pd
from datetime import datetime
import os
import logging

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CẤU HÌNH HỆ THỐNG ---
HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
HADOOP_USER = os.getenv('HADOOP_USER_NAME', 'root')
BRONZE_PATH = "/user/vn30/bronze"
LOCAL_DATA_PATH = os.getenv('LOCAL_DATA_PATH', '/opt/airflow/data/raw')

# --- CẤU HÌNH THỜI GIAN ---
START_DATE = "2020-01-01"
END_DATE = datetime.now().strftime('%Y-%m-%d')

def get_vn30_symbols():
    """
    Lấy danh sách mã VN30 tự động.
    """
    logger.info(" Đang lấy danh sách mã từ rổ VN30...")

    try:
        listing = Listing(source='VCI')
        symbols_ser = listing.symbols_by_group("VN30")
        symbols = symbols_ser.tolist()

        logger.info(f"Đã tìm được {len(symbols)} mã VN30: {symbols}")
        return symbols

    except Exception as e:
        logger.error(f" Lỗi khi lấy danh sách VN30: {e}")
        return []

def ingest_to_bronze(symbols):
    """
    Kéo dữ liệu lịch sử và đẩy lên HDFS (Silver/Gold layers sẽ đọc từ đây).
    Nếu không kết nối được HDFS thì chỉ lưu local.
    """
    # 1. Khởi tạo HDFS Client
    hdfs_client = None
    try:
        hdfs_client = InsecureClient(HDFS_URL, user=HADOOP_USER, root='/')
        hdfs_client.makedirs(BRONZE_PATH)
        logger.info(f"Kết nối HDFS thành công: {HDFS_URL}")
    except Exception as e:
        logger.warning(f"Không thể kết nối HDFS, chỉ lưu local: {e}")
        hdfs_client = None

    # 2. Đảm bảo thư mục cục bộ tồn tại để backup
    if not os.path.exists(LOCAL_DATA_PATH):
        os.makedirs(LOCAL_DATA_PATH)

    for symbol in symbols:
        logger.info(f" Đang kéo dữ liệu: {symbol} (từ {START_DATE})")
        try:
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            df = stock.quote.history(start=START_DATE, end=END_DATE, interval='1D')
            
            if df is not None and not df.empty:
                # Lưu bản backup local
                local_file = f"{LOCAL_DATA_PATH}/{symbol}_raw.csv"
                df.to_csv(local_file, index=False)
                logger.info(f" {symbol}: Lưu local ({len(df)} dòng) -> {local_file}")

                # Đẩy lên HDFS nếu kết nối được
                if hdfs_client:
                    try:
                        hdfs_file = f"{BRONZE_PATH}/{symbol}_raw.csv"
                        with hdfs_client.write(hdfs_file, encoding='utf-8', overwrite=True) as writer:
                            df.to_csv(writer, index=False)
                        logger.info(f" {symbol}: Đẩy lên HDFS thành công")
                    except Exception as e:
                        logger.warning(f" {symbol}: Lưu HDFS thất bại (đã có local): {e}")
            else:
                logger.warning(f" {symbol}: Không có dữ liệu trả về.")
                
        except Exception as e:
            logger.error(f" Lỗi khi xử lý mã {symbol}: {str(e)}")

if __name__ == "__main__":
    logger.info(f"=== BẮT ĐẦU QUY TRÌNH KÉO DỮ LIỆU VN30 (TỪ {START_DATE}) ===")
    
    tickers = get_vn30_symbols()
    if tickers:
        ingest_to_bronze(tickers)
        logger.info("=== HOÀN THÀNH TẦNG BRONZE INGESTION ===")
    else:
        logger.error(" Không có danh sách mã để thực hiện.")