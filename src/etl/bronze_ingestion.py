from vnstock import Listing, Vnstock
from hdfs import InsecureClient
import pandas as pd
from datetime import datetime
import os
import logging
import time

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CẤU HÌNH HỆ THỐNG ---
HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870').strip()
HADOOP_USER = os.getenv('HADOOP_USER_NAME', 'root')
BRONZE_PATH = "/user/vn30/bronze"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCAL_DATA_PATH = os.path.join(BASE_DIR, "data", "bronze")
os.makedirs(LOCAL_DATA_PATH, exist_ok=True)

# --- CẤU HÌNH THỜI GIAN ---
START_DATE = "2020-01-01"
END_DATE = datetime.now().strftime('%Y-%m-%d')

# Giới hạn rate-limit: nghỉ 60s sau mỗi N request
RATE_LIMIT_BATCH = 19


def get_vn30_symbols():
    """Lấy danh sách mã VN30 tự động."""
    logger.info("Đang lấy danh sách mã từ rổ VN30...")
    try:
        listing = Listing(source='VCI')
        symbols_ser = listing.symbols_by_group("VN30")
        symbols = symbols_ser.tolist()
        logger.info(f"Đã tìm được {len(symbols)} mã VN30: {symbols}")
        return symbols
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách VN30: {e}")
        return []


# ===========================================================
#  GIAI ĐOẠN 1: Kéo toàn bộ dữ liệu về local
# ===========================================================
def fetch_all_to_local(symbols: list) -> list:
    """
    Kéo dữ liệu lịch sử của từng mã từ VCI và lưu CSV vào LOCAL_DATA_PATH.

    Returns:
        fetched_files: danh sách (symbol, local_file_path) đã lưu thành công.
    """
    logger.info("=" * 60)
    logger.info("GIAI ĐOẠN 1: Kéo dữ liệu về local")
    logger.info("=" * 60)

    fetched_files = []
    count = 0

    for symbol in symbols:
        logger.info(f"  [{count + 1}/{len(symbols)}] Đang kéo: {symbol}")
        try:
            # Rate-limit: nghỉ sau mỗi RATE_LIMIT_BATCH request
            if count > 0 and count % RATE_LIMIT_BATCH == 0:
                logger.info(f"  Đã kéo {count} mã, nghỉ 60 giây để tránh rate-limit...")
                time.sleep(60)

            stock = Vnstock().stock(symbol=symbol, source='VCI')
            df = stock.quote.history(start=START_DATE, end=END_DATE, interval='1D')

            if df is not None and not df.empty:
                local_file = os.path.join(LOCAL_DATA_PATH, f"{symbol}_raw.csv")
                df.to_csv(local_file, index=False)
                fetched_files.append((symbol, local_file))
                logger.info(f"  ✓ {symbol}: {len(df)} dòng → {local_file}")
            else:
                logger.warning(f"  ✗ {symbol}: Không có dữ liệu trả về.")

            count += 1

        except Exception as e:
            logger.error(f"  ✗ Lỗi khi xử lý {symbol}: {e}")
            count += 1

    logger.info(f"\nGiai đoạn 1 hoàn tất: {len(fetched_files)}/{len(symbols)} mã đã lưu local.")
    return fetched_files


# ===========================================================
#  HELPER: Ghi DataFrame lên HDFS theo partition date
#  Output: /user/vn30/bronze/symbol={symbol}/date={date}/data.csv
# ===========================================================
def write_partitioned(hdfs_client: InsecureClient, symbol: str, df: pd.DataFrame) -> int:
    """
    Ghi dữ liệu lên HDFS theo cấu trúc partition:
        /user/vn30/bronze/symbol={symbol}/date={YYYY-MM-DD}/data.csv

    Trả về số lượng partition (ngày) đã ghi.
    """
    date_col = 'time' if 'time' in df.columns else df.columns[0]
    df = df.copy()
    df['_date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')

    written = 0
    for date, group in df.groupby('_date'):
        partition_dir = f"{BRONZE_PATH}/symbol={symbol}/date={date}"
        hdfs_path     = f"{partition_dir}/data.csv"
        group = group.drop(columns=['_date'])

        # HDFS không tự tạo thư mục cha → phải makedirs trước khi write
        hdfs_client.makedirs(partition_dir)

        with hdfs_client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
            group.to_csv(writer, index=False)
        written += 1

    return written


# ===========================================================
#  GIAI ĐOẠN 2: Push toàn bộ file local lên HDFS (partition)
# ===========================================================
def push_local_to_hdfs(fetched_files: list) -> dict:
    """
    Đẩy tất cả CSV đã lưu local lên HDFS theo cấu trúc partition:
        /user/vn30/bronze/symbol={SYMBOL}/date={YYYY-MM-DD}/data.csv

    Args:
        fetched_files: danh sách (symbol, local_file_path) từ giai đoạn 1.

    Returns:
        dict với keys 'success' và 'failed' chứa list symbol tương ứng.
    """
    logger.info("=" * 60)
    logger.info("GIAI ĐOẠN 2: Push dữ liệu lên HDFS (partition by date)")
    logger.info("=" * 60)

    result = {"success": [], "failed": []}

    if not fetched_files:
        logger.warning("Không có file nào để push, bỏ qua giai đoạn 2.")
        return result

    # --- Kết nối HDFS ---
    try:
        hdfs_client = InsecureClient(HDFS_URL, user=HADOOP_USER, root='/')
        hdfs_client.makedirs(BRONZE_PATH)
        logger.info(f"Kết nối HDFS thành công: {HDFS_URL}")
    except Exception as e:
        logger.error(f"Không thể kết nối HDFS ({HDFS_URL}): {e}")
        logger.error("Dữ liệu vẫn được lưu local. Push HDFS thất bại hoàn toàn.")
        result["failed"] = [sym for sym, _ in fetched_files]
        return result

    # --- Đẩy từng symbol theo partition ---
    for idx, (symbol, local_file) in enumerate(fetched_files, start=1):
        logger.info(f"  [{idx}/{len(fetched_files)}] Đang push: {symbol}")
        try:
            df = pd.read_csv(local_file)
            n  = write_partitioned(hdfs_client, symbol, df)
            result["success"].append(symbol)
            logger.info(f"  ✓ {symbol}: {len(df)} dòng → {n} partitions HDFS")
        except Exception as e:
            result["failed"].append(symbol)
            logger.warning(f"  ✗ {symbol}: Push HDFS thất bại (file local vẫn còn): {e}")

    # --- Báo cáo ---
    total = len(fetched_files)
    ok    = len(result["success"])
    fail  = len(result["failed"])
    logger.info(f"\nGiai đoạn 2 hoàn tất: {ok}/{total} mã push thành công, {fail} thất bại.")
    if result["failed"]:
        logger.warning(f"  Các mã thất bại: {result['failed']}")

    return result


# ===========================================================
#  ENTRY POINT
# ===========================================================
if __name__ == "__main__":
    logger.info(f"=== BẮT ĐẦU QUY TRÌNH KÉO DỮ LIỆU VN30 (TỪ {START_DATE} → {END_DATE}) ===")

    tickers = get_vn30_symbols()
    if not tickers:
        logger.error("Không có danh sách mã để thực hiện. Thoát.")
        exit(1)

    # Giai đoạn 1: kéo về local
    fetched = fetch_all_to_local(tickers)

    # Giai đoạn 2: push lên HDFS (chỉ sau khi hoàn tất giai đoạn 1)
    hdfs_result = push_local_to_hdfs(fetched)

    # Tổng kết
    logger.info("=" * 60)
    logger.info("=== HOÀN THÀNH BRONZE INGESTION ===")
    logger.info(f"  Local : {len(fetched)}/{len(tickers)} mã")
    logger.info(f"  HDFS  : {len(hdfs_result['success'])}/{len(fetched)} mã")
    logger.info("=" * 60)