# dags/vnstock_bronze_dag.py
#
# Phương án C — Full load lần đầu + Incremental hàng ngày
# Cấu trúc HDFS: /user/vn30/bronze/symbol={SYMBOL}/date={DATE}/data.csv

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from vnstock import Listing, Vnstock
from hdfs import InsecureClient
import pandas as pd
import logging
import os
import time

logger = logging.getLogger(__name__)

# ===========================================================
#  CẤU HÌNH
# ===========================================================
HDFS_URL        = os.getenv('HDFS_URL', 'http://namenode:9870').strip()
HADOOP_USER     = os.getenv('HADOOP_USER_NAME', 'root')
BRONZE_PATH     = "/user/vn30/bronze"
LOCAL_DATA_PATH = "/opt/airflow/data/bronze"   # mount: ./data:/opt/airflow/data
os.makedirs(LOCAL_DATA_PATH, exist_ok=True)

FULL_LOAD_START = "2020-01-01"
RATE_LIMIT_BATCH = 19   # Nghỉ 60s sau mỗi N request


# ===========================================================
#  HELPER: Kết nối HDFS
# ===========================================================
def get_hdfs_client() -> InsecureClient:
    return InsecureClient(HDFS_URL, user=HADOOP_USER, root='/')


# ===========================================================
#  HELPER: Ghi DataFrame lên HDFS theo partition date
#  Output: /user/vn30/bronze/symbol={symbol}/date={date}/data.csv
# ===========================================================
def write_partitioned(hdfs_client: InsecureClient, symbol: str, df: pd.DataFrame) -> int:
    """
    Ghi dữ liệu lên HDFS theo cấu trúc partition:
        /user/vn30/bronze/symbol={symbol}/date={YYYY-MM-DD}/data.csv

    Trả về số lượng partition đã ghi.
    """
    # Xác định cột ngày (vnstock trả về cột 'time')
    date_col = 'time' if 'time' in df.columns else df.columns[0]
    df = df.copy()
    df['_date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')

    pass


# ===========================================================
#  TASK 0 — Lấy danh sách mã VN30
# ===========================================================
def task_get_symbols(**context):
    logger.info("Đang lấy danh sách mã VN30...")
    listing = Listing(source='VCI')
    symbols = listing.symbols_by_group("VN30").tolist()
    logger.info(f"Tìm được {len(symbols)} mã: {symbols}")
    context["ti"].xcom_push(key="symbols", value=symbols)


# ===========================================================
#  TASK 1 — Kiểm tra HDFS: có cần full load không?
#           → Trả về task_id để BranchOperator route
# ===========================================================
def task_check_needs_full_load(**context):
    try:
        client = get_hdfs_client()
        entries = client.list(BRONZE_PATH)
        if entries:
            logger.info(f"HDFS đã có {len(entries)} partition(s) → bỏ qua full load")
            return "skip_full_load"
    except Exception:
        pass  # Folder chưa tồn tại hoặc lỗi kết nối → vẫn cần full load

    logger.info("HDFS chưa có dữ liệu → chạy full load")
    return "full_load"


# ===========================================================
#  TASK 2A — Full Load (chỉ chạy lần đầu)
#  Lấy toàn bộ dữ liệu từ 2020 → nay, ghi partition HDFS
# ===========================================================
def task_full_load(**context):
    symbols  = context["ti"].xcom_pull(key="symbols", task_ids="get_vn30_symbols")
    end_date = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"=== FULL LOAD: {FULL_LOAD_START} → {end_date} ({len(symbols)} mã) ===")

    client = get_hdfs_client()
    client.makedirs(BRONZE_PATH)

    success, failed = [], []

    for i, symbol in enumerate(symbols):
        if i > 0 and i % RATE_LIMIT_BATCH == 0:
            logger.info(f"Đã xử lý {i} mã — nghỉ 60s tránh rate limit...")
            time.sleep(60)

        try:
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            df    = stock.quote.history(start=FULL_LOAD_START, end=end_date, interval='1D')

            if df is not None and not df.empty:
                # 1. Lưu local (backup toàn bộ)
                local_file = os.path.join(LOCAL_DATA_PATH, f"{symbol}_raw.csv")
                df.to_csv(local_file, index=False)

                # 2. Ghi thẳng 1 file RAW duy nhất lên HDFS (nhanh gấp 100 lần)
                hdfs_file = f"{BRONZE_PATH}/{symbol}_raw.csv"
                with client.write(hdfs_file, encoding='utf-8', overwrite=True) as writer:
                    df.to_csv(writer, index=False)
                
                logger.info(f"✅ {symbol}: {len(df)} dòng → lưu HDFS thành công")
                success.append(symbol)
            else:
                logger.warning(f"⚠️  {symbol}: Không có dữ liệu")

        except Exception as e:
            logger.error(f"❌ {symbol}: {e}")
            failed.append(symbol)

    logger.info(f"Full load xong: ✅ {len(success)} | ❌ {len(failed)}")
    if failed:
        logger.warning(f"Các mã lỗi: {failed}")
        # Không raise để không block incremental sau đó


# ===========================================================
#  TASK 2B — Skip full load (dummy task)
# ===========================================================
def task_skip_full_load(**context):
    logger.info("HDFS đã có dữ liệu — bỏ qua full load, chuyển sang incremental.")


# ===========================================================
#  TASK 3 — Incremental (hàng ngày sau đóng cửa)
#  Chỉ lấy dữ liệu của ngày chạy, ghi 1 partition/mã
# ===========================================================
def task_incremental_ingest(**context):
    symbols = context["ti"].xcom_pull(key="symbols", task_ids="get_vn30_symbols")

    start_date        = context["ds"]   # VD: "2025-04-03"
    data_interval_end = context.get("data_interval_end") or (
        context["logical_date"] + timedelta(days=1)
    )
    end_date = data_interval_end.strftime('%Y-%m-%d')

    logger.info(f"=== Incremental: {start_date} → {end_date} ({len(symbols)} mã) ===")

    # Kết nối HDFS (không fatal nếu thất bại — vẫn lưu local)
    client = None
    try:
        client = get_hdfs_client()
        client.makedirs(BRONZE_PATH)
    except Exception as e:
        logger.warning(f"Không kết nối HDFS, chỉ lưu local: {e}")

    success, failed = [], []

    for i, symbol in enumerate(symbols):
        if i > 0 and i % RATE_LIMIT_BATCH == 0:
            logger.info("Nghỉ 60s tránh rate limit...")
            time.sleep(60)

        try:
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            df    = stock.quote.history(start=start_date, end=end_date, interval='1D')

            if df is not None and not df.empty:
                # 1. Append vào file local tổng hợp
                local_file  = os.path.join(LOCAL_DATA_PATH, f"{symbol}_raw.csv")
                file_exists = os.path.exists(local_file)
                df.to_csv(local_file, mode='a', header=not file_exists, index=False)

                # 2. Upload file Raw hoàn chỉnh mới nhất đè lên HDFS
                if client:
                    hdfs_file = f"{BRONZE_PATH}/{symbol}_raw.csv"
                    df_full = pd.read_csv(local_file)
                    with client.write(hdfs_file, encoding='utf-8', overwrite=True) as writer:
                        df_full.to_csv(writer, index=False)
                    logger.info(f"✅ {symbol}: Cập nhật HDFS ({len(df_full)} dòng)")
                else:
                    logger.info(f"✅ {symbol}: update local only")

                success.append(symbol)
            else:
                logger.warning(f"⚠️  {symbol}: Không có dữ liệu ngày {start_date}")

        except Exception as e:
            logger.error(f"❌ {symbol}: {e}")
            failed.append(symbol)

    logger.info(f"Incremental xong: ✅ {len(success)} | ❌ {len(failed)}")
    if failed:
        logger.warning(f"Các mã lỗi: {failed}")


# ===========================================================
#  TASK 4 — Quality Check
# ===========================================================
def task_quality_check(**context):
    symbols = context["ti"].xcom_pull(key="symbols", task_ids="get_vn30_symbols")
    ds      = context["ds"]
    missing = [s for s in symbols
               if not os.path.exists(os.path.join(LOCAL_DATA_PATH, f"{s}_raw.csv"))]

    if missing:
        logger.warning(f"⚠️  Quality check: Thiếu {len(missing)} file: {missing}")
    else:
        logger.info(f"✅ Quality check OK — {len(symbols)} mã đủ dữ liệu (ngày {ds})")


# ===========================================================
#  DAG DEFINITION
# ===========================================================
with DAG(
    dag_id="vnstock_bronze",
    description="Full load lần đầu (auto-detect) + Incremental hàng ngày → HDFS partition",
    schedule="0 10 * * 1-5",          # 17:00 ICT (UTC+7 = UTC+0 10:00), Thứ 2-6
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["vnstock", "bronze", "etl"],
) as dag:

    get_symbols = PythonOperator(
        task_id="get_vn30_symbols",
        python_callable=task_get_symbols,
    )

    check_branch = BranchPythonOperator(
        task_id="check_needs_full_load",
        python_callable=task_check_needs_full_load,
    )

    full_load = PythonOperator(
        task_id="full_load",
        python_callable=task_full_load,
        execution_timeout=timedelta(hours=6),   # Full load ~30 mã từ 2020 cần thời gian
    )

    skip_full_load = PythonOperator(
        task_id="skip_full_load",
        python_callable=task_skip_full_load,
    )

    incremental = PythonOperator(
        task_id="incremental_ingest",
        python_callable=task_incremental_ingest,
        execution_timeout=timedelta(hours=2),
        # Chạy dù full_load hay skip_full_load được chọn
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
    )

    # ── Flow ──────────────────────────────────────────────
    #  get_symbols
    #      └── check_needs_full_load
    #              ├── full_load ──────┐
    #              └── skip_full_load ─┤
    #                                  └── incremental_ingest
    #                                              └── quality_check
    get_symbols >> check_branch >> [full_load, skip_full_load] >> incremental >> quality