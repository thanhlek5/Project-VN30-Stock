# airflow/dags/vnstock_scheduler_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from vnstock import Listing, Vnstock
from hdfs import InsecureClient
import pandas as pd
import logging
import os
import time

logger = logging.getLogger(__name__)

# --- GIỮ NGUYÊN CẤU HÌNH TỪ FILE GỐC CỦA BẠN ---
HDFS_URL        = os.getenv('HDFS_URL', 'http://namenode:9870')
HADOOP_USER     = os.getenv('HADOOP_USER_NAME', 'root')
BRONZE_PATH     = "/user/vn30/bronze"
# Trong container: /opt/airflow/dags/ -> /opt/airflow/ (1 cấp)
# data/ và src/ đều được mount vào /opt/airflow/
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
LOCAL_DATA_PATH = os.path.join(BASE_DIR, "data", "bronze")
os.makedirs(LOCAL_DATA_PATH, exist_ok=True)

# =====================================================
# TASK 1 — Lấy danh sách VN30 (copy từ file gốc)
# =====================================================
def task_get_symbols(**context):
    logger.info("Đang lấy danh sách mã VN30...")
    try:
        listing = Listing(source='VCI')
        symbols = listing.symbols_by_group("VN30").tolist()
        logger.info(f"Tìm được {len(symbols)} mã: {symbols}")
        context["ti"].xcom_push(key="symbols", value=symbols)
        return symbols
    except Exception as e:
        logger.error(f"Lỗi lấy VN30: {e}")
        raise

# =====================================================
# TASK 2 — Chỉ lấy dữ liệu ngày MỚI (incremental)
# =====================================================
def task_incremental_ingest(**context):
    symbols    = context["ti"].xcom_pull(key="symbols", task_ids="get_vn30_symbols")
    
    # Airflow tự truyền ngày chạy vào đây
    # next_ds bị deprecated từ Airflow >= 2.4, dùng data_interval_end thay thế
    start_date = context["ds"]       # VD: "2025-04-01"
    # data_interval_end là pendulum.DateTime, gọi .strftime() trực tiếp
    data_interval_end = context.get("data_interval_end") or (context["logical_date"] + timedelta(days=1))
    end_date = data_interval_end.strftime('%Y-%m-%d')

    logger.info(f"=== Lấy dữ liệu mới: {start_date} → {end_date} ===")

    # Kết nối HDFS (giống file gốc)
    hdfs_client = None
    try:
        hdfs_client = InsecureClient(HDFS_URL, user=HADOOP_USER, root='/')
        hdfs_client.makedirs(BRONZE_PATH)
        logger.info("Kết nối HDFS thành công")
    except Exception as e:
        logger.warning(f"Không kết nối HDFS, chỉ lưu local: {e}")

    success, failed = [], []

    for i, symbol in enumerate(symbols):
        # Giữ nguyên logic sleep từ file gốc của bạn
        if i > 0 and i % 19 == 0:
            logger.info("Nghỉ 60s tránh rate limit...")
            time.sleep(60)

        try:
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            df    = stock.quote.history(
                start=start_date,
                end=end_date,
                interval='1D'
            )

            if df is not None and not df.empty:
                # APPEND vào file gốc (không ghi đè 4 năm dữ liệu cũ)
                local_file   = f"{LOCAL_DATA_PATH}/{symbol}_raw.csv"
                file_exists  = os.path.exists(local_file)
                df.to_csv(local_file, mode='a', header=not file_exists, index=False)
                logger.info(f"✅ {symbol}: append {len(df)} dòng → {local_file}")

                # Đẩy lên HDFS
                if hdfs_client:
                    hdfs_file = f"{BRONZE_PATH}/{symbol}_{start_date}.csv"
                    with hdfs_client.write(hdfs_file, encoding='utf-8', overwrite=True) as writer:
                        df.to_csv(writer, index=False)
                    logger.info(f"✅ {symbol}: HDFS OK")

                success.append(symbol)
            else:
                logger.warning(f"⚠️ {symbol}: Không có dữ liệu ngày {start_date}")

        except Exception as e:
            logger.error(f"❌ Lỗi {symbol}: {e}")
            failed.append(symbol)

    logger.info(f"Kết quả: ✅ {len(success)} OK | ❌ {len(failed)} lỗi")
    if failed:
        logger.warning(f"Mã lỗi: {failed}")

# =====================================================
# TASK 3 — Kiểm tra dữ liệu sau khi lấy
# =====================================================
def task_quality_check(**context):
    symbols = context["ti"].xcom_pull(key="symbols", task_ids="get_vn30_symbols")
    ds      = context["ds"]
    missing = []

    for symbol in symbols:
        path = f"{LOCAL_DATA_PATH}/{symbol}_raw.csv"
        if not os.path.exists(path):
            missing.append(symbol)

    if missing:
        logger.warning(f"⚠️ Thiếu file: {missing}")
    else:
        logger.info(f"✅ Quality check OK — {len(symbols)} mã đủ dữ liệu ngày {ds}")

# =====================================================
# DAG — Chạy tự động hàng ngày lúc 17h
# =====================================================
with DAG(
    dag_id="vnstock_bronze_incremental",
    description="Append dữ liệu VN30 mới mỗi ngày vào bronze layer",
    schedule="0 0 * * 1-7",             # 17h, thứ 2 đến thứ 6
    start_date=datetime(2025, 1, 1),
    catchup=False,                        # Không chạy bù ngày cũ
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        # execution_timeout đặt ở từng task, không đặt trong default_args
    },
    tags=["vnstock", "bronze", "incremental"],
) as dag:

    get_symbols = PythonOperator(
        task_id="get_vn30_symbols",
        python_callable=task_get_symbols,
    )

    ingest = PythonOperator(
        task_id="incremental_ingest",
        python_callable=task_incremental_ingest,
        execution_timeout=timedelta(hours=2),  # Task này có thể chạy lâu do kéo nhiều mã
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
    )

    # Thứ tự chạy
    get_symbols >> ingest >> quality