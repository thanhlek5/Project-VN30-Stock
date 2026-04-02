"""
vn30_daily_ingestion_dag.py
============================
Airflow DAG: kéo dữ liệu VN30 hàng ngày (1 lần / ngày)

Luồng:
  [check_market_open]
        │
        ▼
  [fetch_vn30_symbols]
        │
        ▼
  [run_bronze_ingestion]   ← gọi bronze_ingestion.run_daily()
        │
        ▼
  [verify_hdfs_partitions]
        │
        ▼
  [notify_success / notify_failure]

Schedule: mỗi ngày lúc 18:30 ICT (11:30 UTC) — sau khi sàn đóng cửa lúc 15:00 ICT
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty  import EmptyOperator
from airflow.utils.dates      import days_ago

# ─────────────────────────────────────────────
#  DEFAULT ARGS
# ─────────────────────────────────────────────
default_args = {
    "owner":            "vn30_team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  CALLABLE FUNCTIONS
# ─────────────────────────────────────────────

def _check_market_open(**context) -> str:
    """
    Phân nhánh: bỏ qua cuối tuần (thứ 7, CN) và ngày lễ.
    Trả về tên task tiếp theo để BranchPythonOperator dẫn.

    Lưu ý: execution_date trong Airflow là ngày *bắt đầu* interval,
    tức ngày hôm qua của lần chạy thực tế với schedule daily δ→ +1.
    """
    exec_date: datetime = context["data_interval_start"]
    weekday = exec_date.weekday()   # 0=Thứ 2 … 6=CN

    # VNSE nghỉ thứ 7 (5) & CN (6)
    HOLIDAYS_2025 = {
        datetime(2025, 1,  1).date(),
        datetime(2025, 1, 27).date(),
        datetime(2025, 1, 28).date(),
        datetime(2025, 1, 29).date(),
        datetime(2025, 1, 30).date(),
        datetime(2025, 1, 31).date(),
        datetime(2025, 4, 30).date(),
        datetime(2025, 5,  1).date(),
        datetime(2025, 9,  2).date(),
    }
    HOLIDAYS_2026 = {
        datetime(2026, 1,  1).date(),
        datetime(2026, 4, 30).date(),
        datetime(2026, 5,  1).date(),
        datetime(2026, 9,  2).date(),
    }
    ALL_HOLIDAYS = HOLIDAYS_2025 | HOLIDAYS_2026

    if weekday >= 5 or exec_date.date() in ALL_HOLIDAYS:
        logger.info("Ngày %s là ngày nghỉ / cuối tuần — bỏ qua.", exec_date.date())
        return "skip_weekend_holiday"

    logger.info("Ngày %s là ngày giao dịch → tiếp tục.", exec_date.date())
    return "run_bronze_ingestion"


def _run_bronze_ingestion(**context) -> dict:
    """
    Gọi bronze_ingestion.run_daily() với execution_date từ Airflow.
    execution_date = ngày dữ liệu cần lấy (data_interval_start).
    """
    import sys
    import os

    # Đảm bảo src/ nằm trong PYTHONPATH khi chạy trong container Airflow
    src_path = "/opt/airflow/src"
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    from etl.bronze_ingestion import run_daily

    exec_date: datetime = context["data_interval_start"]
    date_str = exec_date.strftime("%Y-%m-%d")

    logger.info("Bắt đầu bronze ingestion cho ngày: %s", date_str)
    result = run_daily(execution_date=date_str)

    # Push kết quả sang XCom để task sau đọc
    context["ti"].xcom_push(key="ingestion_result", value=result)
    logger.info("Ingestion hoàn tất: %s", result)
    return result


def _verify_hdfs_partitions(**context) -> None:
    """
    Kiểm tra sơ bộ HDFS: xác nhận partitions vừa ghi có tồn tại.
    """
    from hdfs import InsecureClient
    import os

    result: dict = context["ti"].xcom_pull(
        task_ids="run_bronze_ingestion", key="ingestion_result"
    ) or {}

    date_str = result.get("date", "")
    success  = result.get("success", [])

    if not date_str or not success:
        logger.warning("Không có partition để verify.")
        return

    hdfs_url  = os.getenv("HDFS_URL", "http://namenode:9870").strip()
    hadoop_usr = os.getenv("HADOOP_USER_NAME", "root")
    bronze_root = "/user/vn30/bronze/stock_history"

    try:
        client = InsecureClient(hdfs_url, user=hadoop_usr, root="/")
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        missing = []
        for symbol in success:
            hdfs_path = (
                f"{bronze_root}"
                f"/year={dt.year}"
                f"/month={dt.month:02d}"
                f"/day={dt.day:02d}"
                f"/symbol={symbol}"
                f"/data.parquet"
            )
            try:
                stat = client.status(hdfs_path, strict=False)
                if stat is None:
                    missing.append(symbol)
            except Exception:
                missing.append(symbol)

        if missing:
            logger.warning("Partition chưa có trên HDFS: %s", missing)
        else:
            logger.info(
                "✓ Tất cả %d partition đã xác nhận trên HDFS.", len(success)
            )
    except Exception as exc:
        logger.warning("Không kết nối được HDFS để verify: %s", exc)


def _notify_on_failure(context) -> None:
    logger.error(
        "DAG [%s] Task [%s] THẤT BẠI. Execution date: %s",
        context["dag"].dag_id,
        context["task_instance"].task_id,
        context["data_interval_start"],
    )
    # TODO: Thêm Slack / email notification ở đây nếu cần


# ─────────────────────────────────────────────
#  ĐỊNH NGHĨA DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="vn30_daily_bronze_ingestion",
    description="Kéo dữ liệu VN30 hàng ngày từ vnstock API rồi lưu Parquet lên HDFS Bronze",
    schedule_interval="30 11 * * 1-5",   # 18:30 ICT (UTC+7) = 11:30 UTC, Thứ 2→6
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["vn30", "bronze", "ingestion", "hdfs"],
    on_failure_callback=_notify_on_failure,
    max_active_runs=1,
    doc_md="""
## VN30 Daily Bronze Ingestion

**Mục đích**: Tự động kéo dữ liệu OHLCV cho 30 mã cổ phiếu VN30 mỗi ngày giao dịch
và lưu vào HDFS theo cấu trúc partition Hive-style.

**Cấu trúc HDFS**:
```
/user/vn30/bronze/stock_history/
  year=YYYY/month=MM/day=DD/symbol=TICKER/data.parquet
```

**Schedule**: 18:30 ICT (11:30 UTC), Thứ 2 – Thứ 6

**Nguồn dữ liệu**: vnstock / VCI
""",
) as dag:

    # 1. Kiểm tra ngày giao dịch
    check_market = BranchPythonOperator(
        task_id="check_market_open",
        python_callable=_check_market_open,
    )

    # Nhánh bỏ qua (cuối tuần / ngày lễ)
    skip = EmptyOperator(task_id="skip_weekend_holiday")

    # 2. Fetch + ghi HDFS
    ingest = PythonOperator(
        task_id="run_bronze_ingestion",
        python_callable=_run_bronze_ingestion,
    )

    # 3. Verify
    verify = PythonOperator(
        task_id="verify_hdfs_partitions",
        python_callable=_verify_hdfs_partitions,
        trigger_rule="all_success",
    )

    # 4. Done markers
    done_ok   = EmptyOperator(task_id="pipeline_success",  trigger_rule="all_success")
    done_skip = EmptyOperator(task_id="pipeline_skipped",  trigger_rule="all_success")

    # ─── Dependency Graph ───
    check_market >> [ingest, skip]
    ingest       >> verify >> done_ok
    skip         >> done_skip
