"""
bronze_ingestion.py
===================
VN30 → HDFS Bronze Layer Ingestion
• Chế độ FULL : kéo toàn bộ lịch sử từ START_DATE đến hôm nay (chạy lần đầu)
• Chế độ DAILY: chỉ kéo ngày hôm qua / ngày được chỉ định   (chạy hàng ngày qua Airflow)

Cấu trúc partition trên HDFS:
  /user/vn30/bronze/stock_history/
    └── year=YYYY/
        └── month=MM/
            └── day=DD/
                └── symbol=TICKER/
                    └── data.parquet
"""

from __future__ import annotations

import io
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient
from vnstock import Listing, Vnstock

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  CẤU HÌNH HỆ THỐNG
# ─────────────────────────────────────────────
HDFS_URL        = os.getenv("HDFS_URL",            "http://namenode:9870").strip()
HADOOP_USER     = os.getenv("HADOOP_USER_NAME",    "root")
HDFS_BRONZE_ROOT = "/user/vn30/bronze/stock_history"   # Gốc Bronze trên HDFS

# Fallback local nếu HDFS unavailable
BASE_DIR        = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCAL_BRONZE    = os.path.join(BASE_DIR, "data", "bronze", "stock_history")

# Rate-limit: ngủ 60 s sau mỗi N request
RATE_LIMIT_BATCH = 19
FULL_HISTORY_START = "2020-01-01"


# ─────────────────────────────────────────────
#  HÀM TIỆN ÍCH
# ─────────────────────────────────────────────

def get_vn30_symbols() -> list[str]:
    """Trả về danh sách mã chứng khoán trong rổ VN30."""
    logger.info("Đang lấy danh sách mã VN30 từ VCI …")
    try:
        listing = Listing(source="VCI")
        symbols = listing.symbols_by_group("VN30").tolist()
        logger.info("Tìm được %d mã VN30: %s", len(symbols), symbols)
        return symbols
    except Exception as exc:
        logger.error("Lỗi khi lấy danh sách VN30: %s", exc)
        return []


def _make_hdfs_client() -> Optional[InsecureClient]:
    """Tạo HDFS client; trả None nếu kết nối thất bại."""
    try:
        client = InsecureClient(HDFS_URL, user=HADOOP_USER, root="/")
        # Kiểm tra kết nối nhẹ
        client.status("/")
        logger.info("Kết nối HDFS thành công: %s", HDFS_URL)
        return client
    except Exception as exc:
        logger.warning("Không thể kết nối HDFS (%s): %s", HDFS_URL, exc)
        return None


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Chuyển DataFrame sang bytes Parquet (PyArrow)."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def _hdfs_parquet_path(root: str, target_date: date, symbol: str) -> str:
    """Tạo đường dẫn partition Hive-style trên HDFS."""
    return (
        f"{root}"
        f"/year={target_date.year}"
        f"/month={target_date.month:02d}"
        f"/day={target_date.day:02d}"
        f"/symbol={symbol}"
        f"/data.parquet"
    )


def _local_parquet_path(root: str, target_date: date, symbol: str) -> str:
    """Tạo đường dẫn partition local tương ứng."""
    return os.path.join(
        root,
        f"year={target_date.year}",
        f"month={target_date.month:02d}",
        f"day={target_date.day:02d}",
        f"symbol={symbol}",
        "data.parquet",
    )


# ─────────────────────────────────────────────
#  FETCH DỮ LIỆU
# ─────────────────────────────────────────────

def fetch_symbol(symbol: str, start: str, end: str) -> Optional[pd.DataFrame]:
    """
    Kéo dữ liệu OHLCV cho một mã từ VCI.
    - Thêm cột 'symbol' để dễ query sau này.
    """
    try:
        stock = Vnstock().stock(symbol=symbol, source="VCI")
        df = stock.quote.history(start=start, end=end, interval="1D")
        if df is not None and not df.empty:
            df["symbol"] = symbol
            # Đảm bảo cột time là datetime
            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"])
            return df
        logger.warning("[%s] Không có dữ liệu trong khoảng %s → %s", symbol, start, end)
        return None
    except Exception as exc:
        logger.error("[%s] Lỗi fetch: %s", symbol, exc)
        return None


# ─────────────────────────────────────────────
#  WRITE – HDFS hoặc LOCAL
# ─────────────────────────────────────────────

def write_partition(
    df: pd.DataFrame,
    target_date: date,
    symbol: str,
    hdfs_client: Optional[InsecureClient],
) -> bool:
    """
    Ghi một partition (date, symbol) theo thứ tự ưu tiên:
      1. HDFS (nếu client tồn tại)
      2. Local fallback
    Trả về True nếu thành công.
    """
    parquet_bytes = _df_to_parquet_bytes(df)

    # --- HDFS ---
    if hdfs_client:
        hdfs_path = _hdfs_parquet_path(HDFS_BRONZE_ROOT, target_date, symbol)
        hdfs_dir  = os.path.dirname(hdfs_path)
        try:
            hdfs_client.makedirs(hdfs_dir)
            with hdfs_client.write(hdfs_path, overwrite=True) as writer:
                writer.write(parquet_bytes)
            logger.info("  ✓ HDFS: %s", hdfs_path)
            return True
        except Exception as exc:
            logger.warning("  ✗ Ghi HDFS thất bại (%s): %s. Fallback local …", hdfs_path, exc)

    # --- LOCAL FALLBACK ---
    local_path = _local_parquet_path(LOCAL_BRONZE, target_date, symbol)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(parquet_bytes)
    logger.info("  ✓ Local: %s", local_path)
    return True


# ─────────────────────────────────────────────
#  CHẾ ĐỘ DAILY  (Airflow gọi hàng ngày)
# ─────────────────────────────────────────────

def run_daily(execution_date: Optional[str] = None) -> dict:
    """
    Kéo dữ liệu cho MỘT ngày (mặc định: hôm qua).
    Airflow truyền execution_date theo định dạng 'YYYY-MM-DD'.

    Partition: year=YYYY/month=MM/day=DD/symbol=TICKER/data.parquet
    """
    if execution_date:
        target = datetime.strptime(execution_date, "%Y-%m-%d").date()
    else:
        target = date.today() - timedelta(days=1)

    start_str = target.strftime("%Y-%m-%d")
    end_str   = start_str

    logger.info("=" * 60)
    logger.info("DAILY INGESTION — ngày: %s", start_str)
    logger.info("=" * 60)

    symbols    = get_vn30_symbols()
    hdfs_client = _make_hdfs_client()
    result     = {"date": start_str, "success": [], "failed": [], "no_data": []}

    for idx, symbol in enumerate(symbols):
        # Rate-limit
        if idx > 0 and idx % RATE_LIMIT_BATCH == 0:
            logger.info("  Đã xử lý %d mã, nghỉ 60 s để tránh rate-limit …", idx)
            time.sleep(60)

        logger.info("[%d/%d] %s", idx + 1, len(symbols), symbol)
        df = fetch_symbol(symbol, start_str, end_str)

        if df is None or df.empty:
            result["no_data"].append(symbol)
            continue

        ok = write_partition(df, target, symbol, hdfs_client)
        (result["success"] if ok else result["failed"]).append(symbol)

    _log_summary(result, len(symbols))
    return result


# ─────────────────────────────────────────────
#  CHẾ ĐỘ FULL HISTORY  (chạy lần đầu)
# ─────────────────────────────────────────────

def run_full_history(start_date: str = FULL_HISTORY_START) -> dict:
    """
    Kéo toàn bộ lịch sử từ start_date đến hôm nay.
    Mỗi ngày trong dữ liệu được lưu thành một partition riêng.
    """
    end_date = date.today().strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("FULL HISTORY INGESTION: %s → %s", start_date, end_date)
    logger.info("=" * 60)

    symbols     = get_vn30_symbols()
    hdfs_client = _make_hdfs_client()
    result      = {"mode": "full", "success": [], "failed": [], "no_data": []}

    for idx, symbol in enumerate(symbols):
        if idx > 0 and idx % RATE_LIMIT_BATCH == 0:
            logger.info("  Đã xử lý %d mã, nghỉ 60 s …", idx)
            time.sleep(60)

        logger.info("[%d/%d] %s — kéo %s → %s", idx + 1, len(symbols), symbol, start_date, end_date)
        df = fetch_symbol(symbol, start_date, end_date)

        if df is None or df.empty:
            result["no_data"].append(symbol)
            continue

        # Gom toàn bộ dữ liệu của mã rồi phân chia ra từng ngày
        try:
            df["_date"] = pd.to_datetime(df["time"]).dt.date
            grouped = df.groupby("_date")
            for grp_date, grp_df in grouped:
                grp_df = grp_df.drop(columns=["_date"])
                write_partition(grp_df, grp_date, symbol, hdfs_client)
            result["success"].append(symbol)
        except Exception as exc:
            logger.error("[%s] Lỗi phân partition: %s", symbol, exc)
            result["failed"].append(symbol)

    _log_summary(result, len(symbols))
    return result


# ─────────────────────────────────────────────
#  INTERNAL UTILS
# ─────────────────────────────────────────────

def _log_summary(result: dict, total: int) -> None:
    ok   = len(result.get("success",  []))
    fail = len(result.get("failed",   []))
    nod  = len(result.get("no_data",  []))
    logger.info("─" * 60)
    logger.info("Tổng kết: %d/%d thành công | %d lỗi | %d không có dữ liệu", ok, total, fail, nod)
    if result.get("failed"):
        logger.warning("  Mã lỗi   : %s", result["failed"])
    if result.get("no_data"):
        logger.info("  Không data: %s", result["no_data"])
    logger.info("─" * 60)


# ─────────────────────────────────────────────
#  ENTRY POINT (chạy tay)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if mode == "full":
        run_full_history()
    elif mode == "daily":
        exec_date = sys.argv[2] if len(sys.argv) > 2 else None
        run_daily(execution_date=exec_date)
    else:
        print(f"Usage: python bronze_ingestion.py [full | daily [YYYY-MM-DD]]")
        sys.exit(1)