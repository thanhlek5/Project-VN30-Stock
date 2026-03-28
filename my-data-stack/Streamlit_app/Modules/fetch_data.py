import pandas as pd
from vnstock import Vnstock
import logging
import time
from functools import wraps

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def retry(max_attempts=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(f"Retry {attempt+1}/{max_attempts} sau {delay}s: {e}")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry(max_attempts=3, delay=2)

def fetch_symbol(
    symbol: str,
    start: str,
    end: str,
    source: str = "VCI"
) -> pd.DataFrame:
    """
    Lấy dữ liệu OHLCV cho một symbol.

    Args:
        symbol : mã cổ phiếu, vd "ACB"
        start  : ngày bắt đầu, format "YYYY-MM-DD"
        end    : ngày kết thúc, format "YYYY-MM-DD"
        source : nguồn dữ liệu ("VCI", "TCBS", "SSI")

    Returns:
        DataFrame với các cột: time, open, high, low, close, volume
    """
    try:
        stock = Vnstock().stock(symbol=symbol, source=source)
        df = stock.quote.history(start=start, end=end, interval="1D")

        if df is None or df.empty:
            logger.warning(f"[{symbol}] Không có dữ liệu trong khoảng {start} → {end}")
            return pd.DataFrame()

        df["symbol"] = symbol
        logger.info(f"[{symbol}] Fetched {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"[{symbol}] Lỗi: {e}")
        return pd.DataFrame()

def fetch_all(
    symbols: list[str],
    start: str,
    end: str,
    source: str = "VCI"
) -> pd.DataFrame:
    """
    Fetch tất cả symbols và gộp thành một DataFrame.

    Returns:
        DataFrame gộp của tất cả symbols. Rỗng nếu mọi symbol đều lỗi.
    """
    frames = []

    for symbol in symbols:
        df = fetch_symbol(symbol, start=start, end=end, source=source)
        if not df.empty:
            frames.append(df)

    if not frames:
        logger.error("Không fetch được dữ liệu nào!")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Tổng: {len(combined)} rows từ {len(frames)}/{len(symbols)} symbols")
    return combined

