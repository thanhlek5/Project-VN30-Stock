# main.py
from Modules.config import VN30_SYMBOLS
from Modules.fetch_data import fetch_all

if __name__ == "__main__":
    df = fetch_all(
        symbols=VN30_SYMBOLS,
        start="2024-01-01",
        end="2024-12-31"
    )

    print(df.shape)       # (N_rows, N_cols)
    print(df.dtypes)      # kiểm tra kiểu dữ liệu
    print(df.isnull().sum())  # kiểm tra missing values
    print(df.head())