import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
from datetime import datetime

st.set_page_config(page_title="VN30 Stock Dashboard", page_icon="📈", layout="wide")

# ==================== CSS TOÀN CỤC + TIÊU ĐỀ ====================
st.markdown("""
<style>
/* ===== TIÊU ĐỀ SIÊU TO ===== */
.big-title {
    font-size: 72px !important;
    font-weight: 900 !important;
    color: white !important;
    margin: 0 0 20px 0 !important;
    padding: 0 !important;
    line-height: 1.2 !important;
}

/* Ẩn tiêu đề mặc định nếu bị trùng */
header[data-testid="stHeader"] {
    background: transparent;
}

/* Chữ tổng thể to hơn */
html, body, p, span, div, label {
    font-size: 18px !important;
}

/* Sidebar: tiêu đề bộ lọc */
.stMultiSelect label p, .stSelectbox label p {
    font-size: 22px !important;
    font-weight: bold !important;
}

/* Sidebar: chữ trong dropdown */
div[data-baseweb="select"] {
    font-size: 20px !important;
}
ul[data-baseweb="menu"] li {
    font-size: 20px !important;
}

/* Sidebar: thẻ tag (ACB x, BID x...) */
span[data-baseweb="tag"] {
    font-size: 20px !important;
    padding: 8px 14px !important;
}
span[data-baseweb="tag"] svg {
    height: 18px !important;
    width: 18px !important;
}

/* Tab buttons */
button[data-baseweb="tab"] p {
    font-size: 22px !important;
    font-weight: bold !important;
}

/* Sidebar header */
[data-testid="stSidebar"] h2 {
    font-size: 28px !important;
}

/* ===== BẢNG DỮ LIỆU CHỮ TO ===== */
.big-table {
    width: 100%%;
    border-collapse: collapse;
}
.big-table th {
    background-color: #1e1e2e !important;
    color: #fafafa !important;
    font-size: 22px !important;
    font-weight: bold !important;
    padding: 14px 16px !important;
    text-align: left !important;
    border-bottom: 3px solid #4a4a6a !important;
    position: sticky;
    top: 0;
    z-index: 1;
}
.big-table td {
    font-size: 20px !important;
    padding: 12px 16px !important;
    border-bottom: 1px solid #3a3a4a !important;
    color: #e0e0e0 !important;
}
.big-table tr:hover td {
    background-color: #2a2a3a !important;
}
.table-container {
    max-height: 700px;
    overflow-y: auto;
    border-radius: 8px;
    border: 1px solid #3a3a4a;
}
</style>
""", unsafe_allow_html=True)

# Tiêu đề to bằng div
st.markdown('<div class="big-title">📈 VN30 Stock Dashboard</div>', unsafe_allow_html=True)

# ==================== TÌM THƯ MỤC DỮ LIỆU ====================
def find_data_path():
    possible = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "bronze")),
        "/app/data/bronze",
        "/app/bronze",
        "../../data/bronze",
        "data/bronze",
    ]
    for p in possible:
        if os.path.isdir(p) and any(f.endswith("_raw.csv") for f in os.listdir(p)):
            return p
    return None

DATA_PATH = find_data_path()

if DATA_PATH is None:
    st.error("❌ Không tìm thấy thư mục `data/bronze` hoặc không có file CSV!")
    st.info("Hãy chạy `bronze_ingestion.py` trước để tải dữ liệu về.")
    st.stop()

csv_files = [f for f in os.listdir(DATA_PATH) if f.endswith("_raw.csv")]
available_tickers = sorted([f.replace("_raw.csv", "") for f in csv_files])

st.sidebar.write(f"📂 **Dữ liệu:** `{DATA_PATH}`")
st.sidebar.write(f"📊 **{len(csv_files)}** mã cổ phiếu")

# ==================== BỘ LỌC ====================
st.sidebar.header("🔍 Bộ lọc")
selected_tickers = st.sidebar.multiselect(
    "Chọn mã cổ phiếu",
    available_tickers,
    default=available_tickers[:3] if available_tickers else [],
)
num_years = st.sidebar.selectbox("Số năm hiển thị", [1, 2, 3, 4], index=1)

# ==================== NỘI DUNG CHÍNH ====================
tab1, tab2 = st.tabs(["📊 Biểu đồ giá", "📋 Bảng dữ liệu"])

with tab1:
    if not selected_tickers:
        st.warning("⚠️ Chọn ít nhất 1 mã cổ phiếu ở sidebar bên trái")
    else:
        fig = go.Figure()
        for ticker in selected_tickers:
            file_path = os.path.join(DATA_PATH, f"{ticker}_raw.csv")
            if not os.path.exists(file_path):
                continue
            df = pd.read_csv(file_path)
            df["time"] = pd.to_datetime(df["time"])
            cutoff = datetime.now() - pd.DateOffset(years=num_years)
            df = df[df["time"] >= cutoff]

            fig.add_trace(go.Scatter(
                x=df["time"],
                y=df["close"],
                mode="lines",
                name=ticker,
                line=dict(width=3),
            ))

        fig.update_layout(
            height=800,
            title=dict(
                text="Biểu đồ lịch sử giá đóng cửa",
                font=dict(size=36),
            ),
            template="plotly_dark",
            legend=dict(font=dict(size=24)),
            xaxis=dict(
                title=dict(text="Thời gian", font=dict(size=26), standoff=40),
                tickfont=dict(size=20),
            ),
            yaxis=dict(
                title=dict(text="Giá (nghìn VNĐ)", font=dict(size=26), standoff=40),
                tickfont=dict(size=20),
            ),
            margin=dict(l=120, r=50, t=100, b=100),
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displayModeBar": True,
                "displaylogo": False,
            },
        )

with tab2:
    if not selected_tickers:
        st.warning("⚠️ Chọn mã cổ phiếu ở sidebar")
    else:
        ticker = st.selectbox("Chọn mã xem chi tiết", selected_tickers)
        file_path = os.path.join(DATA_PATH, f"{ticker}_raw.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df["time"] = pd.to_datetime(df["time"])
            df_show = df.sort_values("time", ascending=False).head(200).copy()

            # Format số
            df_show["time"] = df_show["time"].dt.strftime("%d/%m/%Y")
            for col in ["open", "high", "low", "close"]:
                if col in df_show.columns:
                    df_show[col] = df_show[col].apply(lambda x: f"{x:,.2f}")
            if "volume" in df_show.columns:
                df_show["volume"] = df_show["volume"].apply(lambda x: f"{x:,.0f}")

            # Render bảng HTML với chữ to
            html = df_show.to_html(index=False, classes="big-table", escape=False)
            st.markdown(f'<div class="table-container">{html}</div>', unsafe_allow_html=True)