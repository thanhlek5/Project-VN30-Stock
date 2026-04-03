import os
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta

# ── Config ──────────────────────────────────────────────────────────────────
st.set_page_config(page_title='VN30 Dashboard', layout='wide', page_icon='📈')

PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB   = os.getenv('POSTGRES_DB',       'vn30_db')
PG_USER = os.getenv('POSTGRES_USER',     'admin')
PG_PASS = os.getenv('POSTGRES_PASSWORD', 'admin123')

# ── DB helpers ───────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT,
                            dbname=PG_DB, user=PG_USER, password=PG_PASS)

@st.cache_data(ttl=300)
def query(sql, params=None):
    conn = get_conn()
    return pd.read_sql(sql, conn, params=params)

def load_latest():
    return query("""
        SELECT * FROM vn30_gold_layer
        WHERE time = (SELECT MAX(time) FROM vn30_gold_layer)
        ORDER BY price_diff_pct_1d DESC NULLS LAST
    """)

def load_history(tickers: list, days: int = 180):
    placeholders = ','.join(['%s'] * len(tickers))
    since = (date.today() - timedelta(days=days)).isoformat()
    return query(f"""
        SELECT time, ticker, close, volume, ma20,
               price_diff_pct_1d, volume_vs_avg_20d, dist_from_ma20,
               above_ma20
        FROM vn30_gold_layer
        WHERE ticker IN ({placeholders}) AND time >= %s
        ORDER BY ticker, time
    """, params=tickers + [since])

# ── Sidebar nav ──────────────────────────────────────────────────────────────
st.sidebar.title('📊 VN30 Dashboard')
page = st.sidebar.radio('Trang', [
    '🏠 Tổng quan',
    '🔍 Chi tiết mã',
    '⚙️ Bộ lọc kỹ thuật',
    '📊 So sánh cổ phiếu',
])

# ════════════════════════════════════════════════════════════════════════════
# PAGE 1 — TỔNG QUAN
# ════════════════════════════════════════════════════════════════════════════
if page == '🏠 Tổng quan':
    st.title('🏠 Tổng quan thị trường VN30')

    df = load_latest()
    if df.empty:
        st.warning('Chưa có dữ liệu. Hãy chạy pipeline ETL trước.')
        st.stop()

    latest_date = df['time'].max()
    st.caption(f'Dữ liệu ngày: **{latest_date}**')

    # ── Metric cards ──
    gainers = (df['price_diff_pct_1d'] > 0).sum()
    losers  = (df['price_diff_pct_1d'] < 0).sum()
    top_g   = df.iloc[0]
    top_l   = df.iloc[-1]
    vol_spike = (df['volume_vs_avg_20d'] > 1.5).sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric('📈 Mã tăng', int(gainers))
    c2.metric('📉 Mã giảm', int(losers))
    c3.metric('🏆 Top gainer', top_g['ticker'],
              f"{top_g['price_diff_pct_1d']:+.2f}%")
    c4.metric('⚠️ Volume đột biến', int(vol_spike), '>1.5x avg')

    st.divider()

    # ── Top Gainers / Losers bar chart ──
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader('🏆 Top 10 Gainers')
        top10g = df.nlargest(10, 'price_diff_pct_1d')
        fig = px.bar(top10g, x='price_diff_pct_1d', y='ticker',
                     orientation='h', color='price_diff_pct_1d',
                     color_continuous_scale='Greens',
                     labels={'price_diff_pct_1d': '% 1D', 'ticker': ''})
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader('📉 Top 10 Losers')
        top10l = df.nsmallest(10, 'price_diff_pct_1d')
        fig = px.bar(top10l, x='price_diff_pct_1d', y='ticker',
                     orientation='h', color='price_diff_pct_1d',
                     color_continuous_scale='Reds_r',
                     labels={'price_diff_pct_1d': '% 1D', 'ticker': ''})
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Full table ──
    st.subheader('📋 Toàn bộ VN30')
    display = df[['ticker', 'close', 'price_diff_pct_1d', 'price_diff_pct_1w',
                  'volume_vs_avg_20d', 'above_ma20', 'dist_from_ma20']].copy()
    display.columns = ['Ticker', 'Giá đóng cửa', '% 1D', '% 1W',
                       'Vol/Avg20', 'Trên MA20', 'Cách MA20 (%)']

    for col in ['Giá đóng cửa', '% 1D', '% 1W', 'Vol/Avg20', 'Cách MA20 (%)']:
        display[col] = pd.to_numeric(display[col], errors='coerce')

    def color_pct(val):
        if pd.isna(val): return ''
        return 'color: green' if val > 0 else 'color: red'

    st.dataframe(
        display.style
            .applymap(color_pct, subset=['% 1D', '% 1W', 'Cách MA20 (%)'])
            .format({'Giá đóng cửa': '{:.2f}', '% 1D': '{:+.2f}%',
                     '% 1W': '{:+.2f}%', 'Vol/Avg20': '{:.2f}x',
                     'Cách MA20 (%)': '{:+.2f}%'}, na_rep='-'),
        use_container_width=True, height=600
    )

# ════════════════════════════════════════════════════════════════════════════
# PAGE 2 — CHI TIẾT MÃ
# ════════════════════════════════════════════════════════════════════════════
elif page == '🔍 Chi tiết mã':
    st.title('🔍 Chi tiết cổ phiếu')

    tickers_all = query("SELECT DISTINCT ticker FROM vn30_gold_layer ORDER BY ticker")['ticker'].tolist()
    ticker = st.sidebar.selectbox('Chọn mã', tickers_all)
    period = st.sidebar.selectbox('Khoảng thời gian', ['1M', '3M', '6M', '1Y', '3Y', '4Y'], index=2)
    days_map = {'1M': 30, '3M': 90, '6M': 180, '1Y': 365, '3Y': 1095, '4Y': 1460}

    df = load_history([ticker], days=days_map[period])
    if df.empty:
        st.warning('Không có dữ liệu.')
        st.stop()

    latest = df.iloc[-1]

    # ── Cards ──
    c1, c2, c3, c4 = st.columns(4)
    c1.metric('Giá đóng cửa', f"{latest['close']:.2f}")
    c2.metric('% 1 ngày', f"{latest['price_diff_pct_1d']:+.2f}%")
    c3.metric('Trên MA20', '✅' if latest['above_ma20'] else '❌')
    c4.metric('Cách MA20', f"{latest['dist_from_ma20']:+.2f}%")

    st.divider()

    # ── Price + MA20 line chart ──
    st.subheader(f'📈 Giá & MA20 — {ticker}')
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['time'], y=df['close'],
                             name='Close', line=dict(color='#1f77b4', width=2)))
    fig.add_trace(go.Scatter(x=df['time'], y=df['ma20'],
                             name='MA20', line=dict(color='orange', width=1.5, dash='dash')))
    fig.update_layout(height=400, hovermode='x unified')
    st.plotly_chart(fig, use_container_width=True)

    # ── Volume bar chart ──
    st.subheader('📊 Volume')
    df['vol_color'] = df['volume_vs_avg_20d'].apply(
        lambda x: '#e74c3c' if x > 1.5 else '#3498db')
    fig2 = go.Figure(go.Bar(x=df['time'], y=df['volume'],
                            marker_color=df['vol_color'], name='Volume'))
    fig2.update_layout(height=250)
    st.plotly_chart(fig2, use_container_width=True)
    st.caption('🔴 Đỏ = volume đột biến (>1.5x avg 20 phiên)')

# ════════════════════════════════════════════════════════════════════════════
# PAGE 3 — BỘ LỌC KỸ THUẬT
# ════════════════════════════════════════════════════════════════════════════
elif page == '⚙️ Bộ lọc kỹ thuật':
    st.title('⚙️ Bộ lọc kỹ thuật')

    df = load_latest()
    if df.empty:
        st.warning('Chưa có dữ liệu.')
        st.stop()

    st.sidebar.subheader('Bộ lọc')
    only_above_ma = st.sidebar.checkbox('Chỉ mã trên MA20', value=False)
    min_1d  = st.sidebar.slider('% 1D tối thiểu', -10.0, 10.0, -10.0, 0.5)
    min_vol = st.sidebar.slider('Vol/Avg20 tối thiểu', 0.0, 5.0, 0.0, 0.1)

    filtered = df.copy()
    if only_above_ma:
        filtered = filtered[filtered['above_ma20'] == True]
    filtered = filtered[filtered['price_diff_pct_1d'] >= min_1d]
    filtered = filtered[filtered['volume_vs_avg_20d'] >= min_vol]

    st.write(f'**{len(filtered)} mã** thỏa điều kiện')

    # ── Scatter chart ──
    st.subheader('Scatter: % 1D vs Cách MA20')
    fig = px.scatter(
        filtered, x='dist_from_ma20', y='price_diff_pct_1d',
        size='volume_vs_avg_20d', color='price_diff_pct_1d',
        text='ticker', hover_data=['close', 'volume_vs_avg_20d'],
        color_continuous_scale='RdYlGn',
        labels={'dist_from_ma20': 'Cách MA20 (%)', 'price_diff_pct_1d': '% 1D'},
        size_max=40
    )
    fig.add_hline(y=0, line_dash='dash', line_color='gray')
    fig.add_vline(x=0, line_dash='dash', line_color='gray')
    fig.update_traces(textposition='top center')
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        filtered[['ticker', 'close', 'price_diff_pct_1d', 'price_diff_pct_1w',
                  'volume_vs_avg_20d', 'above_ma20', 'dist_from_ma20']],
        use_container_width=True
    )

# ════════════════════════════════════════════════════════════════════════════
# PAGE 4 — SO SÁNH CỔ PHIẾU
# ════════════════════════════════════════════════════════════════════════════
elif page == '📊 So sánh cổ phiếu':
    st.title('📊 So sánh cổ phiếu')

    tickers_all = query("SELECT DISTINCT ticker FROM vn30_gold_layer ORDER BY ticker")['ticker'].tolist()
    selected = st.sidebar.multiselect('Chọn mã để so sánh (tối đa 5)',
                                      tickers_all, default=tickers_all[:3], max_selections=5)
    period   = st.sidebar.selectbox('Khoảng thời gian', ['1M', '3M', '6M', '1Y', '3Y', '4Y'], index=2)
    metric   = st.sidebar.selectbox('Chỉ số so sánh',
                                    ['close', 'price_diff_pct_1d', 'volume_vs_avg_20d', 'dist_from_ma20'])
    days_map = {'1M': 30, '3M': 90, '6M': 180, '1Y': 365, '3Y': 1095, '4Y': 1460}

    if not selected:
        st.info('Chọn ít nhất 1 mã.')
        st.stop()

    df = load_history(selected, days=days_map[period])
    if df.empty:
        st.warning('Không có dữ liệu.')
        st.stop()

    # ── Normalize close về 100 để so sánh tương đối ──
    if metric == 'close':
        st.subheader('📈 Giá chuẩn hóa (base = 100)')
        pivot = df.pivot(index='time', columns='ticker', values='close')
        pivot_norm = pivot.div(pivot.iloc[0]) * 100
        fig = px.line(pivot_norm, labels={'value': 'Chỉ số (base=100)', 'time': ''})
        fig.update_layout(height=450, hovermode='x unified')
        st.plotly_chart(fig, use_container_width=True)
    else:
        label_map = {
            'price_diff_pct_1d': '% thay đổi 1 ngày',
            'volume_vs_avg_20d': 'Volume / Avg 20D',
            'dist_from_ma20':    'Khoảng cách MA20 (%)',
        }
        st.subheader(f'📈 So sánh: {label_map[metric]}')
        fig = px.line(df, x='time', y=metric, color='ticker',
                      labels={metric: label_map[metric], 'time': ''},
                      height=450)
        fig.update_layout(hovermode='x unified')
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Bảng so sánh snapshot ngày mới nhất ──
    st.subheader('📋 Snapshot ngày mới nhất')
    latest_df = load_latest()
    snapshot = latest_df[latest_df['ticker'].isin(selected)][
        ['ticker', 'close', 'price_diff_pct_1d', 'price_diff_pct_1w',
         'volume_vs_avg_20d', 'above_ma20', 'dist_from_ma20']
    ].set_index('ticker')
    snapshot.columns = ['Giá', '% 1D', '% 1W', 'Vol/Avg20', 'Trên MA20', 'Cách MA20 (%)']

    # Fill NaN để tránh lỗi format NoneType
    for col in ['Giá', '% 1D', '% 1W', 'Vol/Avg20', 'Cách MA20 (%)']:
        snapshot[col] = pd.to_numeric(snapshot[col], errors='coerce')

    st.dataframe(snapshot.style.format({
        'Giá': '{:.2f}', '% 1D': '{:+.2f}%', '% 1W': '{:+.2f}%',
        'Vol/Avg20': '{:.2f}x', 'Cách MA20 (%)': '{:+.2f}%'
    }, na_rep='-'), use_container_width=True)

    # ── Bar chart so sánh % 1D ──
    st.subheader('📊 % thay đổi 1 ngày')
    snap_reset = snapshot.reset_index()
    fig2 = px.bar(snap_reset, x='ticker', y='% 1D',
                  color='% 1D', color_continuous_scale='RdYlGn',
                  text='% 1D', height=350)
    fig2.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
    fig2.update_layout(showlegend=False)
    st.plotly_chart(fig2, use_container_width=True)
