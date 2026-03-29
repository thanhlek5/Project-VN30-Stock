-- Tạo bảng lưu trữ dữ liệu VN30 đã qua xử lý (Gold Layer)
CREATE TABLE IF NOT EXISTS vn30_gold (
    ticker VARCHAR(10),
    time DATE,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT,
    ma20 FLOAT, -- Đường trung bình 20 ngày
    ma50 FLOAT, -- Đường trung bình 50 ngày
    rsi FLOAT,  -- Chỉ số sức mạnh tương đối
    PRIMARY KEY (ticker, time)
);

-- Cấp quyền cho user admin
GRANT ALL PRIVILEGES ON TABLE vn30_gold TO admin;