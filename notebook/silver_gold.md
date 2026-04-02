Tôi đang cần business requirements cho dữ liệu vn30 để làm gold layer 
1. Requirement về "Tăng trưởng" (Price Change) 
Giúp người xem biết mã nào mạnh nhất/yếu nhất trong phiên hoặc trong tuần. Logic: So sánh giá đóng cửa hôm nay với các mốc lịch sử. Các cột cần có ở lớp Gold: price_diff_pct_1d: % thay đổi so với hôm qua. price_diff_pct_1w: % thay đổi so với 7 ngày trước. Dashboard sẽ vẽ: Bảng xếp hạng (Top Gainers/Losers) hoặc biểu đồ Bar Chart. 

2. Requirement về "Sự sôi động" (Volume Analysis) 
Giúp xác định xem sự tăng/giảm giá đó có được ủng hộ bởi dòng tiền lớn hay không. Logic: So sánh khối lượng giao dịch hôm nay với mức trung bình. Các cột cần có ở lớp Gold: volume_vs_avg_20d: Lấy (Volume hôm nay / Trung bình Volume 20 phiên trước đó). Dashboard sẽ vẽ: Biểu đồ cột màu sắc. Nếu chỉ số này > 1.5, tô màu vàng/đỏ để cảnh báo "Đột biến khối lượng". 

3. Requirement về "Vị thế kỹ thuật" (Price Position) 
Giúp trả lời câu hỏi: "Cổ phiếu này đang trong xu hướng tăng hay giảm?". Logic: So sánh giá hiện tại với đường trung bình động (MA). Các cột cần có ở lớp Gold: above_ma20 (Boolean): Giá có nằm trên đường trung bình 20 ngày không? dist_from_ma20: Giá đang cách MA20 bao nhiêu % (để biết có đang bị quá mua hay không). Dashboard sẽ vẽ: Các thẻ chỉ số (Cards) hoặc Filter để lọc nhanh các cổ phiếu đang giữ được trend tăng.

tạo file gold_layer.py ở trong thư mục project_bigdata/src/etl để thực hiệu biến đổi các requirements trên. 

và nạp dữ liệu lên hdfs theo cấu trúc chia theo ticker 
```
gold/
└── ticker=ACB/
    └── part-00000.parquet   ← toàn bộ lịch sử ACB
└── ticker=VCB/
    └── part-00000.parquet
```
và các cột ở trong silver vẫn dữ nguyên và chỉ thêm các cột mới thêm yêu cầu 