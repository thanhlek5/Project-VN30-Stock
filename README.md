# Project-VN30-Stock

## 1. Tổng quan về dự án (Over View)
dự án về Big data từ cổ phiếu của VN30 là một hệ thống thu thập, xử lý và phân tích dữ liệu tài chính từ chỉ số VN30 (thị trường chứng khoán Việt Nam) theo batch processing.

### Mục tiêu của dự án

- Xây dựng hệ thống ETL (Extract, Transform, Load) tự động hóa hoàn toàn.
- Xử lý khối lượng dữ liệu lớn với Apache Spark để tính toán các chỉ số kỹ thuật (MA, RSI, MACD).
- Lưu trữ dữ liệu tối ưu trên Data Lake (HDFS) với định dạng Parquet.
- Cung cấp API và Dashboard trực quan hóa xu hướng thị trường phục vụ việc ra quyết định đầu tư.

## 2. Sơ đồ kiến trúc (System Architecture)
Hệ thống được thiết kế theo kiến trúc Layered Architecture, đảm bảo tính mở rộng và độc lập giữa các thành phần.

1. Ingestion Layer: Thu thập dữ liệu từ "Vnstock" API qua các Python scripts.
2. Storage Layer: Lưu trữ dữ liệu thô (Raw) và dữ liệu đã xử lý (Processed) trên cụm Hadoop HDFS.
3. Processing Layer: Sử dụng Apache Spark (PySpark) để thực hiện Feature Engineering và tính toán chỉ số.
4. Serving Layer: streamlit API truy xuất dữ liệu từ PostgreSQL (Metadata) và HDFS.
5. Visualization Layer: Dashboard trực quan hóa biểu đồ nến và các chỉ báo kỹ thuật.

## 3. Công nghệ sử dụng (Technology Stack)

- Ngôn ngữ chính: Python 3.12
- Lưu trữ (Data Lake): Hadoop HDFS 3.3.4
- Xử lý dữ liệu (Big Data): Apache Spark 3.5.0 (PySpark)
- Điều phối (Orchestration): Apache Airflow 2.8.1 / Cron Jobs
- Cơ sở dữ liệu phục vụ: PostgreSQL 14 (Lưu trữ chỉ số đã xử lý)
- API & Web: Streamlit
- Containerization: Docker & Docker Compose

## 4. Mô tả dữ liệu (Data Description)

### Nguồn dữ liệu 
Dữ liệu được lấy từ thư viện vnstock, bao gồm lịch sử giá của 30 mã cổ phiếu thuộc nhóm VN30.

### Schema dữ liệu thô (Raw Schema)
- date(**Date**): Ngày giao dịch.
- open/high/low/close(**Double**): Giá mở/cao/thấp/đóng cửa.
- volume(**Long**): Khối lượng giao dịch.

### Xử lý & Định dạng
- Định dạng: Dữ liệu được lưu dưới dạng **Parquet** để tối ưu tốc độ đọc/ghi và nén dung lượng trên HDFS.
- Phân vùng (Partitioning): Dữ liệu được phân vùng theo **symbol** và **year** để tăng tốc độ truy vấn.
- Đặc tính Big Data: Hệ thống có khả năng xử lý hàng triệu dòng dữ liệu lịch sử của nhiều mã chứng khoán cùng lúc nhờ cơ chế song song hóa của Spark.

## 5. Stored layers in HDFS: 

### Medallion Architecture: 
- **Bronze layer**: Đây là dữ liệu gốc được lấy trực tiếp từ api của Vnstock. Được lưu ở định dạng csv và mỗi mã sẽ là 1 file csv khác nhau. 
- **Silver layer**: Đây sẽ là nơi chưa dữ liệu đã được làm sạch, xử lý và đã conver qua định dạng parquet. 
- **Gold layer**: Là những dữ liệu đã được làm sạch và sẵn sàng để xử lý, phân tích cho các tasks của business. 

Ba layers trên đều sẽ được lưu trữ trên hdfs của apache hadoop 

ta sẽ tạo thêm database postgresql để đẩy dữ liệu trên Gold layer về  để  tránh trường hợp chậm trong việc lấy dữ liệu trực tiếp trên hdfs. Quan trọng hơn hết là để  tạo dashbroad hơn. 

### Bronze layer: 
Đây là nơi lưu những dữ liệu gốc chưa qua xử lý và sửa đổi gì cả. 


### Silver layer:
Ở phần xử lý layer này, thì api từ vnstock đã rất chất lượng khi những dữ liệu được gọi đã gần như hoàn toàn sạch sẽ và ổn định. ở đây ta chỉ cần xử lý nhẹ ở phần liểu dữ liệu ở từng cột mà thôi. Ban đầu các côt đều là `string` nên cần được đổi thành kiểu dữ liệu phù hợp. 

**Trước khi đổi:**
```
root
 |-- time: string (nullable = true)
 |-- open: string (nullable = true)
 |-- high: string (nullable = true)
 |-- low: string (nullable = true)
 |-- close: string (nullable = true)
 |-- volume: string (nullable = true)
```
**Sau khi đổi:**
```
root
 |-- time: date (nullable = true)
 |-- open: double (nullable = true)
 |-- high: double (nullable = true)
 |-- low: double (nullable = true)
 |-- close: double (nullable = true)
 |-- volume: long (nullable = true)
```

Ngoài ra ta sẽ kiểm tra thêm độ hợp lý của dữ liệu: 

- Xem có giá nào bé hơn 0 không: 
```python
# Giá âm hoặc bằng 0
data_raw.filter("close <= 0 OR open <= 0 OR high <= 0 OR low <= 0").show()

# ouput 
+----+----+----+---+-----+------+
|time|open|high|low|close|volume|
+----+----+----+---+-----+------+
+----+----+----+---+-----+------+

```
- Xem có cột vào volume âm không: 
```python
# Volume âm
data_raw.filter("volume < 0").show()

# output
+----+----+----+---+-----+------+
|time|open|high|low|close|volume|
+----+----+----+---+-----+------+
+----+----+----+---+-----+------+
```
- Xem có có cột nào high < low và close có nằm ngoài khoảng [high,low] không :
```
# high < low
+----------+------+------+-----+------+--------+
|      time|  open|  high|  low| close|  volume|
+----------+------+------+-----+------+--------+
|2024-05-16| 99.24|100.48|98.21| 99.09| 3128143|
|2024-05-22|100.92|102.24| 99.6| 101.0| 4522916|
|2024-05-23|100.63|100.85| 98.8|100.63| 3630410|
|2024-05-24| 101.0| 101.0|95.58| 96.53|13398023|
|2024-05-28| 98.07|100.26|97.41|100.26| 4632228|
|2024-05-29|100.34|101.14|98.65| 99.31| 5844515|
|2024-05-31|  98.8|100.12|97.92| 98.51| 2639516|

# close != [high,close]
+----------+------+------+-----+------+--------+
|      time|  open|  high|  low| close|  volume|
+----------+------+------+-----+------+--------+
|2024-05-16| 99.24|100.48|98.21| 99.09| 3128143|
|2024-05-22|100.92|102.24| 99.6| 101.0| 4522916|
|2024-05-23|100.63|100.85| 98.8|100.63| 3630410|
|2024-05-24| 101.0| 101.0|95.58| 96.53|13398023|
|2024-05-28| 98.07|100.26|97.41|100.26| 4632228|
|2024-05-29|100.34|101.14|98.65| 99.31| 5844515|
|2024-05-31|  98.8|100.12|97.92| 98.51| 2639516|
|2024-06-03|  98.8| 100.7|98.58|100.19| 7782256|
|2024-08-05|102.62|104.15|99.49|100.51|10450171|
|2025-04-03| 97.88|100.35|96.85| 96.85|11536434|
|2025-04-11| 99.84|101.12|97.54|101.12|19273397|
|2025-04-14| 102.4| 102.4|97.28|101.12| 9243738|

```
ở đây ta thấy được là có dữ liệu không hợp lý. Ở đây ta có thế sử  lý bằng cách xóa đi những dòng này nhưng để không bị mất đi dữ liệu theo ngày nên ta sẽ tạo thêm 1 cột để đánh dấu những dòng dữ liệu đó 
```
+----------+-----+-----+-----+-----+-------+---+
|      time| open| high|  low|close| volume|inv|
+----------+-----+-----+-----+-----+-------+---+
|2019-09-18|20.92|20.92|20.34|20.38|2592910|  0|
|2019-09-19|20.45|20.88| 20.3|20.88|1432730|  0|
|2019-09-20|20.85|21.24|20.81|21.03|1396120|  0|
|2019-09-23|20.95|21.06|20.41|20.41|2313090|  0|
|2019-09-24|20.38|20.59|20.16| 20.3|2537600|  0|
|2019-09-25|20.34|20.74|20.27|20.74|1442410|  0|
|2019-09-26|20.85|20.95|20.67|20.85| 992640|  0|
|2019-09-27|20.81|21.24|20.74|21.14|2510520|  0|
|2019-09-30|21.42| 21.5|20.85|20.85|2199170|  0|
|2019-10-01|20.85|21.14|20.81|21.14|1644130|  0|
|2019-10-02|21.14|21.17|20.67|20.67|2439020|  0|
|2019-10-03|20.45|20.74|20.38|20.67|2804850|  0|
|2019-10-04|20.77|20.77|20.48|20.52|1271210|  0|
|2019-10-07|20.52|20.67|20.05|20.05|2371810|  0|
|2019-10-08|20.05| 20.3|19.94|20.27|1337220|  0|
|2019-10-09|20.34|20.59|20.16|20.38| 937530|  0|
|2019-10-10|20.45|20.59|20.38|20.38| 808000|  0|
|2019-10-11|20.48|20.56|20.38|20.41| 794420|  0|
|2019-10-14|20.59|21.03|20.52|20.85|2231030|  0|
|2019-10-15|20.88|20.88|20.74|20.74| 663910|  0|
+----------+-----+-----+-----+-----+-------+---+
```
Tóm Tắt: 
Ta sẽ xử lý những điều sau: 
- Đổi kiểu dữ liệu của từng cột. 
- Tạo cột `invalid` để đánh dấu những ngày dữ liệu không hợp lý 
- tách cột `time` ra thành các cột rõ hơn `month`, `year`, `day`.
### Gold layer: 
ở phần này ta cần phải xác đinh được Business Requirements là gì ? 

#### 2. Requirement: Tăng trưởng (Price Change)

**Mục tiêu**  
Giúp người dùng xác định:
- Mã cổ phiếu tăng mạnh nhất / giảm mạnh nhất
- Xu hướng ngắn hạn theo ngày và tuần

**Logic**  
So sánh giá đóng cửa hiện tại với các mốc lịch sử:
- T-1 (hôm qua)
- T-7 (7 ngày trước)

**Các cột trong Gold Layer** 

```python
price_diff_pct_1d = (close_t - close_t-1) / close_t-1

price_diff_pct_1w = (close_t - close_t-7) / close_t-7
```

**Lưu ý**
- Nếu không có dữ liệu quá khứ (ví dụ ngày đầu), giá trị sẽ là NULL

**Dashboard**
- Bảng xếp hạng Top Gainers / Losers
- Biểu đồ cột (Bar Chart) theo % thay đổi

---

#### 3. Requirement: Sự sôi động (Volume Analysis)

**Mục tiêu**  
Xác định mức độ tham gia của dòng tiền:
- Giá tăng/giảm có được hỗ trợ bởi khối lượng lớn hay không
- Phát hiện các phiên có khối lượng bất thường

**Logic**  
So sánh khối lượng giao dịch hiện tại với trung bình 20 phiên gần nhất

**Các cột trong Gold Layer**
```python 
avg_volume_20d = AVG(volume) trong 20 ngày gần nhất

volume_vs_avg_20d = volume_t / avg_volume_20d
```
#### 4. Requirement: Vị thế kỹ thuật (Price Position)

**Mục tiêu**  
Giúp xác định xu hướng hiện tại của cổ phiếu:
- Đang trong xu hướng tăng hay giảm
- Khoảng cách so với xu hướng trung bình

**Logic**  
So sánh giá đóng cửa với đường trung bình động 20 ngày (MA20)

```python 
ma20 = AVG(close) trong 20 ngày gần nhất

above_ma20 = close_t > ma20

dist_from_ma20 = (close_t - ma20) / ma20
```

**Diễn giải**
- above_ma20 = true → xu hướng tăng
- above_ma20 = false → xu hướng giảm
- dist_from_ma20 càng lớn → giá càng lệch khỏi trung bình (có thể overbought)

**Dashboard**
- Cards: tỷ lệ cổ phiếu trên MA20
- Filter: lọc các cổ phiếu đang trong xu hướng tăng

---

#### 5. Schema đề xuất cho Gold Layer

```
time
symbol
open
high
low
close
volume
invalid
month
year
day

price_diff_pct_1d
price_diff_pct_1w

volume_vs_avg_20d

ma20
above_ma20
dist_from_ma20
```

---

#### 6. Nguyên tắc thiết kế Gold Layer

- Giữ nguyên toàn bộ cột từ Silver Layer
- Chỉ bổ sung thêm các cột đã được tính toán
- Không để dashboard tự tính toán lại logic
- Dữ liệu phải sẵn sàng để sử dụng trực tiếp cho BI và visualization
- Tối ưu cho truy vấn và đọc dữ liệu nhanh (parquet, partition theo symbol)















