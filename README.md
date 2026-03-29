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
- Xem có có cột nào high < low không:






















