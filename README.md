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
4. Serving Layer: Flask API truy xuất dữ liệu từ PostgreSQL (Metadata) và HDFS.
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
- symbol(**String**): Mã chứng khoán (ví dụ: VNM, VIC).
- date(**Date**): Ngày giao dịch.
- open/high/low/close(**Float**): Giá mở/cao/thấp/đóng cửa.
- volume(**Long**): Khối lượng giao dịch.

### Xử lý & Định dạng
- Định dạng: Dữ liệu được lưu dưới dạng **Parquet** để tối ưu tốc độ đọc/ghi và nén dung lượng trên HDFS.
- Phân vùng (Partitioning): Dữ liệu được phân vùng theo **symbol** và **year** để tăng tốc độ truy vấn.
- Đặc tính Big Data: Hệ thống có khả năng xử lý hàng triệu dòng dữ liệu lịch sử của nhiều mã chứng khoán cùng lúc nhờ cơ chế song song hóa của Spark.

































