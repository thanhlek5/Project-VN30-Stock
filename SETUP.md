**Giới thiệu**
Vì đây là dự án Big data và có sử dụng các công cụ như Hadoop và spark. Thì đây là các công cụ lưu trữ và xử lý dữ liệu phân tán. 
Ta có thể cấu hình lại để có thể phân tán và sử  lý dữ liệu cho 4 máy. Nhưng Ta sẽ làm theo hướng đơn giản hơn là không config để  phân tán mà sẽ có 1 máy chứ toàn bộ dữ liệu và 3 mấy kia sẽ chỉ lấy 1 phần để làm và sẽ dùng docker-compose để  tạo env giống nhau để  tránh conflict về  version cho sau này. 
**tại sao lại chọn hướng này** 
Vì nếu theo hướng config cho phân tán thì sẽ phải có 1 cluster để  điều hướng cho 3 workers như thế sẽ mất đi m máy để điều phối làm giảm đi số lượng phần cấn và nếu rơi vào trường hợp cả 3 cluster để  tắt máy thì coi như là đóng băng luôn toàn bộ quy trình. 
Nên chọn cách đơn giản hơn sẽ độc lập đc cả 4 máy và không mất một máy nào làm điều phối. 


**cấu trúc thư mục**
```text
project_bigdata/
├── docker/                         # Chứa cấu hình Docker cho từng dịch vụ
│   ├── airflow/
│   │   └── dockerfile              # Cài thêm các thư viện python (vnstock, pyspark)
│   ├── spark/                      # (placeholder cho Spark custom image)
│   ├── streamlit/
│   │   └── dockerfile              # Cấu hình Streamlit UI
│   └── postgres/
│       └── init.sql                # Tạo sẵn schema/table cho Gold Layer
├── dags/                           # Nơi Airflow quét các file lập lịch (DAGs)
│   └── helpers/
│       └── __init__.py
├── src/                            # Source code chính của hệ thống
│   ├── common/                     # Các hàm dùng chung (connect HDFS, Logging)
│   │   └── __init__.py
│   ├── etl/                        # Trái tim của Big Data (Medallion)
│   │   └── __init__.py
│   ├── api/                        # Backend API
│   │   └── __init__.py
│   └── web/                        # Frontend UI (Streamlit)
│       └── app.py                  # Streamlit dashboard
├── scripts/                        # Các script bổ trợ
│   ├── setup_hdfs.sh               # Tạo các thư mục /bronze, /silver, /gold trên HDFS
│   └── wait-for-it.sh              # Script đợi DB sẵn sàng trước khi chạy app
├── config/                         # Chứa các file cấu hình hệ thống
│   ├── spark-defaults.conf
│   └── airflow.cfg
├── data/
│   ├── sample/
│   ├── raw/
│   └── process/
├── .env                            # Lưu biến môi trường (User, Pass, Port)
├── .gitignore
├── docker-compose.yml              # Kết nối tất cả các container
├── README.md
├── requirements.txt                # Danh sách thư viện Python
└── SETUP.md
```
Lưu ý hãy tạo thư mục giống cấu trúc trên để có thể chạy docker compose. 

**Các bước setup**
vào thư mục project và gõ lệnh:
```docker
docker compose up -build  
```
Sau khi đợi cho các containers được khởi tạo ta chỉ cần gõ:
```
docker compose up -d
```
Là có thể khởi đông các containers 

Nếu muốn tắt và xóa các containers ta có thể gõ:
```
docker compose down 
```
muốn mở lại thì cứ gõ lệnh `docker compose up -d` không cần phải build lại từ đầu. 