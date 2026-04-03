File này dùng để tạo dags cho dự án: 
các file chuẩn bị: 

tạo các file ở thư mục src/etl 
- crawl_bronze.py : file này dùng để lấy dữ liệu `ticker`,`time`, `open`,`high`,`low`,`close`,`volume` của nhóm VN30 của vnstock. và dữ liệu đó được lấy vào  3h30 chiều giờ Việt Nam và dữ liệu được lấy là dữ liệu của ngày hôm đó. Dữ liệu được lưu dưới dạng csv vào hdfs ở user/vn30/bronze_dag có cấu trúc: 
```
bronze_dag/
└── ACB/
    └── Year-moth-day.csv   ← toàn bộ lịch sử ACB và tên được lưu là ngày tháng năm lúc lấy dữ liệu
└── VCB/
    └──.....
└── ....
```
- transform_silver.py : file này sẽ lấy dữ liệu từ user/vn30/bronze_dag ở hdfs để xử lý và thêm bớt các cột theo trong file src/etl/silver_layer.py nhưng bỏ đi cột time vì đã có các cột ngày tháng năm. và được lưu vaò hdfs với ở thư mục /user/vn30/silver_dag có cấu trúc: 
```
silver_dag/
└── ACB/
    └── Year-moth-day.parquet   ← toàn bộ lịch sử ACB và tên được lưu là ngày tháng năm lúc xử lý dữ liệu
    └── Year-moth-day.parquet
└── VCB/
    └──.....
└── ....
```
- aggregate_gold.py : file này sẽ lấy dữ liệu từ /user/vn30/silver_dag từ hdfs để  xử lý các business requirements ở trong notebook/silver_gold.md và có thể tham khảo code xử lý ở trong src/etl/gold_layer.py . lưu vào hdfs ở /user/vn30/gold_dag có cấu trúc: 
```
gold_dag/
└── ACB/
    └── year-month-day.parquet    ← toàn bộ lịch sử ACB và tên được lưu là ngày tháng năm lúc xử lý dữ liệu
    └── Year-moth-day.parquet
└── VCB/
    └── part-00000.parquet
``` 
- pg_dag.py : file này sẽ lấy dữ liệu từ /user/vn30/gold_dag từ hdfs để đẩy dữ liệu vaò postgresql trên docker compose và lưu vào bảng vn30_gold_layer 

tạo file etl_dags.py nằm ở thư mục dags 
- file này dùng để điều phối etl theo: 
```
# Trong file DAG của Airflow
crawl_task >> transform_silver_task >> aggregate_gold_task >> pg_dag_task
```
- có thể  tham khảo code của các file `bronze_ingestion.py`,`sliver_layer.py`, `gold_layer.py`, `gold_to_pg.py` ở thư mục src/etl tuy các file lấy dữ liệu của 4 năm trước nhưng có thể tham khảo logic của chúng.  

- các dữ liệu được lấy và xử lý xuyên suốt file là dữ liệu trong ngày hôm đó.

- trước khi thực hiện hãy liệt kê các bước sẽ làm chi tiết trước để  tôi xác nhận rồi mới bắt đầu làm. 