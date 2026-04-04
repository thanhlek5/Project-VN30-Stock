"""
etl_dags.py
DAG điều phối pipeline VN30 hàng ngày:
  crawl_bronze >> transform_silver >> aggregate_gold >> push_postgres
Chạy lúc 15:30 giờ Việt Nam (08:30 UTC) các ngày thứ 2 - 6.
"""
import sys
import os
sys.path.insert(0, '/opt/airflow')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

NOTIFY_EMAIL = os.getenv('NOTIFY_EMAIL', 'your_email@gmail.com')

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': [NOTIFY_EMAIL],
}

with DAG(
    dag_id='vn30_etl_pipeline',
    default_args=default_args,
    description='VN30 daily ETL: Bronze → Silver → Gold → Postgres',
    schedule_interval='30 8 * * 1-5',   # 15:30 ICT = 08:30 UTC, thứ 2-6
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['vn30', 'etl'],
) as dag:

    def task_crawl_bronze(**ctx):
        from src.etl.crawl_bronze import run
        today = ctx['ds']   # YYYY-MM-DD do Airflow cung cấp
        run(today=today)

    def task_transform_silver(**ctx):
        from src.etl.transform_silver import run
        run(today=ctx['ds'])

    def task_aggregate_gold(**ctx):
        from src.etl.aggregate_gold import run
        run(today=ctx['ds'])

    def task_push_postgres(**ctx):
        from src.etl.pg_dag import run
        run(today=ctx['ds'])

    crawl_task = PythonOperator(
        task_id='crawl_bronze',
        python_callable=task_crawl_bronze,
        provide_context=True,
    )

    notify_start = EmailOperator(
        task_id='notify_start',
        to=NOTIFY_EMAIL,
        subject='[VN30] Pipeline bắt đầu chạy - {{ ds }}',
        html_content="""
        <h3>🚀 VN30 ETL Pipeline đã bắt đầu</h3>
        <p><b>Ngày:</b> {{ ds }}</p>
        <p><b>Thời gian:</b> {{ execution_date }}</p>
        <p>Pipeline đang chạy theo lịch 15:30 ICT.</p>
        """,
    )

    notify_done = EmailOperator(
        task_id='notify_done',
        to=NOTIFY_EMAIL,
        subject='[VN30] ✅ Pipeline hoàn thành - {{ ds }}',
        html_content="""
        <h3>✅ VN30 ETL Pipeline hoàn thành</h3>
        <p><b>Ngày:</b> {{ ds }}</p>
        <p>Dữ liệu đã được cập nhật vào PostgreSQL và HDFS.</p>
        <p>Xem dashboard tại: <a href="http://localhost:8501">http://localhost:8501</a></p>
        """,
    )

    silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=task_transform_silver,
        provide_context=True,
    )

    gold_task = PythonOperator(
        task_id='aggregate_gold',
        python_callable=task_aggregate_gold,
        provide_context=True,
    )

    pg_task = PythonOperator(
        task_id='push_postgres',
        python_callable=task_push_postgres,
        provide_context=True,
    )

    notify_start >> crawl_task >> silver_task >> gold_task >> pg_task >> notify_done
