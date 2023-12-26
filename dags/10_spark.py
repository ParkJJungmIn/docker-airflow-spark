from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    'spark_submit_dag',
    default_args=default_args,
    schedule_interval=None
)

access_key = os.getenv("access_key",'test')
secret_key = os.getenv("secret_key",'test')

spark_submit_task = SparkSubmitOperator(
    task_id='spark_test',
    application='/opt/airflow/dags/test_spark.py',  # Spark 스크립트 파일 경로
    conn_id='spark',  # Airflow에 설정된 Spark 연결 ID
    total_executor_cores='2',
    executor_cores='1',
    num_executors='1',
    name='airflow-spark-test-job',
    execution_timeout=timedelta(minutes=3),
    packages='org.apache.hadoop:hadoop-aws:3.3.4',
    verbose=True,
    dag=dag,
    conf = {
        'spark.hadoop.fs.s3a.access.key' : access_key,
        'spark.hadoop.fs.s3a.secret.key' : secret_key,
    }
)

spark_submit_task