from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello Airflow!")

# Define the DAG
with DAG('sample_dag',
          description='A simple DAG', 
          schedule_interval='0 0 * * *', 
          start_date=datetime(2023, 7, 1),  # 하루전으로 세팅하기 ,이 데그가 실행되는 시작 시점 -> 애를 미래로 해놓으면 그때까지 실행 불가능 , 과거로 되어있으면 과거로부터 지금까지의 데이터를 실행하려고 시도하려고 함-> 
          catchup=False) as dag: # 과거 데이터를 진행하지 않게다라는 설정 포인터 
        # retry를 정의할 수 있다

    # Task 1: Print "Hello Airflow!"
    task1 = PythonOperator(task_id='print_hello_task',
                        python_callable=print_hello,
                        dag=dag)

    # Task 2: Dummy task
    task2 = DummyOperator(task_id='dummy_task',
                        dag=dag)

# Define the task dependencies
task1 >> task2