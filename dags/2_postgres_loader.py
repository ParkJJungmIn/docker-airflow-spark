from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime

# postgres-docker에서 경로로 들어가서 도커 컴포즈 업을 하면 됨
# postgres-docker provider 를 설치해야함


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}

# Define the DAG
with DAG('postgres_loader',
          description='PostgreSQL Loader Example',
          default_args=default_args,
          schedule_interval='0 0 * * *',
          catchup=False) as dag:

    # Task: Execute a SQL query using PostgresOperator
    sql_query = '''
        INSERT INTO sample_table (key, value)
        VALUES ('hello', 'world')
    '''

    postgres_task = PostgresOperator(task_id='execute_sql_query',
                                    postgres_conn_id='my_postgres_connection', # 아무 이름, 지정한 커넥션으로 가지고 오겠다.
                                    sql=sql_query,
                                    dag=dag)

# Data키가 들어오게되면 실행하는 그런 업무를 하는 녀석
    sql_sensor = SqlSensor(task_id='wait_for_condition',
                        conn_id='my_postgres_connection',
                        sql="SELECT COUNT(*) FROM sample_table WHERE key='hello'",
                        mode='poke', #  reschudle
                        poke_interval=30, # 5초로 바꾸고 -> 하면 
                        dag=dag)
    
    sql_query_confirm = '''
        INSERT INTO sample_table (key, value)
        VALUES ('sensor', 'confirmed')
    ''' # 이 쿼리를 실행함 -> 센서 컴퍼드가 실행된다ㅣ

    postgres_confirm_task = PostgresOperator(task_id='execute_sql_confirm_query',
                                    postgres_conn_id='my_postgres_connection',
                                    sql=sql_query_confirm,
                                    dag=dag)
    
postgres_task >> sql_sensor >> postgres_confirm_task