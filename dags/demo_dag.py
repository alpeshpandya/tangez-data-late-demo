from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('A-tangenz-demo-dag', default_args=default_args)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='ingest_meter_data_to_staging',
    bash_command='echo "CQL COPY command to load data from CSV"',
    dag=dag)

t2 = BashOperator(
    task_id='ingest_customer_data_to_staging',
    bash_command='echo "CQL COPY command to load data from CSV"',
    dag=dag)

t3 = BashOperator(
    task_id='transform_meter_data',
    bash_command='echo "Spark job to prepare meter data"',
    dag=dag)

t4 = BashOperator(
    task_id='transform_customer_data',
    bash_command='echo "Spark job to prepare customer data"',
    dag=dag)

t5 = BashOperator(
    task_id='join_meter_and_customer_data',
    bash_command='echo "Spark job to join meter and customer data"',
    dag=dag)


t6 = BashOperator(
    task_id='aggregate_to_datamart',
    bash_command='echo "Spark job to prepare final data mart with aggregates"',
    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6