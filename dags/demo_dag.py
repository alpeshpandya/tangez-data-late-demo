import random
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
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
    task_id='transform_meter_data',
    bash_command='echo "Spark job to prepare meter data"',
    dag=dag)

options = ['join_meter_and_customer_data', 'flag_error_records']

branching1 = BranchPythonOperator(
    task_id='check_data_integrity',
    python_callable=lambda: random.choice(options),
    dag=dag,
)

t1 >> t2 >> branching1

t3 = BashOperator(
    task_id='join_meter_and_customer_data',
    bash_command='echo "Spark job to join meter and customer data"',
    dag=dag)

t4 = BashOperator(
    task_id='flag_error_records',
    bash_command='echo "Store error records for audit and trouble-shooting"',
    dag=dag)

branching1 >> t3
branching1 >> t4

t5 = BashOperator(
    task_id='enrich_data_set',
    bash_command='echo "Enrich data using look-up tables"',
    dag=dag)

t6 = BashOperator(
    task_id='determine_aggregates_to_run',
    bash_command='echo "Identify aggregates those can be calculated based on data"',
    dag=dag)

t3 >> t5 >> t6


t7 = BashOperator(
    task_id='consumption_15_minutes_sum',
    bash_command='echo "Aggregate consumption for last 15 minutes and update aggregates table"',
    dag=dag)

t8 = BashOperator(
    task_id='consumption_1_hour_sum',
    bash_command='echo "Aggregate consumption for last 1 hours and update aggregates table"',
    dag=dag)

t9 = BashOperator(
    task_id='consumption_hourly_by_customer_avg',
    bash_command='echo "Calculate consumption averages by customer on hourly basis"',
    dag=dag)

t10 = BashOperator(
    task_id='consumption_anomaly',
    bash_command='echo "Identify anomalies in consumption data"',
    dag=dag)

t6 >> t7
t6 >> t8
t6 >> t9
t6 >> t10

t11 = BashOperator(
    task_id='update_data_lineage_and_metadata',
    bash_command='echo "Record all changes made by ETL workflow"',
    dag=dag)

t4 >> t11
t7 >> t11
t8 >> t11
t9 >> t11
t10 >> t11

