from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'joe',
    'start_date': days_ago(0),
    'email': ['joe@somemail.com']
}

dag = DAG(
    dag_id='process_web_log',
    default_args=default_args,
    description='Dag for final project',
    schedule_interval=timedelta(days=1),
)

# extract IP addresses from txt file
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='cut -d"-" -f1 accesslog.txt > extracted-data.txt',
    dag=dag,
)

# filter out specific IP address
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='grep -v "198.46.149.143" extracted-data.txt > transformed-data.txt',
    dag=dag,
)

# compress and archive file
load_data = BashOperator(
    task_id='load_data',
    bash_command='tar -zcvf weblog.tar.gz transformed-data.txt',
    dag=dag,
)

# steps to pipeline
extract_data >> transform_data >> load_data