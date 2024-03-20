from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "Dmitry Kruglov",
    "email": "example@mail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
    'start_date': datetime(2024, 1, 1),
#    'end_date': datetime(2022, 3, 14),
}

dag = DAG(
    dag_id='create_directory_DAG',
    default_args=DEFAULT_ARGS,
    description='DAG for create directory',
    schedule_interval='@once',
    tags=['lesson_1', 'kruglov']
)

create_directory_task = BashOperator(
    task_id='create_directory_task',
    bash_command='mkdir -p /opt/airflow/data/kruglov',
    dag=dag,
)


create_directory_task