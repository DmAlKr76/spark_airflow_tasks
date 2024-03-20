from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

from airflow.operators.python import PythonOperator

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

def logging_context(**kwargs):
    print("kwargs: ")
    # for k, v in kwargs.items():
    #     print(f"key: {k}, value: {v}")
    for k, v in kwargs.items():
        logging.info(f"key: {k}, value: {v}")    
        
    logging.info(f"Current datetime: {datetime.now()}")
    
    
dag = DAG(
    dag_id='logging_context_DAG',
    default_args=DEFAULT_ARGS,
    description='DAG for logging context',
    schedule_interval='@once',
    tags=['lesson_1', 'kruglov']
)

logging_task = PythonOperator(
    task_id='logging_context_task',
    python_callable=logging_context,
    provide_context=True,
    dag=dag,
)

logging_task