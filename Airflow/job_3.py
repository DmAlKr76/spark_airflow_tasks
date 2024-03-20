from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import random
import time


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

def generate_daily_data(**kwargs):
    time.sleep(3)
    filename = f"/opt/airflow/data/kruglov/calls/{kwargs['ds']}.csv"
    rows_count = 100000
    regions = ['MSK', 'SPB', 'KHB', 'POV', 'SIB', 'SOU']
    regions_weighs = [50, 30, 10, 4, 3, 3]

    with open(filename, "w") as file:
        header = "user_id,call_region,call_time\n"
        file.write(header)

        for i in range(rows_count):
            file.write(
                f"{random.randint(1, 100)},{random.choices(regions, weights=regions_weighs)[0]},{random.randrange(1000)}\n")

    kwargs['ti'].xcom_push(key="key", value="value")
    logging.info("generate daily data successfully")
    return filename


with DAG(
         dag_id="generate_daily_data",
         start_date=datetime(2024, 1, 1),
         schedule="@daily",
         max_active_runs=2,
         concurrency=2,
         default_args=DEFAULT_ARGS,
         tags=['lesson_1', 'kruglov']) as dag:

    mkdir = BashOperator(
        task_id="mkdir",
        bash_command="mkdir -p /opt/airflow/data/kruglov/calls"
    )

    generate_daily_data = PythonOperator(
        task_id="generate_daily_data",
        python_callable=generate_daily_data,
        provide_context=True,
        do_xcom_push=True
    )

    get_daily_data_size = BashOperator(
        task_id="get_file_size",
        bash_command="wc -c /opt/airflow/data/kruglov/calls/{{ ti.xcom_pull(task_ids='generate_daily_data', key='return_value') }}  | awk '{print $1}'",
        do_xcom_push=True
    )

    mkdir >> generate_daily_data >> get_daily_data_size