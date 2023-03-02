from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, date

from conectabbdd import conecta


def move_data_to_dwh():
    conecta.curves_dl_to_dwh()
    return True



with DAG("dag_data_process", start_date=datetime(2022, 1, 1), schedule_interval="0 4 * * *", catchup=False) as dag:
    move_data_to_dwh = PythonOperator(
        task_id="move_data_to_dwh",
        python_callable=move_data_to_dwh
    )

    # dbt_run = BashOperator(
    #     task_id='dbt_run',
    #     bash_command='dbt run --project-dir=/home/airflow/dbt/daily',
    #     email_on_failure=True,
    #     dag=dag
    # )

    move_data_to_dwh

