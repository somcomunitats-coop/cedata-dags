from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, date


from datasources.apinergia import el_apinergia_curves


def create_structures():
    el_apinergia_curves.db_init()
    return True


with DAG("dag_create_structures", start_date=datetime(2022, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    somenergia_curve_download = PythonOperator(
        task_id="create_structures",
        python_callable=create_structures
    )

    create_structures

