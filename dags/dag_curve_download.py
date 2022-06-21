from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

from datasources.apinergia import el_apinergia_curves


def somenergia_curve_download():
    el_apinergia_curves.download_curves(1234567, 'tg_cchval', '2022-06-01', '2022-06-03')
    return True





with DAG("dag_curve_download", start_date=datetime(2022, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    somenergia_curve_download = PythonOperator(
        task_id="somenergia_curve_download",
        python_callable=somenergia_curve_download
    )
