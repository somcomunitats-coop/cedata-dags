from airflow import DAG
from airflow.operators.python import PythonOperator, BashOperator

from datetime import datetime, date


from datasources.apinergia import el_apinergia_curves

from conectabbdd import conecta


def somenergia_curve_download():
    # agafar tots els contractes (pas 1 del dag)
    contracts = el_apinergia_curves.get_contracts()
    for con in contracts:
        # per cada contracte trobar l'última data baixada
        last_date = el_apinergia_curves.get_last_date_contract(con)
        # crida a baixar dades
        el_apinergia_curves.download_curves(con, 'tg_cchval', last_date, date.today())
    # càlcul agregats (pot ser en un altre pas del dag)
    return True


def move_data_to_dwh():
    conecta.curves_raw_to_dwh()
    return True


with DAG("dag_curve_download", start_date=datetime(2022, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    somenergia_curve_download = PythonOperator(
        task_id="somenergia_curve_download",
        python_callable=somenergia_curve_download
    )

    move_data_to_dwh = PythonOperator(
        task_id="move_data_to_dwh",
        python_callable=move_data_to_dwh
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir=/home/airflow/dbt/daily',
        email_on_failure=True,
        dag=dag
    )

    somenergia_curve_download >> move_data_to_dwh >> dbt_run

