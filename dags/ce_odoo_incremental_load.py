from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime

from odoo import odoo_incremental


def load_incremental_data():
    odoo_incremental.load_incremental_data_pk_date()



with DAG("DAG_ce_odoo_incremental_load", start_date=datetime(2021, 1, 1), schedule_interval="30 1 * * *", max_active_runs=1
        , catchup=False) as dag:
    carregadata = PythonOperator(
        task_id="load_incremental_data",
        python_callable=load_incremental_data,
        email_on_failure=True,
        email=Variable.get("mail_zulip"),
        dag=dag
    )

    carregadata

