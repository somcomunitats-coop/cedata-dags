from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime

from geographycmplace import geographycmplace


def geography_cm_place():
    geographycmplace.calc_geography_cm_place()



with DAG("DAG_geography_cm_place", start_date=datetime(2021, 1, 1), schedule_interval="30 1 * * *", max_active_runs=1
        , catchup=False) as dag:
    carregadata = PythonOperator(
        task_id="geography_cm_place",
        python_callable=geography_cm_place,
        email_on_failure=True,
        email=Variable.get("mail_zulip"),
        dag=dag
    )

    carregadata

