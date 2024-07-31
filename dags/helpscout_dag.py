from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime

from helpscoutproc import helpscout


def load_daily_data():
    helpscout.download_helpscout()



with DAG("DAG_helpscout", start_date=datetime(2021, 1, 1), schedule_interval="30 1 * * *", max_active_runs=1
        , catchup=False) as dag:
    carregadata = PythonOperator(
        task_id="load_daily_data",
        python_callable=load_daily_data,
        email_on_failure=True,
        email=Variable.get("mail_zulip"),
        dag=dag
    )

    carregadata

