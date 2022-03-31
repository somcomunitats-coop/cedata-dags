from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

from conectabbdd import conecta


def _conectafunc():
    return conecta.connecta()


with DAG("Dag_carrega_bbdd", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    conecta_bbdd_insert = BranchPythonOperator(
        task_id="conecta_bbdd_insert",
        python_callable=_conectafunc
    )

    ok = BashOperator(
        task_id="ok",
        bash_command="echo 'ok'"
    )

    ko = BashOperator(
        task_id="ko",
        bash_command="echo 'ko'"
    )

    conecta_bbdd_insert >> [ok, ko]

