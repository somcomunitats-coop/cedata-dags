from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime

from usersorganization import usersorganization


def modify_users_organization():
    usersorganization.change_users_organization()


with DAG("DAG_users_organization", start_date=datetime(2021, 1, 1), schedule_interval=None
        , max_active_runs=1, catchup=False) as dag:

    users_organization = PythonOperator(
        task_id="usersorganization",
        python_callable=modify_users_organization,
        email_on_failure=True,
        email=Variable.get("mail_zulip"),
        dag=dag
    )

    users_organization

