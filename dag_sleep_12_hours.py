from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


@dag(start_date=datetime(2025, 1, 1), schedule="@once", catchup=False, dag_id="sleep_43200",)
def sleep_dag():
    t1 = BashOperator(
        task_id="sleep_12_hours",
        bash_command="sleep 43200",
    )


sleep_dag()