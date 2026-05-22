from datetime import timedelta

from airbyte import constants
from airbyte.fetch_connection_list import (
    fetch_connid_oppid_by_platform,
    fetch_platform_id_from_platform,
)
from airbyte.sapphirebet_scraping_utilities import processing_sapphirebet
from airbyte.slack_alerts import task_id_slack_failure_alert
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

platform = constants.Sapphirebet
platform_id = fetch_platform_id_from_platform(platform)
operators = fetch_connid_oppid_by_platform(platform_id)
dag_id = "SapphirebetExecuteAllOperatorAccounts"
task_groups = []


# Define the DAG
with DAG(
    dag_id,
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "priority_weight": 5,
    },
    description="Scraping Sapphirebet for players report",
    schedule_interval="0 8,16 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["sapphirebet"],
) as dag:
    start_task = DummyOperator(task_id="start_task", dag=dag)

    # Create tasks for each endpoint/file type
    for operator in operators:
        operator_id = operator[3]
        user_name = operator[1]
        password = operator[4]
        with TaskGroup(
            f"Sapphirebet_{user_name}_{operator_id}_sync",
        ) as process_data:
            scraping_task = PythonOperator(
                task_id="download_file",
                python_callable=processing_sapphirebet,
                provide_context=True,
                op_kwargs={
                    "user_name": user_name,
                    "password": password,
                    "operator_id": operator_id,
                    "platform_name": platform,
                },
            )

        # Set task dependencies for this file type
        scraping_task
        task_groups.append(process_data)

    end_task = DummyOperator(task_id="end_task", dag=dag)

    start_task >> task_groups >> end_task
