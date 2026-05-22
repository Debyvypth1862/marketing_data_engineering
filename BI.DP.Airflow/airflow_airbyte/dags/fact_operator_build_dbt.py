from datetime import datetime, timedelta
import pendulum

from airbyte import constants
from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.utilities import run_dbt_and_upload_artifact
from airbyte.Utils import Utils
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup

dag_id = "dbtFactOperatorBuild"
dbt_working_directories = {"Dbt_fact_operator": "/opt/airflow/dags/dbt/fact_operator/fact_operator"}

def check_failure(current_dag_id, task_group_and_task_ids, **context):
    # Convert task_group_and_task_ids to a list if it's a string
    if isinstance(task_group_and_task_ids, str):
        task_group_and_task_ids = [task_group_and_task_ids]

    Utils.update_task_id_details(current_dag_id, task_group_and_task_ids)
    dag_run = context["dag_run"]
    for task_instance in dag_run.get_task_instances():
        if task_instance.state == State.FAILED:
            raise Exception("One or more tasks failed.")

with DAG(
    dag_id=dag_id,
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    start_date=pendulum.datetime(2025, 7, 20, tz="America/Los_Angeles"),
    # Run every 4 hours starting at 12am PDT (7am UTC), 7 days a week
    schedule_interval="0 0,4,8,12,16,20 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting the DAG..."',
    )

    task_group_and_task_ids = []
    for group_id, working_dir in dbt_working_directories.items():
        dbt_project = group_id.split("Dbt_")[1]
        task_group_and_task_ids.extend(
            [f"{group_id}.dbt_build", f"{group_id}.compress_and_upload_artifact"]
        )

        with TaskGroup(
            group_id=group_id,
            tooltip="Group for dbt daily build for each project",
        ) as dbt_build_group:
            dbt_task = PythonOperator(
                task_id="dbt_build",
                python_callable=run_dbt_and_upload_artifact,
                op_kwargs={
                    "execution_date": "{{ execution_date }}",
                    "working_dir": working_dir,
                    "dbt_project": dbt_project,
                },
                provide_context=True,
                trigger_rule="all_done",
            )

            dbt_task

    update_dbt_status = PythonOperator(
        task_id="update_dbt_status",
        python_callable=check_failure,
        trigger_rule="all_done",
        op_kwargs={
            "current_dag_id": dag.dag_id,
            "task_group_and_task_ids": task_group_and_task_ids,
        },
        provide_context=True,
        retries=0,
    )

    start >> dbt_build_group >> update_dbt_status
