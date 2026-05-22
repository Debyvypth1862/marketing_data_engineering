import logging
import queue
import subprocess
import threading
from datetime import datetime, timedelta

import pytz
from airbyte import constants
from airbyte.airbyte_jobs import airbyte_api_connections_sync_v2, handle_airbyte_job_failure
from airbyte.fetch_connection_list import (
    fetch_connid_oppid_by_platform,
    fetch_operator_id_from_ACCOUNT,
    fetch_platform_id_from_platform,
    fetch_connid_oppid_by_platform_and_name
)
from airbyte.airbyte_jobs import trigger_airbyte_job
from airbyte.update_airbyte_job_status import update_airbyte_jobs
from airbyte.opcentre_transactions import update_jobs_v2
from airbyte.opcentre_transactions_date import data_source_item
from airbyte.opcentre_transactions_dq import dq
from airbyte.replace_spl_char import replace_special_characters
from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.utilities import run_dbt_and_upload_artifact
from airbyte.Utils import Utils
from airflow import DAG
from airflow.models import DagRun, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

platform = constants.Sweep
is_reprocess = False
platform_id = fetch_platform_id_from_platform(platform)
result = fetch_connid_oppid_by_platform(platform_id)
total_task_groups = len(result)
dag_id = "SweepExecuteAllOperatorAccounts"
task_group_and_task_ids = []
dbt_working_directory = "/opt/airflow/dags/dbt/sweep/sweep"
dbt_project = "Sweep"
dbt_artifact_bucket = "your-dbt-artifacts-bucket"

spree_platform = constants.Spree
spree_platform_id = fetch_platform_id_from_platform(spree_platform)
spree_airbyte_result = fetch_connid_oppid_by_platform_and_name(spree_platform_id, platform.lower())[0]
spree_airbyte_connection_id = spree_airbyte_result[0]
spree_sweep_operator_id = spree_airbyte_result[3]

def Jira_creation():
    # Check failed job history jobs
    Utils.job_create_jira_ticket(platform)
    # Check failed dqissue history jobs
    Utils.dqissue_create_jira_ticket(platform)

try:
    batch_size = int(Variable.get(f"{platform} batch_size"))
except:
    batch_size = int(Variable.get("batch_size"))

with DAG(
    dag_id=dag_id,
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval="0 15,21 * * *",  # 7AM,1PM PST (15 & 21 UTC)
    start_date=datetime(2024, 5, 27),
    catchup=False,
    max_active_runs=1,
    tags=["hourly"]
) as dag:
    start = PythonOperator(task_id="start", python_callable=Utils.pause_recovery_dags)

    task_groups = []
    for res in result:
        operator_id_tuple = fetch_operator_id_from_ACCOUNT(res[0])
        task_name = f"process_operator_{operator_id_tuple[0][0]}.{replace_special_characters(f'S1_ Sweep {res[2]}_{res[1]}_{res[3]}')}"
        task_group_name = f"process_operator_{res[3]}"
        stage_1 = replace_special_characters(f"S1_ Sweep {res[2]}_{res[1]}_{res[3]}")
        stage_2 = "S2_UpdateDataExtractionMetadata"
        stage_3 = "S3_TransactionSlicing"
        stage_4 = "S4_DQ"
        no_of_tasks = 4
        with TaskGroup(
            f"process_operator_{res[3]}",
            tooltip=f"This task group performs data processing for {res[3]} operator",
        ) as process_data:
            operator_task = PythonOperator(
                task_id=stage_1,
                python_callable=airbyte_api_connections_sync_v2,
                on_failure_callback=handle_airbyte_job_failure,
                queue="kubernetes",
                op_kwargs={
                    "airbyte_connections": [res[0]],
                    "platform_id": platform_id,
                    "platform": platform,
                    "task_name": task_name,
                },
                trigger_rule="all_done",
            )

            update_opcentre = PythonOperator(
                task_id=stage_2,
                python_callable=update_jobs_v2,
                op_kwargs={
                    "platform_id": platform_id,
                    "platform": platform,
                    "task_name": task_name,
                    "operator_id": res[3],
                },
                do_xcom_push=False,
            )

            transactions_dates = PythonOperator(
                task_id=stage_3,
                python_callable=data_source_item,
                on_failure_callback=handle_airbyte_job_failure,
                op_kwargs={"platform": platform, "task_name": task_name, "operator_id": operator_id_tuple[0][0]},
                do_xcom_push=False,
            )

            opcentre_dq = PythonOperator(
                task_id=stage_4,
                python_callable=dq,
                op_kwargs={
                    "platform": platform,
                    "task_name": task_name,
                    "is_reprocess": is_reprocess,
                    "operator_id": operator_id_tuple[0][0],
                },
                queue="kubernetes",
                do_xcom_push=False,
            )

            operator_task >> update_opcentre >> transactions_dates >> opcentre_dq

        for i in range(no_of_tasks):
            stage = eval(f"stage_{i + 1}")
            task_group_and_task_ids.append(f"{task_group_name}.{stage}")

        task_groups.append(process_data)

    end_task_group = PythonOperator(
        task_id="wait_for_end_task_group",
        python_callable=Utils.waiting_end_task_group,
        trigger_rule="all_done",
    )

    dbt_task = PythonOperator(
        task_id="dbt_build",
        python_callable=run_dbt_and_upload_artifact,
        op_kwargs={
            "execution_date": "{{ execution_date }}",
            "working_dir": dbt_working_directory,
            "dbt_project": dbt_project,
        },
        provide_context=True,
        trigger_rule="all_done",
    )

    spree_sync_task = PythonOperator(
        task_id="airbyte_trigger_spree_sweep_sync_to_gbq",
        python_callable=trigger_airbyte_job,
        on_failure_callback=lambda: None,
        queue="kubernetes",
        op_kwargs={
            "connection_id": spree_airbyte_connection_id,
        },
        trigger_rule="all_done",
    )

    update_airbyte_job_status = PythonOperator(
        task_id="update_spree_sweep_sync_airbyte_job_status",
        python_callable=update_airbyte_jobs,
        op_kwargs={
            "platform": platform,
            "airbyte_trigger_task_name": "airbyte_trigger_spree_sweep_sync_to_gbq",
            "operator_id": spree_sweep_operator_id, 
        },
        do_xcom_push=False,
    )

    Jira_create = PythonOperator(
        task_id="Jira_create",
        python_callable=Jira_creation,
        trigger_rule="all_done",
        queue="kubernetes",
    )

    end = PythonOperator(
        task_id="end",
        python_callable=Utils.unpause_dags,
        trigger_rule="all_done",
        provide_context=True,
        op_kwargs={
            "current_dag_id": dag_id,
            "DagRun": DagRun,
            "task_group_and_task_ids": task_group_and_task_ids,
        },
        retries=3,
    )

    for i in range(0, len(task_groups) - batch_size, batch_size):
        for j in range(batch_size):
            if i + batch_size + j < len(task_groups):
                current_task_group = task_groups[i + j]
                next_task_group = task_groups[i + batch_size + j]
                current_task_group >> next_task_group

    start_batch = task_groups[:batch_size]
    for tg in start_batch:
        start >> tg

    end_batch = task_groups[-batch_size:]
    for tg in end_batch:
        tg >> end_task_group

    end_task_group >> dbt_task >> spree_sync_task >> update_airbyte_job_status >> Jira_create >> end
