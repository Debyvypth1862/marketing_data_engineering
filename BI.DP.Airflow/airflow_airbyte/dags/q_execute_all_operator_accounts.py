from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.models import DagRun

from airbyte.airbyte_jobs import airbyte_api_connections_sync, handle_airbyte_job_failure
from airbyte.fetch_connection_list import (
    fetch_connid_oppid_by_platform,
    fetch_operator_id_from_ACCOUNT,
    fetch_platform_id_from_platform,
)
from airbyte.replace_spl_char import replace_special_characters
from airbyte.opcentre_transactions import update_jobs
from airbyte.opcentre_transactions_date import data_source_item
from airbyte.opcentre_transactions_dq import dq
from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte import constants
from airbyte.Utils import Utils
from airbyte.dbt_helpers import (
    check_dag_running,
    check_enable_dbt_downstream_tasks,
    should_trigger_dbt
)

platform = constants.Q
is_reprocess=False
platform_id = fetch_platform_id_from_platform(platform)
result = fetch_connid_oppid_by_platform(platform_id)
total_task_groups = len(result)
dag_id="QExecuteAllOperatorAccounts"
task_group_and_task_ids = []

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
    schedule_interval="21 7 * * *",
    start_date=datetime(2024, 5, 27),
    catchup=False,
    max_active_runs=1,
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=Utils.pause_recovery_dags
    )

    task_groups = []
    for res in result:
        operator_id_tuple = fetch_operator_id_from_ACCOUNT(res[0])
        task_name = f'process_operator_{operator_id_tuple[0][0]}.{replace_special_characters(f"S1_ Q {res[2]}_{res[1]}_{res[3]}")}'
        task_group_name = f"process_operator_{res[3]}"
        stage_1 = replace_special_characters(f"S1_ Q {res[2]}_{res[1]}_{res[3]}")
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
                python_callable=airbyte_api_connections_sync,
                on_failure_callback=handle_airbyte_job_failure,
                queue="kubernetes",
                executor_config={
                    "KubernetesExecutor": {
                        "request_cpu": "0.25",
                        "request_memory": "250Mi",
                        "limit_memory": "500Mi",
                    }
                },
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
                python_callable=update_jobs,
                op_kwargs={
                    "platform_id": platform_id,
                    "platform": platform,
                    "task_name": task_name,
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
                op_kwargs={"platform": platform, "task_name": task_name,
                           "is_reprocess": is_reprocess, "operator_id": operator_id_tuple[0][0]},
                queue="kubernetes",
                do_xcom_push=False,
            )

            operator_task >> update_opcentre >> transactions_dates >> opcentre_dq

        for i in range(no_of_tasks):
            stage = eval(f'stage_{i+1}')
            task_group_and_task_ids.append(f'{task_group_name}.{stage}')               

        task_groups.append(process_data)

    end_task_group = PythonOperator(
        task_id="wait_for_end_task_group",
        python_callable=Utils.waiting_end_task_group,
        trigger_rule="all_done",
    )
    
    Jira_create = PythonOperator(
        task_id = 'Jira_create',
        python_callable=Jira_creation,
        trigger_rule="all_done",
        queue="kubernetes"
    )

    end = PythonOperator(
        task_id="end",
        python_callable=Utils.unpause_dags,
        trigger_rule="all_done",
        provide_context=True,
        op_kwargs={"current_dag_id":dag_id,"DagRun":DagRun,"task_group_and_task_ids":task_group_and_task_ids},
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

    end_task_group >> Jira_create >> end

    # Check if DBT triggering is globally enabled
    check_dbt_enabled = ShortCircuitOperator(
        task_id="check_dbt_enabled",
        python_callable=should_trigger_dbt,
        provide_context=True,
    )

    # Check if DBT DAG is running before triggering
    check_dbt_running = ShortCircuitOperator(
        task_id="check_dbt_running",
        python_callable=check_dag_running,
        op_kwargs={"dag_id_to_check": "QDbtTriggered"},
        provide_context=True,
    )

    # Trigger DBT DAG after main processing completes
    trigger_q_dbt = TriggerDagRunOperator(
        task_id="trigger_q_dbt",
        trigger_dag_id="QDbtTriggered",
        conf={'triggered_by': 'main_dag'},
        wait_for_completion=False,
    )

    # Check if downstream DBT tasks should be executed
    check_downstream_tasks = ShortCircuitOperator(
        task_id="check_enable_dbt_downstream_tasks",
        python_callable=check_enable_dbt_downstream_tasks,
        provide_context=True,
    )

    end >> check_downstream_tasks >> check_dbt_enabled >> check_dbt_running >> trigger_q_dbt

