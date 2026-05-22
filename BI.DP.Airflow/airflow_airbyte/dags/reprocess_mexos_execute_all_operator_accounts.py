from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.state import State
from airflow.models import Variable
from airflow.models import DagRun

from airbyte.fetch_connection_list import (
    fetch_recovery_source_info,
    fetch_platform_id_from_platform,
)
from airbyte.replace_spl_char import replace_special_characters
from airbyte.opcentre_transactions_reprocess import update_jobs
from airbyte.opcentre_transactions_date_reprocess import data_source_item
from airbyte.opcentre_transactions_dq import dq
from airbyte.create_sources_and_connections.zero_recovery import update_recovery_sources
from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte import constants
from airbyte.Utils import Utils
from airbyte.reprocess_helpers import handle_recovery_failure, update_recovery_sources_wrapper
from airbyte.dbt_helpers import (
    check_dag_running,
    check_enable_dbt_downstream_tasks,
    should_trigger_dbt
)
import logging
logger = logging.getLogger(__name__)


def check_failure(current_dag_id,task_group_and_task_ids,**context):
    dag_run = context['dag_run']
    fail_dag_flag = Utils.update_task_id_details(current_dag_id,task_group_and_task_ids)
    if fail_dag_flag != 0:
        dag_run.set_state(State.FAILED)   
        logger.info("Upstream task failed. Marking this task as failed.")   

platform = constants.Mexos
is_reprocess=True
platform_id = fetch_platform_id_from_platform(platform)
result = fetch_recovery_source_info(platform_id)
total_task_groups = len(result)
task_group_and_task_ids = []
dag_id="ReprocessMexosExecuteAllOperatorAccounts"
batch_size = int(Variable.get("batch_size_reprocess"))

with DAG(
    dag_id="ReprocessMexosExecuteAllOperatorAccounts",
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None,  # Triggered by DBT DAG
    start_date=datetime(2024, 5, 27),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(days=1),  # Kill DAG if it runs longer than 1 day
) as dag:
    start = BashOperator(task_id="start", bash_command="echo start")

    task_groups = []
    for res in result:
        if (
            res["connection_status"] == "Enabled"
            and res["airbyte_connection_id"] != ""
            and res["validation_status"] == "Valid"
            and res["tlog_deleted"] == 0
        ):
            operator_id = res["operator_id"]
            name_of_task = replace_special_characters(
                f"S1_ Mexos {res['name']} {res['username']} {res['operator_id']}"
            )
            task_name = f"ProcessOperator{operator_id}.{name_of_task}"
            task_group_name = f"ProcessOperator{res['operator_id']}"
            stage_1 = replace_special_characters(f"S1_ Mexos {res['name']} {res['username']} {res['operator_id']}")
            stage_2 = "S2_UpdateDataExtractionMetadata"
            stage_3 = "S3_TransactionSlicing"
            stage_4 = "S4_DQ"
            no_of_tasks = 4  
            
            with TaskGroup(
                f'ProcessOperator{res["operator_id"]}',
                tooltip=f'This task group performs data processing for {res["operator_id"]} operator',
            ) as process_data:
                operator_task = PythonOperator(
                    task_id=stage_1,
                    python_callable=update_recovery_sources_wrapper,
                    on_failure_callback=handle_recovery_failure,
                    queue="kubernetes",
                    op_kwargs={"source_info": res, "operator_id": operator_id},
                    params={"operator_id": operator_id},  # Pass to callback for logging
                    provide_context=True,  # Required to pass 'ti' in context
                    trigger_rule="all_done",
                )

                update_opcentre = PythonOperator(
                    task_id=stage_2,
                    python_callable=update_jobs,
                    op_kwargs={"platform_id": platform_id, "task_name": task_name},
                    do_xcom_push=False,
                )

                transactions_dates = PythonOperator(
                    task_id=stage_3,
                    python_callable=data_source_item,
                    op_kwargs={"platform": platform, "task_name": task_name, "operator_id": operator_id},
                    do_xcom_push=False,
                )

                opcentre_dq = PythonOperator(
                    task_id=stage_4,
                    python_callable=dq,
                    queue="kubernetes",
                    op_kwargs={"platform": platform, "task_name": task_name,
                               "is_reprocess": is_reprocess, "operator_id": operator_id},
                    do_xcom_push=False,
                )

                operator_task >> update_opcentre >> transactions_dates >> opcentre_dq

            for i in range(no_of_tasks):
                stage = eval(f'stage_{i+1}')
                task_group_and_task_ids.append(f'{task_group_name}.{stage}')     

            task_groups.append(process_data)
            
    end = PythonOperator(
        task_id="end",
        python_callable=check_failure,
        trigger_rule="all_done",
        op_kwargs={"current_dag_id":dag_id,"task_group_and_task_ids":task_group_and_task_ids},
        provide_context=True,
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
        tg >> end

    # Check if downstream DBT tasks should be executed
    check_downstream_tasks = ShortCircuitOperator(
        task_id="check_enable_dbt_downstream_tasks",
        python_callable=check_enable_dbt_downstream_tasks,
        provide_context=True,
    )

    # Check if DBT triggering is globally enabled
    check_dbt_enabled = ShortCircuitOperator(
        task_id="check_dbt_enabled",
        python_callable=should_trigger_dbt,
        provide_context=True,
    )

    # Trigger DBT DAG after reprocess completes
    trigger_mexos_dbt = TriggerDagRunOperator(
        task_id="trigger_mexos_dbt",
        trigger_dag_id="MexosDbtTriggered",
        conf={'triggered_by': 'reprocess_dag'},
        wait_for_completion=False,
    )

    end >> check_downstream_tasks >> check_dbt_enabled >> trigger_mexos_dbt

