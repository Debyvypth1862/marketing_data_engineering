from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.dbt_helpers import (
    run_dbt_external_table_refresh,
    run_dbt,
    check_failure,
    create_jira_for_failed_dbt,
    decide_trigger_reprocess
)

platform_name = "Income Access"
working_dir = "/opt/airflow/dags/dbt/income_access/income_access"
task_group_and_task_ids = []
no_of_tasks = 2
dag_id = "IncomeaccessDbtTriggered"
reprocess_dag_id = "ReprocessIncomeAccessExecuteAllOperatorAccounts"

with DAG(
        dag_id=dag_id,
        default_args={
            "owner": "airflow",
            "on_failure_callback": task_id_slack_failure_alert,
            "retries": 3,
            "retry_delay": timedelta(minutes=2),
        },
        schedule_interval=None,  # This DAG is triggered by another DAG
        start_date=datetime(2024, 5, 27),
        catchup=False,
        max_active_runs=1,
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting Income Access DBT DAG..."',
    )

    stage_1 = f"Refresh_Snowflake_Extended_Tables_Metadata_{platform_name.replace(' ', '_')}"
    stage_2 = f"Dbt_{platform_name.replace(' ', '_')}"

    with TaskGroup(
            group_id=f"{platform_name.replace(' ', '_')}_DBT", tooltip=f"DBT processing for {platform_name}"
    ) as income_access_dbt_group:
        refresh_task = PythonOperator(
            task_id=stage_1,
            python_callable=run_dbt_external_table_refresh,
            op_kwargs={'execution_date': '{{ execution_date }}', 'working_dir': working_dir, 'platform_name': platform_name},
            provide_context=True,
            trigger_rule="all_done",
        )

        dbt_task = PythonOperator(
            task_id=stage_2,
            python_callable=run_dbt,
            op_kwargs={'execution_date': '{{ execution_date }}', 'working_dir': working_dir, 'platform_name': platform_name},
            provide_context=True,
            trigger_rule="all_done",
        )

        refresh_task >> dbt_task

    for i in range(no_of_tasks):
        stage = eval(f'stage_{i+1}')
        task_group_and_task_ids.append(f'{platform_name.replace(" ", "_")}_DBT.{stage}')

    Jira_create = PythonOperator(
        task_id='Jira_create',
        python_callable=create_jira_for_failed_dbt,
        trigger_rule="all_done",
        op_kwargs={'execution_date': '{{ execution_date }}', 'dag': dag},
        provide_context=True,
        queue="kubernetes"
    )

    end = PythonOperator(
        task_id="end",
        python_callable=check_failure,
        trigger_rule="all_done",
        op_kwargs={"current_dag_id": dag_id, "task_group_and_task_ids": task_group_and_task_ids},
        provide_context=True,
        retries=0,
    )

    # Branch task to decide whether to trigger reprocess
    branch_task = BranchPythonOperator(
        task_id='decide_trigger_reprocess',
        python_callable=decide_trigger_reprocess,
        op_kwargs={"reprocess_dag_id": reprocess_dag_id},
        provide_context=True,
    )

    # Trigger reprocess DAG with context
    trigger_reprocess_dag = TriggerDagRunOperator(
        task_id="trigger_reprocess_dag",
        trigger_dag_id=reprocess_dag_id,
        wait_for_completion=False,
        trigger_rule="none_failed_min_one_success",
        conf={'triggered_by': 'dbt_dag'},
    )

    # Skip reprocess trigger (dummy task for branch)
    skip_reprocess_trigger = DummyOperator(
        task_id='skip_reprocess_trigger',
        trigger_rule='none_failed_min_one_success',
    )

    start >> income_access_dbt_group >> Jira_create >> end >> branch_task
    branch_task >> [trigger_reprocess_dag, skip_reprocess_trigger]
