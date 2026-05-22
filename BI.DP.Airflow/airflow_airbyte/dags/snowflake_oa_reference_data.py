from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airbyte.snowflake_oa_reference_data import data_transfer
from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.Utils import Utils

task_group_and_task_ids = []
dag_id="SnowflakeOAReferenceData"
with DAG(
    dag_id="SnowflakeOAReferenceData",
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval="15 * * * *",  # run every hour at 15 minutes past the hour
    start_date=datetime(2024, 5, 27),
    catchup=False,
    max_active_runs=1,
) as dag:
    start = BashOperator(task_id="start", bash_command="echo start")

    transfer_task = PythonOperator(
        task_id=f"SnowflakeOAsReferenceData",
        python_callable=data_transfer,
        do_xcom_push=False,
    )
    
    trigger_apisix_update = TriggerDagRunOperator(
        task_id="trigger_apisix_update",
        trigger_dag_id= "Api_six_update_rate_limit",
        wait_for_completion=True,
        trigger_rule="all_done",
        conf={'triggered_by': 'main_dag'},  # Mark as triggered by main DAG (first run)
    )

    trigger_apisix_sync = TriggerDagRunOperator(
        task_id="trigger_apisix_sync",
        trigger_dag_id= "Api_six_sync",
        wait_for_completion=True,
        trigger_rule="all_done",
        conf={'triggered_by': 'main_dag'},  # Mark as triggered by main DAG (first run)
    )

    trigger_add_modify_airbyte = TriggerDagRunOperator(
        task_id="AddModifyDeleteAirbyteSourcesAndConnections",
        trigger_dag_id= "AddModifyDeleteAirbyteSourcesAndConnections",
        wait_for_completion=True,
        trigger_rule="all_done",
        conf={'triggered_by': 'main_dag'},  # Mark as triggered by main DAG (first run)
    )


    task_group_and_task_ids.append('SnowflakeOAsReferenceData')
    task_group_and_task_ids.append('trigger_apisix_update')
    task_group_and_task_ids.append('trigger_apisix_sync')
    task_group_and_task_ids.append('AddModifyDeleteAirbyteSourcesAndConnections')

    end = PythonOperator(
        task_id="end",
        python_callable=Utils.update_task_id_details,
        trigger_rule="all_done",
        provide_context=True,
        op_kwargs={"current_dag_id":dag_id,"task_group_and_task_ids":task_group_and_task_ids},
        retries=3,
    )     
    start >> transfer_task >> trigger_apisix_update >> trigger_apisix_sync >> trigger_add_modify_airbyte >> end
