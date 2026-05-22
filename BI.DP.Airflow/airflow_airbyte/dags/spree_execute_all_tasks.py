import logging
import os
import subprocess

from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup


import pytz
from airbyte import constants
from airbyte.airbyte_jobs import trigger_airbyte_job
from airbyte.update_airbyte_job_status import update_airbyte_jobs
from airbyte.fetch_connection_list import (
    fetch_connid_oppid_by_platform,
    fetch_platform_id_from_platform,
)
from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.utilities import run_dbt_and_upload_artifact, compress_and_upload_dbt_artifact
from airbyte.Utils import Utils
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.state import State
from airbyte.db_connection import snowflake_conn

platform = constants.Spree
is_reprocess = False
platform_id = fetch_platform_id_from_platform(platform)
airbyte_conn_ids = fetch_connid_oppid_by_platform(platform_id)
dag_id = "SpreeExecuteAllTasks"

dbt_working_directory = "/opt/airflow/dags/dbt/spree/spree"
dbt_project = "Spree"
dbt_artifact_bucket = "your-dbt-artifacts-bucket"

excluded_platform = [constants.BRC, constants.Sweep, constants.Redtrack, constants.Voluum, constants.BRT]
sync_exclusion_list = [platform.lower() for platform in excluded_platform]

def check_failure(current_dag_id, task_group_and_task_ids, **context):
    # Convert task_group_and_task_ids to a list if it's a string
    if isinstance(task_group_and_task_ids, str):
        task_group_and_task_ids = [task_group_and_task_ids]

    Utils.update_task_id_details(current_dag_id, task_group_and_task_ids)
    dag_run = context["dag_run"]
    for task_instance in dag_run.get_task_instances():
        if task_instance.state == State.FAILED:
            raise Exception("One or more tasks failed.")


def check_snowflake_task_status():
    query = """
    SELECT STATE
    FROM RAW.ADS_DATA.TASK_EXECUTION_LOG
    WHERE task_name = 'RUN_CAMPAIGN_DATA_UPDATE_TASK'
      AND schema_name = 'ADS_DATA'
    ORDER BY log_timestamp DESC
    LIMIT 10
    """

    try:
        results = snowflake_conn(query)

        if results is None:
            logging.error("Failed to fetch Snowflake task status")
            return False

        if len(results) < 1:
            logging.warning("No task history found")
            return False
        latest_status = results[0][0] if results[0] else None
        logging.info(f"Latest task status: {latest_status}")
        return latest_status == 'SUCCEEDED'

    except Exception as e:
        logging.error(f"Error checking Snowflake task status: {str(e)}")
        return False


def check_traffic_source_data():
    query = """
    SELECT TRAFFIC_SOURCE, COUNT(*) as record_count
    FROM RAW.ADS_DATA.ADS_CAMPAIGN_SUMMARY
    WHERE TRAFFIC_SOURCE IN ('Google', 'Bing', 'Facebook')
      AND DATE >= DATEADD(MONTH, -3, CURRENT_DATE())
    GROUP BY TRAFFIC_SOURCE
    """

    try:
        results = snowflake_conn(query)

        if results is None:
            logging.error("Failed to fetch traffic source data")
            return False

        required_sources = {'Google', 'Bing', 'Facebook'}
        found_sources = set()

        for row in results:
            traffic_source = row[0]
            record_count = row[1]
            if record_count > 0:
                found_sources.add(traffic_source)
                logging.info(f"Found {record_count} records for {traffic_source}")

        missing_sources = required_sources - found_sources

        if missing_sources:
            logging.warning(f"Missing data for traffic sources in last 3 months: {', '.join(missing_sources)}")
            return False

        logging.info("All required traffic sources (Google, Bing, Facebook) have data in the last 3 months")
        return True

    except Exception as e:
        logging.error(f"Error checking traffic source data: {str(e)}")
        return False


with DAG(
    dag_id=dag_id,
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    start_date=datetime(2024, 5, 27),
    schedule_interval="0 11 * * *",  # 3:00 AM PST (11:00 AM UTC)
    catchup=False,
    max_active_runs=1,
    tags=["hourly"]
) as dag:
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

    update_dbt_status = PythonOperator(
        task_id="update_dbt_status",
        python_callable=check_failure,
        trigger_rule="all_done",
        op_kwargs={
            "current_dag_id": dag.dag_id,
            "task_group_and_task_ids": "dbt_build",
        },
        provide_context=True,
        retries=0,
    )
    task_groups = []
    task_group_and_task_ids = []

    for conn_id in airbyte_conn_ids:
        if conn_id[2] not in sync_exclusion_list:
            airbyte_task_group_name = f"airbyte_trigger_spree_{conn_id[2]}"
            airbyte_trigger_task_id = f"airbyte_trigger_spree_{conn_id[2]}_sync_to_gbq"
            update_airbyte_job_status_task_id = f"update_airbyte_job_status_{conn_id[2]}"
            airbyte_connection = conn_id[0]
            operator_id = conn_id[3]
            task_group_and_task_ids.append(
                f"{airbyte_task_group_name}.{airbyte_trigger_task_id}"
            )
            task_group_and_task_ids.append(
                f"{airbyte_task_group_name}.{update_airbyte_job_status_task_id}"
            )

            with TaskGroup(
                group_id=airbyte_task_group_name,
                tooltip="Group for triggering airbyte connections that sync to GBQ",
            ) as airbyte_trigger_group:
                
                # Check if this is the ads_spend task group that needs the Snowflake check
                if conn_id[2] == "ads_spend":
                    def combined_check():
                        snowflake_status = check_snowflake_task_status()
                        traffic_source_status = check_traffic_source_data()

                        logging.info(f"Snowflake task status: {snowflake_status}")
                        logging.info(f"Traffic source data status: {traffic_source_status}")

                        # Both conditions must be true to proceed
                        return snowflake_status and traffic_source_status

                    # Add the combined check before the airbyte trigger
                    check_task = ShortCircuitOperator(
                        task_id="check_snowflake_and_traffic_source_status",
                        python_callable=combined_check,
                        trigger_rule="all_done",
                    )
                    
                    airbyte_trigger_task = PythonOperator(
                        task_id=airbyte_trigger_task_id,
                        python_callable=trigger_airbyte_job,
                        on_failure_callback=lambda: None,
                        queue="kubernetes",
                        op_kwargs={
                            "connection_id": airbyte_connection,
                            "platform_id": platform_id,
                            "platform": platform,
                        },
                        trigger_rule="all_done",
                    )

                    update_airbyte_job_status = PythonOperator(
                        task_id=update_airbyte_job_status_task_id,
                        python_callable=update_airbyte_jobs,
                        op_kwargs={
                            "platform": platform,
                            "airbyte_trigger_task_name": f"{airbyte_task_group_name}.{airbyte_trigger_task_id}",
                            "operator_id": operator_id, 
                        },
                        do_xcom_push=False,
                    )
                    
                    # Add the check before the airbyte trigger
                    check_task >> airbyte_trigger_task >> update_airbyte_job_status
                else:
                    # For other connections, keep the original structure
                    airbyte_trigger_task = PythonOperator(
                        task_id=airbyte_trigger_task_id,
                        python_callable=trigger_airbyte_job,
                        on_failure_callback=lambda: None,
                        queue="kubernetes",
                        op_kwargs={
                            "connection_id": airbyte_connection,
                            "platform_id": platform_id,
                            "platform": platform,
                        },
                        trigger_rule="all_done",
                    )

                    update_airbyte_job_status = PythonOperator(
                        task_id=update_airbyte_job_status_task_id,
                        python_callable=update_airbyte_jobs,
                        op_kwargs={
                            "platform": platform,
                            "airbyte_trigger_task_name": f"{airbyte_task_group_name}.{airbyte_trigger_task_id}",
                            "operator_id": operator_id, 
                        },
                        do_xcom_push=False,
                    )
                    
                    airbyte_trigger_task >> update_airbyte_job_status

                task_groups.append(airbyte_trigger_group)

    update_airflow_task_status = PythonOperator(
        task_id="update_airflow_task_status",
        python_callable=check_failure,
        trigger_rule="all_done",
        op_kwargs={
            "current_dag_id": dag.dag_id,
            "task_group_and_task_ids": task_group_and_task_ids,
        },
        provide_context=True,
        retries=0,
    )

    (dbt_task  >> update_dbt_status >> task_groups >> update_airflow_task_status)