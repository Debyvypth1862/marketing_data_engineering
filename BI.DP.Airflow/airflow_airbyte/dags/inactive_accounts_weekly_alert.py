from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airbyte.inactive_accounts_alert import execute_inactive_accounts_report
from airbyte.slack_file_notification import send_inactive_accounts_alert
from airbyte.Utils import Utils
import os
import logging

logger = logging.getLogger(__name__)

dag_id = "InactiveAccountsWeeklyAlert"

def simple_failure_callback(context):
    """
    Simple failure callback that logs the error
    """
    task_instance = context.get('task_instance')
    if task_instance:
        logger.error(f"Task {task_instance.task_id} failed in DAG {task_instance.dag_id}")

def cleanup_temp_files(**context):
    """
    Cleanup function - no action needed when using in-memory files
    Args:
        **context: Airflow context (to pull XCom if needed)
    """
    try:
        # Pull excel_data from XCom to verify it exists
        task_instance = context.get('task_instance')
        if task_instance:
            excel_data = task_instance.xcom_pull(task_ids='generate_inactive_accounts_report')
            if excel_data:
                logger.info("✅ File was created in-memory, no cleanup required")
                logger.info("No temporary files to delete")
            else:
                logger.info("No data to clean up")
        else:
            logger.info("No context provided, skipping cleanup check")
        return True
    except Exception as e:
        logger.error(f"Error in cleanup function: {e}")
        return True  # Don't fail the DAG on cleanup errors

with DAG(
    dag_id=dag_id,
    default_args={
        "owner": "airflow",
        "on_failure_callback": simple_failure_callback,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    # Schedule for every Monday at 6:00 AM PDT (13:00 UTC)
    # PDT is UTC-7, so 6 AM PDT = 1 PM UTC
    schedule_interval="0 13 * * MON",  # Cron: minute hour day month day_of_week
    start_date=datetime(2024, 12, 16),  # Start from next Monday
    catchup=False,
    max_active_runs=1,
    tags=["alerts", "weekly", "accounts"],
) as dag:
    
    # Start task
    start = BashOperator(
        task_id="start", 
        bash_command="echo 'Starting Inactive Accounts Weekly Alert Process'"
    )
    
    # Generate inactive accounts report
    generate_report = PythonOperator(
        task_id="generate_inactive_accounts_report",
        python_callable=execute_inactive_accounts_report,
        do_xcom_push=True,
        queue="kubernetes"
    )
    
    # Send Slack notification with Excel attachment
    send_slack_alert = PythonOperator(
        task_id="send_slack_alert_with_file",
        python_callable=send_inactive_accounts_alert,
        # Don't use op_kwargs - let the function pull XCom data from context
        trigger_rule="all_done",
        queue="kubernetes",
        retries=1,  # Only retry once for Slack
        retry_delay=timedelta(minutes=2),
    )
    
    # Clean up temporary files
    cleanup_files = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=cleanup_temp_files,
        # Don't use op_kwargs - let the function pull XCom data from context
        trigger_rule="all_done",
        queue="kubernetes",
    )
    
    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=Utils.update_task_id_details,
        trigger_rule="all_done",
        provide_context=True,
        op_kwargs={
            "current_dag_id": dag_id,
            "task_group_and_task_ids": ["generate_inactive_accounts_report", "send_slack_alert_with_file", "cleanup_temp_files"]
        },
        retries=3,
    )
    
    # Define task dependencies
    start >> generate_report >> send_slack_alert >> cleanup_files >> end
