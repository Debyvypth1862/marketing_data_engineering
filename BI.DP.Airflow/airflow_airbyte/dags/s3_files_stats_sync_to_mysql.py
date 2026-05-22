from datetime import datetime, timedelta

from airbyte.airbyte_jobs import trigger_airbyte_job
from airbyte.slack_alerts import task_id_slack_failure_alert
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

airbyte_connection_id = Variable.get("s3_file_stats_sync_job_id")
dag_id = "S3FilesStatsSyncToMySQL"

with DAG(
    dag_id=dag_id,
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    start_date=datetime(2025, 6, 13),
    schedule_interval="0 */2 * * *",  # Every 2 hours
    catchup=False,
    max_active_runs=1,
    tags=["near-real-time"],
) as dag:
    airbyte_trigger_task = PythonOperator(
        task_id="airbyte_trigger_task",
        python_callable=trigger_airbyte_job,
        on_failure_callback=lambda: None,
        queue="kubernetes",
        op_kwargs={
            "connection_id": airbyte_connection_id,
        },
    )
    airbyte_trigger_task
