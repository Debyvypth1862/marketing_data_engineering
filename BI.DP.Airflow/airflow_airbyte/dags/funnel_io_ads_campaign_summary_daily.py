from datetime import datetime, timedelta
import pendulum

from airbyte.slack_alerts import task_id_slack_failure_alert
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

dag_id = "funnel_io_ads_campaign_summary_daily"
platform_name = "Funnel_IO"
working_dir = "/opt/airflow/dags/dbt/funnel_io/funnel_io"

def run_funnel_io_dbt_model(working_dir, platform_name, execution_date, **context):
    """
    Run specific dbt model for ADS_CAMPAIGN_SUMMARY upsert
    """
    import subprocess
    import logging
    from airflow.exceptions import AirflowException
    
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting DBT run for {platform_name} - ADS_CAMPAIGN_SUMMARY model")
    logger.info(f"Working directory: {working_dir}")
    
    # Run dbt for specific model
    result = subprocess.run(
        f"cd {working_dir} && dbt run --models ADS_CAMPAIGN_SUMMARY --debug",
        shell=True,
        capture_output=True,
        text=True
    )
    
    # Log all output
    if result.stdout:
        logger.info(f"DBT stdout:\n{result.stdout}")
    if result.stderr:
        logger.error(f"DBT stderr:\n{result.stderr}")
    
    if result.returncode == 0:
        logger.info("DBT run successful")
        return result.stdout
    else:
        error_message = f"DBT run failed with return code {result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        logger.error(error_message)
        raise AirflowException(error_message)

with DAG(
    dag_id=dag_id,
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    description="Daily upsert of Facebook Ads campaign data from Funnel.io to ADS_CAMPAIGN_SUMMARY table",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Los_Angeles"),
    # Run daily at 6am PST (after source data is typically refreshed)
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "funnel_io", "facebook_ads", "daily"]
) as dag:
    
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting Funnel.io DBT DAG..."',
    )

    stage_1 = f"Dbt_{platform_name}"

    # Install dbt dependencies
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {working_dir} && dbt deps",
    )

    # Debug dbt connection
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {working_dir} && dbt debug",
    )

    with TaskGroup(
            group_id=f"{platform_name}_DBT", tooltip=f"DBT processing for {platform_name}"
    ) as funnel_io_dbt_group:
        dbt_task = PythonOperator(
            task_id=stage_1,
            python_callable=run_funnel_io_dbt_model,
            op_kwargs={'execution_date': '{{ execution_date }}', 'working_dir': working_dir, 'platform_name': platform_name},
            provide_context=True,
            trigger_rule="all_done",
        )



    end = BashOperator(
        task_id="end",
        bash_command='echo "Funnel.io DBT processing completed successfully"',
    )

    start >> dbt_deps >> dbt_debug >> funnel_io_dbt_group >> end