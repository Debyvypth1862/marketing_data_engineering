import pytz
import logging
import os
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.state import State

from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.snowflake_refresh_table import generate_snowflake_query
from airbyte.Utils import Utils
from airflow.exceptions import AirflowException
import subprocess

task_logger = logging.getLogger("airflow.task")
dbt_working_directories = {
    "Dbt_Buffalo_partner": "/opt/airflow/dags/dbt/buffalo_partner/buffalo_partner",
    "Dbt_Cellxpert": "/opt/airflow/dags/dbt/cellxpert/cellxpert",
    "Dbt_Ego": "/opt/airflow/dags/dbt/ego/ego",
    "Dbt_Income_access": "/opt/airflow/dags/dbt/income_access/income_access",
    "Dbt_Mexos": "/opt/airflow/dags/dbt/mexos/mexos",
    "Dbt_Myaffiliates": "/opt/airflow/dags/dbt/myaffiliates/myaffiliates",
    "Dbt_Netrefer": "/opt/airflow/dags/dbt/netrefer/netrefer",
    "Dbt_Q": "/opt/airflow/dags/dbt/q_platform/q_platform",
    "Dbt_Softswiss": "/opt/airflow/dags/dbt/softswiss/softswiss",
    "Dbt_Smartico": "/opt/airflow/dags/dbt/smartico/smartico",
    "Dbt_Google_Analytics_4": "/opt/airflow/dags/dbt/ga4/ga4",
    "Dbt_ReferON": "/opt/airflow/dags/dbt/referon/referon",
    "Dbt_Sapphirebet": "/opt/airflow/dags/dbt/sapphirebet/sapphirebet"
}
task_group_and_task_ids = []
no_of_tasks =2
dag_id = "DbtExecuteAllPlatforms"

def check_failure(current_dag_id,task_group_and_task_ids,**context):
    Utils.update_task_id_details(current_dag_id,task_group_and_task_ids)
    dag_run = context["dag_run"]
    for task_instance in dag_run.get_task_instances():
        if task_instance.state == State.FAILED:
            raise Exception("One or more tasks failed.")

def is_last_run_of_day(execution_date, dag, **kwargs):
    execution_time = datetime.fromisoformat(execution_date).astimezone(pytz.UTC)
    current_day = execution_time.date()
    # Check if there are any DAG runs after the current one within the same day
    next_run_time = execution_time + timedelta(minutes=60)
    if next_run_time.date() > current_day:
        task_logger.info(f"Next run time is ---{next_run_time}, Current day is{current_day} ")
        return "Send_Alerts.Fetch_Unprocessed_Count_And_Paths" 

    task_logger.info(f"Next run time is ---{next_run_time}, Current day is{current_day} ")
    return "end" 


# Function to fetch unprocessed records with PATH from Snowflake
def get_unprocessed_count_and_paths(**kwargs):
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    db = os.getenv("snowflake_db_dbt")
    schema = os.getenv("snowflake_schema_dbt")
    tbl = os.getenv("snowflake_table_dbt")
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    #Substracting 1hour to make sure current date is todays date
    current_date = (datetime.now()  - timedelta(hours=1)).strftime('%Y-%m-%d')
    task_logger.info(f"Current date from python package---->{current_date}")

    query = f"""
            SELECT COUNT(*) AS unprocessed_count, PATH
            FROM {db}.{schema}.{tbl}
            WHERE IS_PROCESSED = FALSE and PICKED_FOR_REPROCESS = FALSE and S3_FILE_ARRIVAL_DATE = '{current_date}' GROUP BY PATH;       
            """

    # Execute the query and fetch the result
    results = snowflake_hook.get_records(query)
    
    # Prepare a list of paths and the count
    unprocessed_count = 0
    paths = []
    
    if results:
        unprocessed_count = sum([record[0] for record in results]) 
        paths = [record[1] for record in results]  # Extracting paths

    return unprocessed_count, paths

def run_dbt_external_table_refresh(working_dir, platform_name, execution_date, **context):
    job_id = context['task_instance'].job_id
    # Get the current time and the execution hour
    execution_time = datetime.fromisoformat(execution_date).astimezone(pytz.UTC)
    current_day = execution_time.date()

    result = subprocess.run(f"cd {working_dir} &&  dbt run-operation stage_external_sources --vars 'ext_data_refresh: true'", 
                            shell=True, 
                            capture_output=True, 
                            text=True)
    if result.returncode == 0:
        logging.info(f"{result.stdout}")
        Utils.generate_data_dbt_execute(job_id, platform_name, "DBT", "Success", "", current_day)
    else:
        logging.info(f"{result.stderr}")
        Utils.generate_data_dbt_execute(job_id, platform_name, "DBT", "Failed", result.stdout, current_day)
        raise AirflowException(f"{result.stderr}")
    
def run_dbt(working_dir, platform_name, execution_date, **context):
    job_id = context['task_instance'].job_id
    # Get the current time and the execution hour
    execution_time = datetime.fromisoformat(execution_date).astimezone(pytz.UTC)
    current_day = execution_time.date()

    result = subprocess.run(f"cd {working_dir} && dbt run", 
                            shell=True, 
                            capture_output=True, 
                            text=True)
    if result.returncode == 0:
        logging.info(f"{result.stdout}")
        Utils.generate_data_dbt_execute(job_id, platform_name, "DBT", "Success", "", current_day)
    else:
        Utils.generate_data_dbt_execute(job_id, platform_name, "DBT", "Failed", result.stdout, current_day)
        raise AirflowException(f"{result.stdout}")

def Jira_creation(execution_date, dag, **kwargs):
    # Get the current time and the execution hour
    execution_time = datetime.fromisoformat(execution_date).astimezone(pytz.UTC)
    current_day = execution_time.date()
    # Check failed dbt jobs
    Utils.select_failed_dbt(current_day)

with DAG(
        dag_id="DbtExecuteAllPlatforms",
        default_args={
            "owner": "airflow",
            "on_failure_callback": task_id_slack_failure_alert,
            "retries": 3,
            "retry_delay": timedelta(minutes=2),
            "priority_weight": 5,
            
        },
        schedule_interval="15 * * * *",  # run every hour at 15 minutes past the hour
        start_date=datetime(2024, 5, 27),
        catchup=False,
        max_active_runs=1,
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting the DAG..."',
        
    )

    # TaskGroup for fetching unprocessed count and paths + sending Slack status
    with TaskGroup("Send_Alerts", tooltip="Fetch unprocessed count and send Slack notification") as Send_Alerts:
        Fetch_Unprocessed_Count_And_Paths = PythonOperator(
            task_id='Fetch_Unprocessed_Count_And_Paths',
            python_callable=get_unprocessed_count_and_paths,
            provide_context=True,
        )

        Send_Dbt_Status_With_Unprocessed_Count_And_Paths = SlackWebhookOperator(
            task_id='Send_Dbt_Status_With_Unprocessed_Count_And_Paths',
            slack_webhook_conn_id='slack_webhook',
            message="""
            The final DBT DAG run for today has been completed successfully and the total number of unprocessed file(s) for today are {{ task_instance.xcom_pull(task_ids='Send_Alerts.Fetch_Unprocessed_Count_And_Paths')[0] }}.
            {% if task_instance.xcom_pull(task_ids='Send_Alerts.Fetch_Unprocessed_Count_And_Paths')[0] > 0 %}
            {{ task_instance.xcom_pull(task_ids='Send_Alerts.Fetch_Unprocessed_Count_And_Paths')[1] }}
            {% else %}
            No unprocessed files for today.
            {% endif %}
                """,
        )

        # Set task dependencies
        Fetch_Unprocessed_Count_And_Paths >> Send_Dbt_Status_With_Unprocessed_Count_And_Paths

    # Continue with the existing logic
    check_last_run = BranchPythonOperator(
        task_id='Check_Last_Run_Of_Day',
        python_callable=is_last_run_of_day,
        op_kwargs={'execution_date': '{{ execution_date }}', 'dag': dag},
        provide_context=True,
        trigger_rule="all_done",
    )

    Jira_create = PythonOperator(
        task_id = 'Jira_create',
        python_callable=Jira_creation,
        trigger_rule="all_done",
        op_kwargs={'execution_date': '{{ execution_date }}', 'dag': dag},
        provide_context=True,
        queue="kubernetes"
    )

    end = PythonOperator(
        task_id="end",
        python_callable=check_failure,
        trigger_rule="all_done",
        op_kwargs={"current_dag_id":dag_id,"task_group_and_task_ids":task_group_and_task_ids},        
        provide_context=True,
        retries=0,
        
    )

    task_groups = []

    for task_id, working_dir in dbt_working_directories.items():
        platform_name = task_id.replace("Dbt_", "")
        task_group_name = f"{platform_name}",
        stage_1 = f"Refresh_Snowflake_Extended_Tables_Metadata_{platform_name}"
        stage_2 = task_id
        with TaskGroup(
                group_id=f"{platform_name}", tooltip=f"Group for {platform_name}"
        ) as tg:
            # refresh_task = SnowflakeOperator(
            #     task_id=f"Refresh_Snowflake_Extended_Tables_Metadata_{platform_name}",
            #     sql=generate_snowflake_query(platform_name),
            #     snowflake_conn_id="snowflake_conn",
                
            # )


            refresh_task = PythonOperator(
                task_id=f"Refresh_Snowflake_Extended_Tables_Metadata_{platform_name}",
                python_callable=run_dbt_external_table_refresh,
                op_kwargs={'execution_date': '{{ execution_date }}','working_dir': working_dir, 'platform_name': platform_name},
                provide_context=True,
                trigger_rule="all_done",
                queue="kubernetes",
            )

            dbt_task = PythonOperator(
                task_id=task_id,
                python_callable=run_dbt,
                op_kwargs={'execution_date': '{{ execution_date }}','working_dir': working_dir, 'platform_name': platform_name},
                provide_context=True,
                trigger_rule="all_done",
                queue="kubernetes",
            )

            refresh_task >> dbt_task
            
        for i in range(no_of_tasks):
            stage = eval(f'stage_{i+1}')
            task_group_and_task_ids.append(f'{platform_name}.{stage}')                

        task_groups.append(tg)

    start >> task_groups
    for tg in task_groups:
        tg >> check_last_run
    check_last_run >> [Send_Alerts, end]
    Send_Alerts >> Jira_create >> end
