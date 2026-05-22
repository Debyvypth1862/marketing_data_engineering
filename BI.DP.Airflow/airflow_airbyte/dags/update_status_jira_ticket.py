import logging
import requests
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.state import State

from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.Utils import Utils
from airbyte.db_connection import mysql_conn

task_logger = logging.getLogger("airflow.task")
task_group_and_task_ids = []
dag_id = "Update_Status_Jira_Ticket"

def check_and_update_jira_tickets(table_name, id_field, status_field):
    logger = logging.getLogger("airflow.task")
    logger.info(f"Starting to check Jira ticket status for {table_name}")
    
    # Load Jira configuration once
    config = Utils.load_jira_config()
    headers = Utils.create_authorization_header(config["email"], config["token"])
    
    connection = None
    try:
        connection = mysql_conn()
        if connection is None:
            logger.error("Failed to connect to MySQL database")
            return f"Failed to connect to MySQL database for {table_name}"
            
        with connection.cursor(dictionary=True) as cursor:
            # Build SELECT clause
            select_fields = [f"id, {id_field}, {status_field}"]
            
            select_clause = ", ".join(select_fields)
            
            # Execute query
            cursor.execute(
                f"SELECT {select_clause} FROM {table_name} "
                f"WHERE {id_field} IS NOT NULL AND {id_field} != '' "
                f"AND ({status_field} != 'COMPLETED' OR {status_field} IS NULL)"
            )
            records = cursor.fetchall()
            
            logger.info(f"Found {len(records)} {table_name} records to check")
            
            # Process records in batches (optional)
            for record in records:
                ticket_id = record[id_field]
                current_status = record[status_field]
                
                # Get Jira ticket status using the Jira API
                url = f"{config['url_api']}/{ticket_id}"
                
                try:
                    response = requests.get(url, headers=headers)
                    
                    if response.status_code == 200:
                        jira_data = response.json()
                        jira_status = jira_data.get('fields', {}).get('status', {}).get('name')
                        
                        logger.info(f"Ticket {ticket_id} - Current status: {current_status}, Jira status: {jira_status}")
                        
                        # If status is different, update the database
                        if jira_status and jira_status != current_status:
                            logger.info(f"Updating ticket {ticket_id} status from {current_status} to {jira_status}")
                            
                            cursor.execute(
                                f"UPDATE {table_name} SET {status_field} = %s WHERE id = %s",
                                (jira_status, record['id'])
                            )
                            connection.commit()
                    else:
                        logger.error(f"Failed to get Jira ticket {ticket_id} status. Status code: {response.status_code}")
                        logger.error(f"Response: {response.text}")
                        
                except Exception as e:
                    logger.error(f"Error checking Jira ticket {ticket_id}: {str(e)}")
    except Exception as e:
        logger.error(f"Error in check_and_update_jira_tickets for {table_name}: {str(e)}")
    finally:
        if connection is not None and connection.is_connected():
            connection.close()
    
    return f"Jira ticket status check completed for {table_name}"

def fn_check_operators_jira(**kwargs):
    return check_and_update_jira_tickets(
        table_name="ACCOUNT",
        id_field="shortcut_ticket_id",
        status_field="shortcut_ticket_status"
    )

def fn_check_jobs_jira(**kwargs):
    return check_and_update_jira_tickets(
        table_name="JOB",
        id_field="shortcut_ticket_id",
        status_field="shortcut_ticket_status"
    )

def fn_check_dbt_jira(**kwargs):
    return check_and_update_jira_tickets(
        table_name="DBT_JOB",
        id_field="incident_ticket_id",
        status_field="incident_ticket_status"
    )

# Function to check if any tasks failed and update task details
def check_failure(current_dag_id,task_group_and_task_ids,**context):
    Utils.update_task_id_details(current_dag_id,task_group_and_task_ids)
    dag_run = context["dag_run"]
    for task_instance in dag_run.get_task_instances():
        if task_instance.state == State.FAILED:
            raise Exception("One or more tasks failed.")

with DAG(
        dag_id=dag_id,
        default_args={
            "owner": "airflow",
            "on_failure_callback": task_id_slack_failure_alert,
            "retries": 3,
            "retry_delay": timedelta(minutes=2),
            "priority_weight": 5,
        },
        schedule_interval="0 1 * * *",  # Run daily at 1:00 AM
        start_date=datetime(2024, 5, 27),
        catchup=False,
        max_active_runs=1,
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting the DAG..."',
    )

    check_operators_jira = PythonOperator(
        task_id='Check_Operators_Jira',
        python_callable=fn_check_operators_jira,
        op_kwargs={},
        provide_context=True,
        trigger_rule="all_success",
    )
    
    check_jobs_jira = PythonOperator(
        task_id='Check_Jobs_Jira',
        python_callable=fn_check_jobs_jira,
        op_kwargs={},
        provide_context=True,
        trigger_rule="all_success",
    )
    
    check_dbt_jira = PythonOperator(
        task_id='Check_Dbt_Jira',
        python_callable=fn_check_dbt_jira,
        op_kwargs={},
        provide_context=True,
        trigger_rule="all_success",
    )

    end = PythonOperator(
        task_id="end",
        python_callable=check_failure,
        trigger_rule="all_done",
        op_kwargs={"current_dag_id":dag_id,"task_group_and_task_ids":task_group_and_task_ids},        
        provide_context=True,
        retries=0,
    )

    start >> [check_operators_jira, check_jobs_jira, check_dbt_jira] >> end
