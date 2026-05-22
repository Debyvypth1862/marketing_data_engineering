from airflow import DAG
from airflow.operators.python import PythonOperator

from airbyte.api_six_sync import update_rate_limit
from datetime import timedelta, datetime
from airbyte.sys_server import cron_schedule 
from airbyte.slack_alerts import task_id_slack_failure_alert

# This DAG will update the rate limit of the route based on number of rate limit which stored in the Airflow variables.
# So to update the rate limit, you can access to the Airflow and change the number in the variables and execute this DAG.

with DAG(dag_id='Api_six_update_rate_limit',
         default_args={'owner': 'airflow',
                       'on_failure_callback': task_id_slack_failure_alert,
                       'retries': 3,
                       'retry_delay': timedelta(minutes=2)},
         schedule_interval=cron_schedule,
         start_date=datetime(2024,5,27),
         catchup=False,
         max_active_runs=1
         ) as dag:
      

    update_rate_limit = PythonOperator(task_id=f'Api_six_update_rate_limit', 
                                python_callable=update_rate_limit,
                                do_xcom_push=False 
                            )
    
    update_rate_limit 