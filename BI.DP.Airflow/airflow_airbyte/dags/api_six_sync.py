from airflow import DAG
from airflow.operators.python import PythonOperator

from airbyte.api_six_sync import apisix_sync
from datetime import timedelta, datetime
from airbyte.sys_server import cron_schedule 
from airbyte.slack_alerts import task_id_slack_failure_alert



with DAG(dag_id='Api_six_sync',
         default_args={'owner': 'airflow',
                       'on_failure_callback': task_id_slack_failure_alert,
                       'retries': 3,
                       'retry_delay': timedelta(minutes=2)},
         schedule_interval=None,
         start_date=datetime(2024,5,27),
         catchup=False,
         max_active_runs=1
         ) as dag:
      

    apisix_sync = PythonOperator(task_id=f'Api_six_sync', 
                                python_callable=apisix_sync,
                                do_xcom_push=False 
                            )
    
    apisix_sync 