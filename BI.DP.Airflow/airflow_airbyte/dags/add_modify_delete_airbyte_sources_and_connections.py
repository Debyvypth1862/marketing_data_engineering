from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airbyte.create_sources_and_connections.create_sources import create_check_sources
from airbyte.enable_disable_source import enable_disable
from airbyte.update_static_tables import update_data_source
from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.Utils import Utils

dag_id="AddModifyDeleteAirbyteSourcesAndConnections"
task_group_and_task_ids = []
with DAG(
    dag_id=dag_id,
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    schedule_interval=None,
    start_date=datetime(2024, 5, 27),
    catchup=False,
    max_active_runs=1
) as dag:
    start = BashOperator(task_id="start", bash_command="echo start")

    validate_task = PythonOperator(
        task_id="AddModifyDeleteAirbyteSourcesAndConnections",
        python_callable=create_check_sources,
        do_xcom_push=False,
    )

    enable_disable_task = PythonOperator(
        task_id="EnableDisable",
        python_callable=enable_disable,
        do_xcom_push=False
    )

    update_static_tables = PythonOperator(
        task_id="UpdateStaticTables",
        python_callable=update_data_source,
        do_xcom_push=False,
    )
    stage_1 ="AddModifyDeleteAirbyteSourcesAndConnections" 
    stage_2 ="EnableDisable"
    stage_3 = "UpdateStaticTables"
    no_of_tasks =3 
    
    for i in range(no_of_tasks):
        stage = eval(f'stage_{i+1}')
        task_group_and_task_ids.append(stage)   
        
    end = PythonOperator(
        task_id="end",
        python_callable=Utils.update_task_id_details,
        trigger_rule="all_done",
        provide_context=True,
        op_kwargs={"current_dag_id":dag_id,"task_group_and_task_ids":task_group_and_task_ids},
        retries=0,
    )         

    start >> validate_task >> enable_disable_task >> update_static_tables >> end
