from airflow import DAG
from airflow.operators.python import PythonOperator
from airbyte.sys_server import cron_schedule_validation_dag 
from airbyte.validate import validate
from airbyte.update_validation_details import update_validation_details
from datetime import timedelta, datetime
from airbyte.fetch_connection_list import fetch_platform_id_from_platform
from airbyte import constants
from airbyte.slack_alerts import task_id_slack_failure_alert
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance
from airbyte.Utils import Utils

Mexos_platform_id = (fetch_platform_id_from_platform(constants.Mexos))
Smartico_platform_id = (fetch_platform_id_from_platform(constants.Smartico))
SoftSwiss_platform_id = (fetch_platform_id_from_platform(constants.SoftSwiss))
Alanbase_platform_id = (fetch_platform_id_from_platform(constants.Alanbase))
NetRefer_platform_id = (fetch_platform_id_from_platform(constants.NetRefer))
MyAffiliates_platform_id = (fetch_platform_id_from_platform(constants.MyAffiliates))
Income_Access_platform_id = (fetch_platform_id_from_platform(constants.Income_Access))
EGO_platform_id = (fetch_platform_id_from_platform(constants.EGO))
Cellxpert_platform_id = (fetch_platform_id_from_platform(constants.Cellxpert))
Buffalo_Partners_platform_id = (fetch_platform_id_from_platform(constants.Inhouse))
Q_platform_id = (fetch_platform_id_from_platform(constants.Q))
Sweep_platform_id = (fetch_platform_id_from_platform(constants.Sweep))
Redtrack_platform_id = (fetch_platform_id_from_platform(constants.Redtrack))
Referon_platform_id = (fetch_platform_id_from_platform(constants.Referon))

dag_id='Validate_all_operator_accounts'
task_group_and_task_ids = []
with DAG(dag_id=dag_id,
         default_args={'owner': 'airflow',
                       'on_failure_callback': task_id_slack_failure_alert,
                        'retries': 3,
                        'retry_delay': timedelta(minutes=2)},
         schedule_interval=None,
         start_date=datetime(2024,5,27),
         catchup=False,
         max_active_runs=1
         ) as dag:
      start = BashOperator(task_id="start", bash_command="echo start")
      
      task_names=constants.Validate_task_names
      task_group_and_task_ids = task_names
      task_group_and_task_ids.append('update_validation_details')
      Update_details = PythonOperator(
      task_id = 'update_validation_details',
      python_callable=update_validation_details,
      trigger_rule="all_done",
      op_kwargs = {"task_names" : task_names },
      do_xcom_push=False
          )
      
      
      with TaskGroup(f'ValidateAll', tooltip=f'This task group performs Validation for all platforms') as Validate:
        Mexos_Validate = PythonOperator(
          task_id = 'Validate_Mexos',
          python_callable=validate,
          op_kwargs = {"platform_id" : Mexos_platform_id },
          queue="kubernetes"
              )
        
        Cellxpert_Validate = PythonOperator(
          task_id = 'Validate_Cellxpert',
          python_callable=validate,
          op_kwargs = {"platform_id" : Cellxpert_platform_id },
          queue="kubernetes"
              )
        
        Smartico_Validate = PythonOperator(
          task_id = 'Validate_Smartico',
          python_callable=validate,
          op_kwargs = {"platform_id" : Smartico_platform_id },
          queue="kubernetes"
              )          
            
        SoftSwiss_Validate = PythonOperator(
          task_id = 'Validate_SoftSwiss',
          python_callable=validate,
          op_kwargs = {"platform_id" : SoftSwiss_platform_id },
          queue="kubernetes"
              ) 
        Alanbase_Validate = PythonOperator(
          task_id = 'Validate_Alanbase',
          python_callable=validate,
          op_kwargs = {"platform_id" : Alanbase_platform_id },
          queue="kubernetes"
              ) 
        
        NetRefer_Validate = PythonOperator(
          task_id = 'Validate_NetRefer',
          python_callable=validate,
          op_kwargs = {"platform_id" : NetRefer_platform_id },
          queue="kubernetes"
              ) 
        
        MyAffiliates_Validate = PythonOperator(
          task_id = 'Validate_MyAffiliates',
          python_callable=validate,
          op_kwargs = {"platform_id" : MyAffiliates_platform_id },
          queue="kubernetes"
              )
        
        Income_Access_Validate = PythonOperator(
          task_id = 'Validate_Income_Access',
          python_callable=validate,
          op_kwargs = {"platform_id" : Income_Access_platform_id },
          queue="kubernetes"
              ) 
        
        EGO_Validate = PythonOperator(
          task_id = 'Validate_EGO',
          python_callable=validate,
          op_kwargs = {"platform_id" : EGO_platform_id },
          queue="kubernetes"
              ) 
        
        Buffalo_Partners_Validate = PythonOperator(
          task_id = 'Validate_Buffalo_Partners',
          python_callable=validate,
          op_kwargs = {"platform_id" : Buffalo_Partners_platform_id },
          queue="kubernetes"
              ) 
        
        Q_Validate = PythonOperator(
          task_id = 'Validate_Q',
          python_callable=validate,
          op_kwargs = {"platform_id" : Q_platform_id },
          queue="kubernetes"
              )    

        Sweep_Validate = PythonOperator(
          task_id = 'Validate_Sweep',
          python_callable=validate,
          op_kwargs = {"platform_id" : Sweep_platform_id },
          queue="kubernetes"
              )
        
        Redtrack_Validate = PythonOperator(
          task_id = 'Validate_Redtrack',
          python_callable=validate,
          op_kwargs = {"platform_id" : Redtrack_platform_id },
          queue="kubernetes"
              )
        
        Referon_Validate = PythonOperator(
          task_id = 'Referon_Validate',
          python_callable=validate,
          op_kwargs = {"platform_id" : Referon_platform_id },
          queue="kubernetes"
              )

        [Alanbase_Validate, Cellxpert_Validate, Mexos_Validate, Q_Validate, Buffalo_Partners_Validate, EGO_Validate, Income_Access_Validate, MyAffiliates_Validate, NetRefer_Validate, SoftSwiss_Validate, Smartico_Validate, Referon_Validate, Sweep_Validate, Redtrack_Validate]

      Jira_create = PythonOperator(
        task_id = 'Jira_create',
        python_callable=Utils.operator_create_jira_ticket,
        trigger_rule="all_done",
        queue="kubernetes"
      )

      end = PythonOperator(
        task_id="end",
        python_callable=Utils.update_task_id_details,
        trigger_rule="all_done",
        provide_context=True,
        op_kwargs={"current_dag_id":dag_id,"task_group_and_task_ids":task_group_and_task_ids},
        retries=3,
        )    
      
      start >> Validate >> Update_details >> Jira_create >> end  
