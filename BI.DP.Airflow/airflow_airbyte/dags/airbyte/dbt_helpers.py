"""
Helper functions for DBT DAGs and trigger logic.

This module provides common utilities for all platform DBT DAGs including:
- DBT execution functions (external table refresh, build)
- Jira creation functions
- Trigger check functions (prevent queuing and infinite loops)
- Global DBT trigger enable/disable control
- Common branching logic

Usage:
    from airbyte.dbt_helpers import (
        run_dbt_external_table_refresh,
        run_dbt,
        check_enable_dbt_downstream_tasks,
        should_trigger_dbt,
        check_dag_running,
        decide_trigger_reprocess,
        check_failure
    )

    # For DBT tasks
    refresh_task = PythonOperator(
        task_id="refresh_task",
        python_callable=run_dbt_external_table_refresh,
        op_kwargs={
            'execution_date': '{{ execution_date }}',
            'working_dir': '/opt/airflow/dags/dbt/ego/ego',
            'platform_name': 'Ego'
        },
        provide_context=True,
    )

    # For checking if DBT triggering is globally enabled
    check_dbt_enabled = ShortCircuitOperator(
        task_id="check_dbt_enabled",
        python_callable=should_trigger_dbt,
        provide_context=True,
    )

    # For checking if DAG is running before triggering
    check_dbt_running = ShortCircuitOperator(
        task_id="check_dbt_running",
        python_callable=check_dag_running,
        op_kwargs={"dag_id_to_check": "EgoDbtTriggered"},
        provide_context=True,
    )

    # For deciding whether to trigger reprocess DAG
    branch_task = BranchPythonOperator(
        task_id='decide_trigger_reprocess',
        python_callable=decide_trigger_reprocess,
        op_kwargs={"reprocess_dag_id": "ReprocessEgoExecuteAllOperatorAccounts"},
        provide_context=True,
    )
"""

import pytz
import logging
import subprocess
from datetime import datetime
from airflow.models import DagRun, Variable
from airflow.utils.state import State
from airflow.utils.session import create_session
from airflow.exceptions import AirflowException
from airbyte.Utils import Utils

logger = logging.getLogger(__name__)


def run_dbt_external_table_refresh(working_dir, platform_name, execution_date, **context):
    """
    Execute DBT external table refresh operation.

    This function runs the dbt run-operation command to refresh external table metadata
    in Snowflake, ensuring the latest data is available before running DBT models.

    Args:
        working_dir (str): Path to the DBT project directory
        platform_name (str): Name of the platform (e.g., 'Ego', 'Cellxpert')
        execution_date (str): Airflow execution date (ISO format)
        **context: Airflow context including task_instance

    Returns:
        None

    Raises:
        AirflowException: If DBT operation fails
    """
    job_id = context['task_instance'].job_id
    execution_time = datetime.fromisoformat(execution_date).astimezone(pytz.UTC)
    current_day = execution_time.date()

    logger.info(f"Starting external table refresh for {platform_name}")
    logger.info(f"Working directory: {working_dir}")
    logger.info(f"Execution date: {current_day}")

    result = subprocess.run(
        f"cd {working_dir} &&  dbt run-operation stage_external_sources --vars 'ext_data_refresh: true'",
        shell=True,
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        logging.info(f"{result.stdout}")
        Utils.generate_data_dbt_execute(job_id, platform_name, "DBT", "Success", "", current_day)
        logger.info(f"External table refresh completed successfully for {platform_name}")
    else:
        logging.info(f"{result.stderr}")
        Utils.generate_data_dbt_execute(job_id, platform_name, "DBT", "Failed", result.stdout, current_day)
        raise AirflowException(f"{result.stderr}")


def run_dbt(working_dir, platform_name, execution_date, **context):
    """
    Execute DBT build command.

    This function runs the dbt build command which runs all models, tests, and snapshots
    in the DBT project in dependency order.

    Args:
        working_dir (str): Path to the DBT project directory
        platform_name (str): Name of the platform (e.g., 'Ego', 'Cellxpert')
        execution_date (str): Airflow execution date (ISO format)
        **context: Airflow context including task_instance

    Returns:
        None

    Raises:
        AirflowException: If DBT build fails
    """
    job_id = context['task_instance'].job_id
    execution_time = datetime.fromisoformat(execution_date).astimezone(pytz.UTC)
    current_day = execution_time.date()

    logger.info(f"Starting DBT build for {platform_name}")
    logger.info(f"Working directory: {working_dir}")
    logger.info(f"Execution date: {current_day}")

    result = subprocess.run(
        f"cd {working_dir} && dbt build",
        shell=True,
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        logging.info(f"{result.stdout}")
        Utils.generate_data_dbt_execute(job_id, platform_name, "DBT", "Success", "", current_day)
        logger.info(f"DBT build completed successfully for {platform_name}")
    else:
        Utils.generate_data_dbt_execute(job_id, platform_name, "DBT", "Failed", result.stdout, current_day)
        raise AirflowException(f"{result.stdout}")


def check_enable_dbt_downstream_tasks(**context):
    """
    Check if DBT downstream tasks should be executed via Airflow Variable.

    This function checks the global 'enable_dbt_downstream_tasks' variable to determine
    if DAGs should proceed with DBT-related downstream tasks after the end task.
    This provides a centralized on/off switch to control whether DBT tasks are executed.

    Args:
        **context: Airflow context (unused but required for operator)

    Returns:
        bool: True if variable exists and is set to "yes", False otherwise (default)

    Usage:
        check_downstream = ShortCircuitOperator(
            task_id="check_enable_dbt_downstream_tasks",
            python_callable=check_enable_dbt_downstream_tasks,
            provide_context=True,
        )
    """
    try:
        enable_downstream = Variable.get("enable_dbt_downstream_tasks", default_var="no")
        should_proceed = enable_downstream.lower() == "yes"

        if should_proceed:
            logger.info("DBT downstream tasks are ENABLED (enable_dbt_downstream_tasks=yes)")
        else:
            logger.info("DBT downstream tasks are DISABLED (enable_dbt_downstream_tasks=no or not set) - stopping at end task")

        return should_proceed
    except Exception as e:
        logger.warning(f"Error checking enable_dbt_downstream_tasks variable: {e}. Defaulting to DISABLED.")
        return False


def should_trigger_dbt(**context):
    """
    Check if DBT triggering is enabled via Airflow Variable.

    This function checks the global 'enable_dbt_trigger' variable to determine
    if platform DAGs should trigger their respective DBT DAGs. This provides
    a centralized on/off switch for all DBT triggering across all platforms.

    Args:
        **context: Airflow context (unused but required for operator)

    Returns:
        bool: True if DBT triggering is enabled (variable = "yes"), False otherwise

    Usage:
        check_dbt_enabled = ShortCircuitOperator(
            task_id="check_dbt_enabled",
            python_callable=should_trigger_dbt,
            provide_context=True,
        )
    """
    try:
        enable_trigger = Variable.get("enable_dbt_trigger", default_var="no")
        should_trigger = enable_trigger.lower() == "yes"

        if should_trigger:
            logger.info("DBT triggering is ENABLED (enable_dbt_trigger=yes)")
        else:
            logger.info("DBT triggering is DISABLED (enable_dbt_trigger=no or not set)")

        return should_trigger
    except Exception as e:
        logger.warning(f"Error checking enable_dbt_trigger variable: {e}. Defaulting to DISABLED.")
        return False


def check_dag_running(dag_id_to_check, **context):
    """
    Check if a specific DAG is currently running or queued.

    This function is designed to be used with ShortCircuitOperator to prevent
    triggering a DAG if it's already running or queued.

    Args:
        dag_id_to_check (str): The DAG ID to check for active runs
        **context: Airflow context (unused but required for operator)

    Returns:
        bool: False if DAG is running/queued (skip downstream), True otherwise (continue)
    """
    with create_session() as session:
        # Check for both RUNNING and QUEUED states
        active_dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id_to_check,
            DagRun.state.in_([State.RUNNING, State.QUEUED])
        ).count()

    if active_dag_runs > 0:
        logger.info(f"Skipping trigger - {dag_id_to_check} is currently running or queued ({active_dag_runs} active runs)")
        return False

    logger.info(f"No active runs detected for {dag_id_to_check} - proceeding with trigger")
    return True


def decide_trigger_reprocess(reprocess_dag_id, **context):
    """
    Decide whether to trigger reprocess DAG based on who triggered this DBT run.

    This function implements the loop prevention logic:
    - If triggered by main DAG: trigger reprocess (first run)
    - If triggered by reprocess DAG: skip to avoid infinite loop (second run)
    - If reprocess DAG is currently running or queued: skip to avoid queuing

    Args:
        reprocess_dag_id (str): The ID of the reprocess DAG to potentially trigger
        **context: Airflow context including dag_run

    Returns:
        str: Task ID to branch to ('trigger_reprocess_dag' or 'skip_reprocess_trigger')
    """
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    triggered_by = conf.get('triggered_by', 'main_dag')

    logger.info(f"DBT DAG triggered by: {triggered_by}")

    if triggered_by == 'reprocess_dag':
        # This is the second run (after reprocess), don't trigger again
        logger.info("Skipping reprocess trigger - already triggered by reprocess DAG")
        return 'skip_reprocess_trigger'

    # Check if reprocess DAG is currently running or queued
    with create_session() as session:
        # Check for both RUNNING and QUEUED states
        active_dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == reprocess_dag_id,
            DagRun.state.in_([State.RUNNING, State.QUEUED])
        ).count()

    if active_dag_runs > 0:
        logger.info(f"Skipping reprocess trigger - {reprocess_dag_id} is currently running or queued ({active_dag_runs} active runs)")
        return 'skip_reprocess_trigger'

    # This is the first run (after main DAG), trigger reprocess
    logger.info("Triggering reprocess DAG - first run after main DAG and no active runs")
    return 'trigger_reprocess_dag'


def check_failure(current_dag_id, task_group_and_task_ids, **context):
    """
    Check if any tasks in the DAG run have failed.

    This function is typically used as the final task in a DAG to verify
    that all tasks completed successfully before updating metadata.

    Args:
        current_dag_id (str): The current DAG ID
        task_group_and_task_ids (list): List of task IDs to update in metadata
        **context: Airflow context including dag_run

    Returns:
        None

    Raises:
        Exception: If one or more tasks failed
    """
    Utils.update_task_id_details(current_dag_id, task_group_and_task_ids)
    dag_run = context["dag_run"]
    for task_instance in dag_run.get_task_instances():
        if task_instance.state == State.FAILED:
            raise Exception("One or more tasks failed.")


def create_jira_for_failed_dbt(execution_date, dag, **kwargs):
    """
    Create Jira tickets for failed DBT jobs.

    This function is idempotent - it queries the DBT_JOB table and only creates
    Jira tickets for failures that don't already have tickets.

    Args:
        execution_date (str): Airflow execution date (ISO format)
        dag (DAG): The DAG object
        **kwargs: Additional keyword arguments

    Returns:
        None
    """
    execution_time = datetime.fromisoformat(execution_date).astimezone(pytz.UTC)
    current_day = execution_time.date()
    # Check failed dbt jobs and create Jira tickets if needed
    Utils.select_failed_dbt(current_day)
