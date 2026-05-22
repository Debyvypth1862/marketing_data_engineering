"""
Maintenance DAG to monitor and cleanup long-running DAG runs and stuck tasks.

This DAG runs every 6 hours to:
1. Find DAG runs that have been RUNNING or QUEUED for more than threshold (default: 24 hours)
2. Kill stuck tasks within those DAG runs
3. Mark the DAG run as failed
4. Send notifications about cleanup actions

Schedule: Every 6 hours (00:00, 06:00, 12:00, 18:00)
Max Runtime Threshold: 24 hours (configurable via Airflow Variable: dag_max_runtime_hours)
States Monitored: RUNNING, QUEUED
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import DagRun, TaskInstance, Variable
from airflow.utils.state import State
from airflow.utils.session import create_session
from airflow.utils.timezone import utcnow
from airbyte.slack_alerts import task_id_slack_failure_alert
import logging

logger = logging.getLogger(__name__)


def get_max_runtime_hours():
    """
    Get maximum runtime threshold from Airflow Variable.
    Default: 24 hours (1 day)
    """
    try:
        max_hours = int(Variable.get("dag_max_runtime_hours", default_var="24"))
        logger.info(f"Using max runtime threshold: {max_hours} hours")
        return max_hours
    except Exception as e:
        logger.warning(f"Error reading dag_max_runtime_hours variable: {e}. Using default 24 hours")
        return 24


def find_long_running_dags(**context):
    """
    Find DAG runs that have been running or queued longer than the threshold.

    Returns:
        list: List of tuples (dag_id, run_id, hours_running, start_date, state)
    """
    max_runtime_hours = get_max_runtime_hours()
    threshold = utcnow() - timedelta(hours=max_runtime_hours)

    with create_session() as session:
        # Find DAGs that are RUNNING or QUEUED for too long
        long_running_dags = session.query(
            DagRun.dag_id,
            DagRun.run_id,
            DagRun.start_date,
            DagRun.state
        ).filter(
            DagRun.state.in_([State.RUNNING, State.QUEUED]),
            DagRun.start_date < threshold
        ).all()

        results = []
        for dag in long_running_dags:
            hours_running = (utcnow() - dag.start_date).total_seconds() / 3600
            results.append({
                'dag_id': dag.dag_id,
                'run_id': dag.run_id,
                'hours_running': round(hours_running, 2),
                'start_date': dag.start_date.isoformat(),
                'state': dag.state
            })
            logger.info(f"Found long-running DAG: {dag.dag_id} (run_id: {dag.run_id}, state: {dag.state}) - Running for {hours_running:.2f} hours")

        # Push to XCom for next task
        context['task_instance'].xcom_push(key='long_running_dags', value=results)

        if results:
            logger.warning(f"Found {len(results)} long-running/queued DAG runs exceeding {max_runtime_hours} hours")
        else:
            logger.info(f"No DAG runs found running or queued longer than {max_runtime_hours} hours")

        return len(results)


def kill_stuck_tasks(**context):
    """
    Kill all running tasks in long-running DAG runs.

    Returns:
        dict: Summary of killed tasks
    """
    ti = context['task_instance']
    long_running_dags = ti.xcom_pull(task_ids='find_long_running_dags', key='long_running_dags')

    if not long_running_dags:
        logger.info("No long-running DAGs to cleanup")
        return {"killed_tasks": 0, "dags_processed": 0}

    killed_tasks = []

    with create_session() as session:
        for dag_info in long_running_dags:
            dag_id = dag_info['dag_id']
            run_id = dag_info['run_id']

            # Find all running/queued tasks in this DAG run
            stuck_tasks = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.state.in_([State.RUNNING, State.QUEUED])
            ).all()

            logger.info(f"Processing DAG {dag_id} (run_id: {run_id}) - Found {len(stuck_tasks)} stuck tasks")

            for task in stuck_tasks:
                try:
                    logger.info(f"Killing task: {task.task_id} in DAG {dag_id} (state: {task.state})")

                    # Set task state to failed
                    task.state = State.FAILED
                    task.end_date = utcnow()

                    killed_tasks.append({
                        'dag_id': dag_id,
                        'run_id': run_id,
                        'task_id': task.task_id,
                        'original_state': task.state,
                        'killed_at': utcnow().isoformat()
                    })

                except Exception as e:
                    logger.error(f"Error killing task {task.task_id} in DAG {dag_id}: {e}")

        session.commit()

    summary = {
        'killed_tasks': len(killed_tasks),
        'dags_processed': len(long_running_dags),
        'details': killed_tasks
    }

    logger.info(f"Cleanup summary: Killed {len(killed_tasks)} tasks across {len(long_running_dags)} DAG runs")

    # Push to XCom for notification
    ti.xcom_push(key='cleanup_summary', value=summary)

    return summary


def mark_dags_as_failed(**context):
    """
    Mark long-running DAG runs as failed after killing their tasks.

    Returns:
        dict: Summary of marked DAG runs
    """
    ti = context['task_instance']
    long_running_dags = ti.xcom_pull(task_ids='find_long_running_dags', key='long_running_dags')

    if not long_running_dags:
        logger.info("No DAG runs to mark as failed")
        return {"marked_dags": 0}

    marked_dags = []

    with create_session() as session:
        for dag_info in long_running_dags:
            dag_id = dag_info['dag_id']
            run_id = dag_info['run_id']

            try:
                dag_run = session.query(DagRun).filter(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == run_id
                ).first()

                if dag_run and dag_run.state in [State.RUNNING, State.QUEUED]:
                    logger.info(f"Marking DAG run as failed: {dag_id} (run_id: {run_id}, previous state: {dag_run.state})")

                    dag_run.state = State.FAILED
                    dag_run.end_date = utcnow()

                    marked_dags.append({
                        'dag_id': dag_id,
                        'run_id': run_id,
                        'hours_running': dag_info['hours_running'],
                        'previous_state': dag_info.get('state', 'unknown'),
                        'marked_at': utcnow().isoformat()
                    })

            except Exception as e:
                logger.error(f"Error marking DAG run as failed {dag_id} (run_id: {run_id}): {e}")

        session.commit()

    summary = {
        'marked_dags': len(marked_dags),
        'details': marked_dags
    }

    logger.info(f"Marked {len(marked_dags)} DAG runs as failed")

    # Push to XCom for notification
    ti.xcom_push(key='marked_dags_summary', value=summary)

    return summary


def send_cleanup_notification(**context):
    """
    Send Slack notification about cleanup actions.
    Only sends if there were actual cleanups performed.
    """
    ti = context['task_instance']
    cleanup_summary = ti.xcom_pull(task_ids='kill_stuck_tasks', key='cleanup_summary')
    marked_dags_summary = ti.xcom_pull(task_ids='mark_dags_failed', key='marked_dags_summary')

    if not cleanup_summary or cleanup_summary.get('killed_tasks', 0) == 0:
        logger.info("No cleanup actions performed - skipping notification")
        return

    # Build notification message
    killed_tasks = cleanup_summary.get('killed_tasks', 0)
    dags_processed = cleanup_summary.get('dags_processed', 0)
    marked_dags = marked_dags_summary.get('marked_dags', 0)

    message = f"""
🧹 *DAG Maintenance Cleanup Report*

*Summary:*
• Processed: {dags_processed} long-running DAG run(s)
• Killed: {killed_tasks} stuck task(s)
• Marked as Failed: {marked_dags} DAG run(s)

*Details:*
"""

    # Add details for each DAG
    for detail in cleanup_summary.get('details', []):
        dag_id = detail.get('dag_id')
        task_id = detail.get('task_id')
        message += f"\n• `{dag_id}` → Task `{task_id}` killed"

    message += f"""

*Threshold:* DAG runs running longer than {get_max_runtime_hours()} hours

_Cleanup performed at: {utcnow().isoformat()}_
"""

    logger.info(f"Cleanup notification:\n{message}")

    # You can send this to Slack using your existing slack_alerts module
    # For now, just log it
    logger.warning(f"CLEANUP PERFORMED: {killed_tasks} tasks killed, {marked_dags} DAGs marked as failed")

    return message


# DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='maintenance_cleanup_long_running_dags',
    default_args=default_args,
    description='Monitor and cleanup DAG runs running longer than threshold',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'cleanup', 'monitoring'],
) as dag:

    # Start task
    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting maintenance cleanup of long-running DAGs'"
    )

    # Task 1: Find long-running DAGs
    find_long_running = PythonOperator(
        task_id='find_long_running_dags',
        python_callable=find_long_running_dags,
        provide_context=True,
    )

    # Task 2: Kill stuck tasks
    kill_tasks = PythonOperator(
        task_id='kill_stuck_tasks',
        python_callable=kill_stuck_tasks,
        provide_context=True,
    )

    # Task 3: Mark DAG runs as failed
    mark_failed = PythonOperator(
        task_id='mark_dags_failed',
        python_callable=mark_dags_as_failed,
        provide_context=True,
    )

    # Task 4: Send notification
    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_cleanup_notification,
        provide_context=True,
        trigger_rule='all_done',  # Run even if previous tasks fail
    )

    # End task
    end = BashOperator(
        task_id="end",
        bash_command="echo 'Maintenance cleanup completed'",
        trigger_rule='all_done',  # Always run
    )

    # Task dependencies
    start >> find_long_running >> kill_tasks >> mark_failed >> notify >> end
