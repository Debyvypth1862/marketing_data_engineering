from datetime import datetime, timedelta
import pendulum

from airbyte.slack_alerts import task_id_slack_failure_alert
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

dag_id = "dbtApiBuild"
dbt_working_directories = {
    "Dbt_api_endpoint": "/opt/airflow/dags/dbt/api_endpoint/api_endpoint",
    "Dbt_crypto_cashback": "/opt/airflow/dags/dbt/crypto_cashback/crypto_cashback"
}

def run_api_endpoint_dbt_build(working_dir, dbt_project, execution_date, **context):
    import subprocess
    import logging
    import threading
    import queue
    from airflow.exceptions import AirflowException

    # Run dbt build command (no profiles-dir needed, it should find profiles.yml automatically)
    dbt_command = f"cd {working_dir} && dbt build"

    # Create queues for stdout and stderr
    stdout_queue = queue.Queue()
    stderr_queue = queue.Queue()
    stdout_lines = []
    stderr_lines = []

    # Define reader functions for each stream
    def read_stream(stream, queue, lines):
        for line in iter(stream.readline, ""):
            queue.put(line)
            lines.append(line)
            print(line, end="")
        stream.close()

    # Start the process
    process = subprocess.Popen(
        dbt_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,  # Line-buffered
        universal_newlines=True,
    )

    # Start threads to read each stream
    stdout_thread = threading.Thread(
        target=read_stream, args=(process.stdout, stdout_queue, stdout_lines)
    )
    stderr_thread = threading.Thread(
        target=read_stream, args=(process.stderr, stderr_queue, stderr_lines)
    )
    stdout_thread.daemon = True
    stderr_thread.daemon = True
    stdout_thread.start()
    stderr_thread.start()

    # Wait for the process to complete
    return_code = process.wait()

    # Wait for the threads to finish
    stdout_thread.join()
    stderr_thread.join()

    # Combine captured output
    stdout = "".join(stdout_lines)
    stderr = "".join(stderr_lines)

    if return_code != 0:
        error_message = f"stderr:\n{stderr}"
        logging.error(f"dbt build failed: {error_message}")
        raise AirflowException("DBT build failed. Check the logs for more details.")

    logging.info("dbt build completed successfully")
    return stdout

with DAG(
    dag_id=dag_id,
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Los_Angeles"),
    # Run daily at 5am PST
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "api"]
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting API Endpoint data pipeline..."',
    )

    # Create task groups for each dbt project
    task_groups = {}
    for group_id, working_dir in dbt_working_directories.items():
        dbt_project = group_id.split("Dbt_")[1]

        with TaskGroup(
            group_id=group_id,
            tooltip=f"Group for {dbt_project} dbt build",
        ) as dbt_build_group:
            dbt_build = PythonOperator(
                task_id=f"{dbt_project}_dbt_build",
                python_callable=run_api_endpoint_dbt_build,
                op_kwargs={
                    "execution_date": "{{ execution_date }}",
                    "working_dir": working_dir,
                    "dbt_project": dbt_project,
                },
                provide_context=True,
                trigger_rule="all_done",
            )
        task_groups[group_id] = dbt_build_group

    end = BashOperator(
        task_id="end",
        bash_command='echo "API Endpoint data pipeline completed successfully"',
    )

    # Set up dependencies: start -> api_endpoint -> crypto_cashback -> end
    start >> task_groups["Dbt_api_endpoint"] >> task_groups["Dbt_crypto_cashback"] >> end