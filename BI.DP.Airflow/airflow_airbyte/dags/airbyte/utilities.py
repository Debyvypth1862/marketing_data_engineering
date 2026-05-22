import logging
import os
import queue
import subprocess
import threading
import uuid
import zipfile
from datetime import date, datetime

import boto3
import pytz
from airflow.exceptions import AirflowException

from airbyte.env_config import DATA_SOURCE_ITEM_TABLE, JOB_DETAIL_TABLE, JOBS_TABLE
from airbyte.Utils import Utils

task_logger = logging.getLogger("airflow.task")


def run_dbt(working_dir, dbt_project, execution_date, **context):
    job_id = context['task_instance'].job_id
    # Get the current time and the execution hour
    execution_time = datetime.fromisoformat(execution_date).astimezone(pytz.UTC)
    current_day = execution_time.date()
    dbt_command = f"cd {working_dir} && dbt run-operation stage_external_sources --vars 'ext_data_refresh: true' && dbt build"

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

    if return_code == 0:
        Utils.generate_data_dbt_execute(job_id, dbt_project, "DBT", "Success", "", current_day)
    else:
        error_message = f"stdout:\n{stdout}\nstderr:\n{stderr}"
        Utils.generate_data_dbt_execute(
            job_id, dbt_project, "DBT", "Failed", error_message, current_day
        )
        raise AirflowException(error_message)

def run_dbt_and_upload_artifact(working_dir, dbt_project, execution_date, **context):
    job_id = context['task_instance'].job_id
    dag_id = context["dag"].dag_id
    logical_date =context["dag_run"].logical_date.isoformat()
    dbt_artifact_bucket = "your-dbt-artifacts-bucket"

    # Get the current time and the execution hour
    execution_time = datetime.fromisoformat(execution_date).astimezone(pytz.UTC)
    current_day = execution_time.date()
    dbt_command = f"cd {working_dir} && dbt run-operation stage_external_sources --vars 'ext_data_refresh: true' && dbt build"

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

    compress_and_upload_dbt_artifact(dbt_artifact_bucket, working_dir, dag_id, logical_date)

    if return_code == 0:
        Utils.generate_data_dbt_execute(job_id, dbt_project, "DBT", "Success", "", current_day)
    else:
        error_message = f"stderr:\n{stderr}"
        Utils.generate_data_dbt_execute(
            job_id, dbt_project, "DBT", "Failed", error_message, current_day
        )
        raise AirflowException("DBT build failed. Check the logs for more details.")


def compress_and_upload_dbt_artifact(
    dbt_artifact_bucket, working_dir, dag_id, logical_date
):
    """
    Compress target and logs folders into a zip file and upload to S3 bucket.

    Args:
        working_dir: The dbt working directory path
        dag_id: The DAG ID to use in the zip file name
        logical_date: The execution date in ISO format
    """
    try:
        # Convert logical_date string to datetime object
        execution_time = datetime.fromisoformat(logical_date).astimezone(pytz.UTC)
        formatted_date = execution_time.strftime("%Y%m%d_%H%M%S")

        # Create zip filename
        zip_filename = f"{dag_id}_{formatted_date}.zip"
        zip_filepath = os.path.join("/tmp", zip_filename)

        logging.info(f"Creating zip file: {zip_filepath}")
        
        # Go back one level in the working directory
        parent_dir = os.path.dirname(working_dir.rstrip('/'))
        logging.info(f"Using directory: {parent_dir} to find dbt artifact")

        # Create zip file
        with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Add target folder
            for artifact in ["build", "logs"]:
                artifact_dir = os.path.join(parent_dir, artifact)
                if os.path.exists(artifact_dir):
                    for root, dirs, files in os.walk(artifact_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, parent_dir)
                            zipf.write(file_path, arcname)
                            logging.debug(f"Added {file_path} to zip")
                else:
                    logging.warning(
                        f"{artifact} directory {artifact_dir} does not exist"
                    )

        # Upload to S3
        logging.info(f"Uploading {zip_filepath} to S3 bucket {dbt_artifact_bucket}")
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key"),
        )
        s3_key = f"dbt_artifacts/{dag_id}/{zip_filename}"
        s3_client.upload_file(zip_filepath, dbt_artifact_bucket, s3_key)

        # Clean up local zip file
        os.remove(zip_filepath)
        logging.info(
            f"Successfully uploaded artifact to s3://{dbt_artifact_bucket}/{s3_key}"
        )
        logging.info(
            f"Artifact URL: https://{dbt_artifact_bucket}.s3.amazonaws.com/{s3_key}"
        )

        return f"s3://{dbt_artifact_bucket}/{s3_key}"

    except Exception as e:
        error_message = f"Error compressing and uploading dbt artifacts: {str(e)}"
        logging.error(error_message)
        raise AirflowException(error_message)


def insert_jobs_table(
    mysql_connection,
    operator_id,
    status,
    execution_time_taken,
    job_created_at,
    job_updated_at,
    error_message=None,
    records_extracted=None,
    data_size=None,
):
    """Insert into the jobs table. Return job_id"""
    cursor = mysql_connection.cursor()  # Create a cursor for executing SQL queries
    job_id = str(uuid.uuid4())
    config_type = "sync"

    job_insert_query = f"""
        INSERT INTO {JOBS_TABLE}
            (operator_id, job_id, config_type, status, records_extracted, records_loaded, 
            data_size, execution_time_taken, created_at, updated_at, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    job_insert_values = (
        operator_id,
        job_id,
        config_type,
        status,
        records_extracted,
        records_extracted,
        data_size,
        execution_time_taken,
        job_created_at,
        job_updated_at,
        error_message
    )
    cursor.execute(job_insert_query, job_insert_values)
    mysql_connection.commit()
    inserted_id = cursor.lastrowid
    task_logger.info(f"values inserted in JOBS Table with id {inserted_id}")
    
    return job_id


def insert_job_detail_table(
    mysql_connection,
    job_id,
    stream_name,
    records_extracted,
    data_size,
    job_created_at,
    status="succeeded",
    error_message=None,
):
    cursor = mysql_connection.cursor()  # Create a cursor for executing SQL queries
    created_at = ended_at = job_created_at

    attempt_insert_query = f"""
        INSERT INTO {JOB_DETAIL_TABLE}
            (job_id, attempt_id, status, stream_name, records_extracted, records_loaded,
            data_size, created_at, ended_at, failure_summary)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    attempt_insert_values = (
        job_id,
        1,
        status,
        stream_name,
        records_extracted,
        records_extracted,
        data_size,
        created_at,
        ended_at,
        error_message
    )
    cursor.execute(attempt_insert_query, attempt_insert_values)
    mysql_connection.commit()
    job_detail_id = cursor.lastrowid
    task_logger.info(f"values inserted in JOB_DETAIL Table with id {job_detail_id}")

    return job_detail_id


def insert_data_source_item_table(
    mysql_connection,
    job_id,
    job_detail_id,
    data_source_id,
    s3_path,
    status,
    records_extracted,
    job_created_at,
):
    cursor = mysql_connection.cursor()  # Create a cursor for executing SQL queries
    created_at = ended_at = job_created_at
    transaction_date = date.today()

    data_source_insert_query = f"""
        INSERT INTO {DATA_SOURCE_ITEM_TABLE}
            (job_id, job_detail_id, data_source_id, path, transaction_date, records_extracted, 
            status, created_at, last_updated_at)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    data_source_values = (
        job_id,
        job_detail_id,
        data_source_id,
        s3_path,
        transaction_date,
        records_extracted,
        status,
        created_at,
        ended_at
    )
    cursor.execute(data_source_insert_query, data_source_values)
    mysql_connection.commit()
    data_source_item_id = cursor.lastrowid
    task_logger.info(
        f"values inserted in DATA_SOURCE_ITEM Table with id {data_source_item_id}"
    )
