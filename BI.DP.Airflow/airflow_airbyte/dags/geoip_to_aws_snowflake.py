import logging
import os
import shutil
import zipfile
from datetime import datetime, timedelta, date
import uuid
import csv
import boto3
import requests
from airbyte import constants
from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.env_config import output_bucket,aws_access_key_id,aws_secret_access_key
from airbyte.utilities import insert_data_source_item_table, insert_job_detail_table, insert_jobs_table
from airbyte.db_connection import mysql_conn
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Connection, Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.utils.task_group import TaskGroup
from sqlalchemy.orm.session import Session
from airbyte.fetch_connection_list import (
    fetch_connid_oppid_by_platform,
    fetch_platform_id_from_platform,
    fetch_data_source_id_using_platform_and_name
)

task_logger = logging.getLogger("airflow.task")

ENDPOINTS = {
    "country": "/geoip/databases/GeoIP2-Country-CSV/download",
    "city": "/geoip/databases/GeoIP2-City-CSV/download",
}

# list of files to upload
files_to_upload = [
    "GeoIP2-City-Blocks-IPv4.csv",
    "GeoIP2-City-Blocks-IPv6.csv",
    "GeoIP2-City-Locations-en.csv",
    "GeoIP2-Country-Blocks-IPv4.csv",
    "GeoIP2-Country-Blocks-IPv6.csv",
    "GeoIP2-Country-Locations-en.csv",
]
platform = constants.Maxmind

def download_geoip_file_and_upload_to_s3(**context):
    """Download GeoIP2 CSV file from MaxMind"""
    ip_type = context["ip_type"]
    conn = BaseHook.get_connection("maxmind_geoip2")
    base_url = conn.host
    endpoint = ENDPOINTS.get(ip_type)
    mysql_connection = mysql_conn()
    platform_id = fetch_platform_id_from_platform(platform)
    result = fetch_connid_oppid_by_platform(platform_id)
    operator_id = result[0][3]

    # Temporary directory and file path
    local_zip_file = f"/tmp/geoip2_{ip_type}_csv.zip"

    try:
        job_created_at = job_updated_at = datetime.now() 
        task_logger.info(
            f"Starting download of GeoIP2 {ip_type.capitalize()} CSV from MaxMind"
        )

        # Construct full URL
        url = f"{base_url}{endpoint}"

        # Download the file
        response = requests.get(
            url,
            auth=(conn.login, conn.password),
            params={"suffix": "zip"},
            headers={"Accept": "application/zip"},
            stream=True,
            timeout=600,
        )
        response.raise_for_status()

        with open(local_zip_file, "wb") as f:
            downloaded_size = 0
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded_size += len(chunk)
                if downloaded_size % (10 * 1024 * 1024) == 0:
                    task_logger.info(
                        f"Downloaded {downloaded_size / (1024 * 1024):.2f} MB"
                    )

        file_size = os.path.getsize(local_zip_file)
        task_logger.info(
            f"Download completed {local_zip_file}. File size: {file_size / (1024 * 1024):.2f} MB"
        )
        job_id = insert_jobs_table(
            mysql_connection,
            operator_id,
            "succeeded",
            file_size,
            job_created_at,
            job_updated_at,
        )

    except Exception as e:
        job_id = insert_jobs_table(
            mysql_connection,
            operator_id,
            "failed",
            file_size,
            job_created_at,
            job_updated_at,
        )
        task_logger.error(f"Download failed for {ip_type}: {str(e)}")
        mysql_connection.close()
        raise

    # Temporary directory for extraction
    temp_dir = f"/tmp/geoip2_{ip_type}"

    try:
        # Clean up directory if it exists
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        os.makedirs(temp_dir, exist_ok=True)
        task_logger.info(f"Unzipping file {local_zip_file} to {temp_dir}")

        # Unzip the file
        with zipfile.ZipFile(local_zip_file, "r") as zip_ref:
            zip_info_list = zip_ref.infolist()
            total_files = len(zip_info_list)
            task_logger.info(f"Found {total_files} files in the zip archive")

            # Extract files directly to temp_dir without preserving directory structure
            for zip_info in zip_info_list:
                if not zip_info.filename.endswith("/"):
                    # Get just the filename without directory structure
                    filename = os.path.basename(zip_info.filename)
                    # Extract to temp_dir with just the filename
                    zip_ref.extract(zip_info.filename, temp_dir)

                    # If we need to flatten the structure, move files from nested dirs to temp_dir
                    source_path = os.path.join(temp_dir, zip_info.filename)
                    if os.path.dirname(source_path) != temp_dir:
                        dest_path = os.path.join(temp_dir, filename)
                        if os.path.exists(source_path):
                            shutil.move(source_path, dest_path)

        # List extracted files for logging
        extracted_files = []
        total_size = 0
        for root, _, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                total_size += file_size
                extracted_files.append(file_path)

        task_logger.info(
            f"Extraction completed. {len(extracted_files)} files extracted: {', '.join(extracted_files)}. Total size: {total_size / (1024 * 1024):.2f} MB"
        )

    except Exception as e:
        task_logger.error(f"Unzip operation failed: {str(e)}")
        mysql_connection.close()
        raise

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        uploaded_files = []
        total_uploaded_size = 0

        # Upload each extracted file to S3
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.endswith(".csv") and file in files_to_upload:
                    local_file_path = os.path.join(root, file)
                    file_size = os.path.getsize(local_file_path)
                    # Get number of rows in the CSV file
                    with open(local_file_path, 'r') as file_obj:
                        reader = csv.reader(file_obj)
                        row_count = sum(1 for _ in reader) - 1 # excluding header
                        
                    task_logger.info(f"File {file} contains {row_count} rows")

                    # Construct S3 key, preserving directory structure
                    relative_path = os.path.relpath(local_file_path, temp_dir)
                    s3_key = f"Maxmind/geoip2/{relative_path}"

                    task_logger.info(
                        f"Uploading {local_file_path} to s3://{output_bucket}/{s3_key}"
                    )
                    s3_client.upload_file(
                        local_file_path, output_bucket, s3_key
                    )

                    uploaded_files.append(s3_key)
                    total_uploaded_size += file_size

                    stream_name = file.split('.')[0]
                    task_logger.info(f"stream_name: {stream_name}")
                    data_source_id = fetch_data_source_id_using_platform_and_name(platform, stream_name)

                    job_detail_id = insert_job_detail_table(
                        mysql_connection,
                        job_id,
                        stream_name,
                        row_count,  # records_extracted
                        file_size,  # data_size
                        job_created_at,
                        "succeeded"  # status
                    )

                    insert_data_source_item_table(
                        mysql_connection,
                        job_id,
                        job_detail_id,
                        data_source_id,
                        s3_key,
                        "Succeeded",
                        row_count,
                        job_created_at
                    )

        task_logger.info(
            f"Upload completed. {len(uploaded_files)} files uploaded. Total size: {total_uploaded_size / (1024 * 1024):.2f} MB"
        )

        # Remove extracted files
        if os.path.exists(temp_dir):
            task_logger.info(f"Cleaning up extraction directory: {temp_dir}")
            shutil.rmtree(temp_dir)

        # Remove zip file
        if os.path.exists(local_zip_file):
            task_logger.info(f"Removing zip file: {local_zip_file}")
            os.remove(local_zip_file)

        task_logger.info("Cleanup completed successfully")
        mysql_connection.close()
        return True

    except Exception as e:
        task_logger.error(f"S3 upload failed: {str(e)}")
        mysql_connection.close()
        raise


# Define the DAG
with DAG(
    "geoip_to_aws_snowflake",
    default_args={
        "owner": "airflow",
        "on_failure_callback": task_id_slack_failure_alert,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "priority_weight": 5,
    },
    description="Download GeoIP2 Country and City CSV zip files, unzip, and upload to S3",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["geoip2"],
) as dag:
    start_task = DummyOperator(task_id="start_task", dag=dag)

    # Create tasks for each endpoint/file type
    for ip_type, endpoint in ENDPOINTS.items():
        with TaskGroup(
            f"geoip2_{ip_type}_sync",
            tooltip=f"This task group performs data processing for geoip2 {ip_type} ",
        ) as process_data:
            download_task = PythonOperator(
                task_id="download_file",
                python_callable=download_geoip_file_and_upload_to_s3,
                provide_context=True,
                op_kwargs={"ip_type": ip_type},
            )

        # Set task dependencies for this file type
        download_task

    end_task = DummyOperator(task_id="end_task", dag=dag)

    start_task >> process_data >> end_task
