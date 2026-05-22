import datetime
import json
import logging
import os
import sys

import requests
from airflow.exceptions import AirflowException
from requests.auth import HTTPBasicAuth

sys.path.insert(1, "dags/airbyte")

from db_connection import mysql_conn
from fetch_connection_list import fetch_connection_data_source_table

task_logger = logging.getLogger("airflow.task")
AIRBYTE_SERVER = os.getenv("airbyte_server")


def update_airbyte_jobs(ti, platform, airbyte_trigger_task_name, operator_id):
    try:
        JOBS = os.getenv("jobs")
        JOB_DETAIL = os.getenv("job_detail")
        DATA_SOURCE_ITEM = os.getenv("data_source_item")
        connection = mysql_conn()  # Establish MySQL connection
        cursor = connection.cursor()  # Create a cursor for executing SQL queries

        job_id = ti.xcom_pull(key="job_id", task_ids=airbyte_trigger_task_name)

        auth = HTTPBasicAuth(
            "airbyte", "password"
        )  # Basic authentication for the Airbyte API
        url = f"{AIRBYTE_SERVER}api/v1/jobs/get"
        headers = {"accept": "application/json", "content-type": "application/json"}
        payload = {"id": job_id}

        task_logger.info(f"Retrieving job information for job_id: {job_id}")
        response = requests.post(url=url, headers=headers, auth=auth, json=payload)

        # Parse the response JSON from Airbyte API
        response_json = response.json()
        

        # Extract job details from the response
        job_id = response_json["job"]["id"]
        config_type = response_json["job"]["configType"]
        job_created_at = datetime.datetime.fromtimestamp(
            response_json["job"]["createdAt"]
        )
        job_updated_at = datetime.datetime.fromtimestamp(
            response_json["job"]["updatedAt"]
        )
        status = response_json["job"]["status"]
        last_attempt = response_json["attempts"][-1]["attempt"]

        # Extract statistics from the job attempt, or default to zero if the data is missing
        if "totalStats" not in last_attempt:
            records_extracted = 0
            records_loaded = 0
            data_size = 0
        else:
            records_extracted = last_attempt["totalStats"].get("recordsEmitted", 0)
            records_loaded = last_attempt["totalStats"].get("recordsCommitted", 0)
            data_size = last_attempt["totalStats"].get("bytesEmitted", 0)

        start_time = datetime.datetime.fromtimestamp(
            last_attempt.get("createdAt", datetime.datetime.now().timestamp())
        )
        end_time = datetime.datetime.fromtimestamp(
            last_attempt.get("endedAt", datetime.datetime.now().timestamp())
        )

        execution_time_taken = end_time - start_time

        failure_origin = ""
        error_message = ""
        # Extract failure details if available
        if "failureSummary" in last_attempt:
            if "failures" in last_attempt["failureSummary"]:
                failure_data = last_attempt["failureSummary"]["failures"][0]
                failure_origin = failure_data.get("failureOrigin", "")
                error_message = failure_data.get("externalMessage", "")

        # SQL query to insert the job details into the jobs table
        job_insert_query = f"""
            INSERT INTO {JOBS}
                (operator_id, job_id, config_type, status, records_extracted, records_loaded, data_size,
                execution_time_taken, created_at, updated_at, attempt_started, attempt_ended, failure_origin,
                error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        job_insert_values = (
            operator_id,
            job_id,
            config_type,
            status,
            records_extracted,
            records_loaded,
            data_size,
            execution_time_taken,
            job_created_at,
            job_updated_at,
            start_time,
            end_time,
            failure_origin,
            error_message,
        )
        cursor.execute(job_insert_query, job_insert_values)
        task_logger.info("values inserted in JOBS Table")

        # Get data source ID from data source table
        data_source_tables = fetch_connection_data_source_table(platform, operator_id)

        # Loop through each attempt for the job and insert the stream stats and other details
        if len(response_json["attempts"]) != 0:
            for attempt in response_json["attempts"]:
                if "streamStats" in attempt["attempt"]:
                    if len(attempt["attempt"]["streamStats"]) != 0:
                        for stream in attempt["attempt"]["streamStats"]:
                            if "totalStats" not in attempt["attempt"]:
                                task_logger.info("no totalstats")
                                attempt_records_extracted = 0
                                attempt_records_loaded = 0
                                attempt_data_size = 0
                            else:
                                if "recordsEmitted" not in stream["stats"]:
                                    attempt_records_extracted = 0
                                else:
                                    attempt_records_extracted = stream["stats"][
                                        "recordsEmitted"
                                    ]

                                if "recordsCommitted" not in stream["stats"]:
                                    attempt_records_loaded = 0
                                else:
                                    attempt_records_loaded = stream["stats"][
                                        "recordsCommitted"
                                    ]

                                if "bytesEmitted" not in stream["stats"]:
                                    attempt_data_size = 0
                                else:
                                    attempt_data_size = stream["stats"]["bytesEmitted"]

                            try:
                                attempt_start_time = datetime.datetime.fromtimestamp(
                                    attempt["attempt"]["createdAt"]
                                )
                            except:
                                attempt_start_time = datetime.datetime.now()

                            try:
                                attempt_end_time = datetime.datetime.fromtimestamp(
                                    attempt["attempt"]["endedAt"]
                                )
                            except:
                                attempt_end_time = datetime.datetime.now()

                            if "failureSummary" not in attempt:
                                attempt_failure_summary = ""
                            else:
                                attempt_failure_summary = json.dumps(
                                    attempt["attempt"]["failureSummary"]
                                )

                            # Insert attempt details into JOB_DETAIL table
                            attempt_insert_query = f"""
                                INSERT INTO {JOB_DETAIL}
                                    (job_id, attempt_id, status, stream_name, records_extracted, records_loaded,
                                    data_size, created_at, ended_at, failure_summary, sync_window, is_recovery)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            attempt_insert_values = (
                                job_id,
                                attempt["attempt"]["id"],
                                attempt["attempt"]["status"],
                                stream["streamName"],
                                attempt_records_extracted,
                                attempt_records_loaded,
                                attempt_data_size,
                                attempt_start_time,
                                attempt_end_time,
                                attempt_failure_summary,
                                datetime.datetime.now().strftime("%Y-%m-%d"),
                                0,
                            )
                            cursor.execute(attempt_insert_query, attempt_insert_values)
                            # Get the last inserted ID
                            job_detail_id = cursor.lastrowid
                            task_logger.info(
                                f"Values inserted in JOB_DETAIL table with ID: {job_detail_id}"
                            )
                            
                            # look for the data_source_id with the same stream name
                            data_source_id = None
                            for data_source in data_source_tables:
                                if data_source[2] == stream["streamName"]:  # index 2 is source_name
                                    data_source_id = data_source[0]  # index 0 is id
                                    break

                            # insert data source item
                            data_source_insert_query = f"""
                                INSERT INTO {DATA_SOURCE_ITEM}
                                    (job_id, job_detail_id, data_source_id, item_name, path, transaction_date, records_extracted, 
                                    status, created_at, last_updated_at)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            data_source_insert_values = (
                                job_id,
                                job_detail_id,
                                data_source_id,
                                stream["streamName"],
                                None,
                                datetime.datetime.now().strftime("%Y-%m-%d"),
                                0,
                                "Complete",
                                datetime.datetime.now(),
                                datetime.datetime.now(),
                            )
                            cursor.execute(
                                data_source_insert_query, data_source_insert_values
                            )
                            data_source_item_id = cursor.lastrowid
                            task_logger.info(
                                f"Values inserted in data_source_item ID: {data_source_item_id}"
                            )
                    else:
                        for stream in response_json["job"]["enabledStreams"]:
                            attempt_records_extracted = 0
                            attempt_records_loaded = 0
                            attempt_data_size = 0
                            try:
                                attempt_start_time = datetime.datetime.fromtimestamp(
                                    attempt["attempt"]["createdAt"]
                                )
                            except:
                                attempt_start_time = datetime.datetime.now()

                            try:
                                attempt_end_time = datetime.datetime.fromtimestamp(
                                    attempt["attempt"]["endedAt"]
                                )
                            except:
                                attempt_end_time = datetime.datetime.now()

                            if "failureSummary" not in attempt:
                                attempt_failure_summary = ""
                            else:
                                attempt_failure_summary = json.dumps(
                                    attempt["attempt"]["failureSummary"]
                                )

                            # Insert attempt details into JOB_DETAIL table for failed job attempts
                            attempt_insert_query = f"""
                                INSERT INTO {JOB_DETAIL}
                                    (job_id, attempt_id, status,stream_name, records_extracted, records_loaded, 
                                    data_size, created_at, ended_at, failure_summary, sync_window, is_recovery)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            attempt_insert_values = (
                                job_id,
                                attempt["attempt"]["id"],
                                attempt["attempt"]["status"],
                                stream["name"],
                                attempt_records_extracted,
                                attempt_records_loaded,
                                attempt_data_size,
                                attempt_start_time,
                                attempt_end_time,
                                attempt_failure_summary,
                                datetime.datetime.now().strftime("%Y-%m-%d"),
                                0,
                            )
                            cursor.execute(attempt_insert_query, attempt_insert_values)
                            job_detail_id = cursor.lastrowid
                            task_logger.info(
                                f"Values inserted in job_attempts of failed job with ID: {job_detail_id}"
                            )

                            # look for the data_source_id with the same stream name
                            data_source_id = None
                            for data_source in data_source_tables:
                                if data_source[2] == stream["name"]:  # index 2 is source_name
                                    data_source_id = data_source[0]  # index 0 is id
                                    break

                            data_source_insert_query = f"""
                                INSERT INTO {DATA_SOURCE_ITEM}
                                    (job_id, job_detail_id, data_source_id, item_name, path, transaction_date, records_extracted, 
                                    status, created_at, last_updated_at)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            data_source_insert_values = (
                                job_id,
                                job_detail_id,
                                data_source_id,
                                stream["streamName"],
                                None,
                                datetime.datetime.now().strftime("%Y-%m-%d"),
                                0,
                                "Complete",
                                datetime.datetime.now(),
                                datetime.datetime.now(),
                            )
                            cursor.execute(
                                data_source_insert_query, data_source_insert_values
                            )
                            data_source_item_id = cursor.lastrowid
                            task_logger.info(
                                f"Values inserted in data_source_item ID: {data_source_item_id}"
                            )

        connection.commit()
        connection.close()
    except Exception as e:
        raise AirflowException(f"Error {e}.")
