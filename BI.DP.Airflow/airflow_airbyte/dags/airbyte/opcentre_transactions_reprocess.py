import os
import sys
import datetime
import json
import requests
from requests.auth import HTTPBasicAuth
import logging
from airflow.exceptions import AirflowFailException

sys.path.insert(1, "dags/airbyte")
from fetch_connection_list import (
    fetch_recovery_connection_operator_table,
    fetch_connid_oppid_by_platform,
)
from db_connection import mysql_conn

task_logger = logging.getLogger("airflow.task")
AIRBYTE_SERVER = os.getenv("airbyte_server")


def update_jobs(ti, platform_id, task_name):
    try:
        # Retrieve job configuration and details from environment variables
        jobs = os.getenv("jobs")
        job_detail = os.getenv("job_detail")

        connection = mysql_conn()  # Establish MySQL connection
        cursor = connection.cursor()  # Create a cursor for executing SQL queries
        print(task_name)

        # Pull job start date from XCom for the current task run
        job_and_configs_startdate = ti.xcom_pull(key="job_startdate", task_ids=task_name)
        task_logger.info(f"job_and_configs_startdate ---> {job_and_configs_startdate}")

        # Initialize empty lists to store job IDs, config IDs, and start dates
        job_ids = []
        config_ids = []
        recovery_dates_list = []

        # Split the job start date string by "/" to extract individual parts
        parts = job_and_configs_startdate.split("/")

        if len(parts) == 3:
            job_ids.append(int(parts[0])) # Append job ID
            config_ids.append(parts[1]) # Append config ID
            recovery_dates_list.append(parts[2]) # Append start date
        else:
            task_logger.info(f"Invalid format: {parts}")
        # Fetch the operator_id for the given platform_id from the database
        opp = fetch_recovery_connection_operator_table(platform_id)
        opp_list = [item[0] for config_id in config_ids for item in opp if item[3] == config_id]

        task_logger.info(f"jobs_ids ---> {job_ids}")
        task_logger.info(f"config_ids --->{config_ids}")
        task_logger.info(f"recovery_dates --->{recovery_dates_list}")

        recovery_dates = []

        for item in recovery_dates_list:
            streams = item.split(", ")

            curr_stream = [s for s in streams]
            recovery_dates.append(curr_stream)

        task_logger.info(f"recovery_dates-->{recovery_dates})")
        task_logger.info(f"jobs_ids ---> {job_ids}")

        # looping through job_ids and opp_list
        for ids, opp, dates in zip(job_ids, opp_list, recovery_dates):
            # Prepare the request to fetch job details from Airbyte's API

            auth = HTTPBasicAuth("airbyte", "password")
            url = f"{AIRBYTE_SERVER}api/v1/jobs/get"
            headers = {"accept": "application/json", "content-type": "application/json"}
            payload = {"id": ids}
            response = requests.post(url=url, headers=headers, auth=auth, json=payload)
            
            # Parse the response JSON from Airbyte API
            response_json = response.json()

            # Extract job details from the response
            job_id = response_json["job"]["id"]
            connection_id = response_json["job"]["configId"]
            configType = response_json["job"]["configType"]
            job_created_at = datetime.datetime.fromtimestamp(response_json["job"]["createdAt"])
            job_updated_at = datetime.datetime.fromtimestamp(response_json["job"]["updatedAt"])
            status = response_json["job"]["status"]
            task_logger.info(f"config_id ---> {connection_id}")

            last_attempt = response_json["attempts"][-1]["attempt"]

            # Extract statistics from the job attempt, or default to zero if the data is missing

            if "totalStats" not in last_attempt:
                records_extracted = 0
                records_loaded = 0
                data_size = 0

            else:
                if "recordsEmitted" not in last_attempt["totalStats"]:
                    records_extracted = 0
                else:
                    records_extracted = last_attempt["totalStats"]["recordsEmitted"]

                if "recordsCommitted" not in last_attempt["totalStats"]:
                    records_loaded = 0
                else:
                    records_loaded = last_attempt["totalStats"]["recordsCommitted"]

                if "bytesEmitted" not in last_attempt["totalStats"]:
                    data_size = 0
                else:
                    data_size = last_attempt["totalStats"]["bytesEmitted"]

                if "createdAt" in last_attempt:
                    start_time = datetime.datetime.fromtimestamp(last_attempt["createdAt"])
                else:
                    start_time = datetime.datetime.now()

                if "endedAt" in last_attempt:
                    end_time = datetime.datetime.fromtimestamp(last_attempt["endedAt"])
                else:
                    end_time = datetime.datetime.now()
                execution_time_taken = end_time - start_time

                failure_origin = ""
                error_message = ""
            # Extract failure details if available                
                if "failureSummary" in last_attempt:
                    if "failures" in last_attempt["failureSummary"]:
                        failure_data = last_attempt["failureSummary"]["failures"][0]
                        if "failureOrigin" in failure_data:
                            failure_origin = failure_data["failureOrigin"]
                        if "externalMessage" in failure_data:
                            error_message = failure_data["externalMessage"]

                job_insert_query = f"""
                    INSERT INTO {jobs}
                        (operator_id, job_id, config_type, status, records_extracted, records_loaded, data_size,
                        execution_time_taken, created_at, updated_at, attempt_started, attempt_ended, failure_origin,
                        error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                job_insert_values = (
                    opp, job_id, configType, status, records_extracted, records_loaded, data_size, execution_time_taken,
                    job_created_at, job_updated_at, start_time, end_time, failure_origin, error_message
                )
                cursor.execute(job_insert_query, job_insert_values)
                task_logger.info("values inserted in JOBS Table")

                stream_names = []
                if len(response_json["attempts"]) != 0:
                    for attempt in response_json["attempts"]:
                        if "streamStats" in attempt["attempt"]:
                            if len(attempt["attempt"]["streamStats"]) != 0:
                                for stream in attempt["attempt"]["streamStats"]:
                                    stream_names.append(stream["streamName"])
                            else:
                                pass

                task_logger.info(f"streams are --->{stream_names}")
                streams_dict = {k: "" for k in stream_names}
                for stream in dates:
                    for d in stream.split(", "):
                        stream_name, rec_dts = d.split(" :")
                        stream_name = stream_name[1:-1]  # Removing the quotes
                        streams_dict[stream_name] = rec_dts

                all_stream_dates = [streams_dict[k] for k in stream_names]

                if len(response_json["attempts"]) != 0:
                    for attempt in response_json["attempts"]:
                        if "streamStats" in attempt["attempt"]:
                            if len(attempt["attempt"]["streamStats"]) != 0:
                                for stream, date in zip(attempt["attempt"]["streamStats"], all_stream_dates):

                                    recovery_dates = ", ".join(date)

                                    if "totalStats" not in attempt["attempt"]:
                                        attempt_records_extracted = 0
                                        attempt_records_loaded = 0
                                        attempt_data_size = 0
                                    else:
                                        if "recordsEmitted" not in stream["stats"]:
                                            attempt_records_extracted = 0
                                        else:
                                            attempt_records_extracted = stream["stats"]["recordsEmitted"]

                                        if "recordsCommitted" not in stream["stats"]:
                                            attempt_records_loaded = 0
                                        else:
                                            attempt_records_loaded = stream["stats"]["recordsCommitted"]

                                        if "bytesEmitted" not in stream["stats"]:
                                            attempt_data_size = 0
                                        else:
                                            attempt_data_size = stream["stats"]["bytesEmitted"]

                                    try:
                                        attempt_start_time = (
                                            datetime.datetime.fromtimestamp(attempt["attempt"]["createdAt"])
                                        )
                                    except:
                                        attempt_start_time = datetime.datetime.now()

                                    try:
                                        attempt_end_time = (
                                            datetime.datetime.fromtimestamp(attempt["attempt"]["endedAt"])
                                        )
                                    except:
                                        attempt_end_time = datetime.datetime.now()

                                    if "failureSummary" not in attempt:
                                        attempt_failure_summary = ""
                                    else:
                                        attempt_failure_summary = json.dumps(attempt["attempt"]["failureSummary"])

                                    # Insert into JOB_DETAIL
                                    attempt_insert_query = f"""
                                        INSERT INTO {job_detail}
                                            (job_id, attempt_id, status,stream_name, records_extracted, records_loaded,
                                            data_size, created_at, ended_at, failure_summary, sync_window, is_recovery)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                    """
                                    attempt_insert_values = (
                                        job_id, attempt["attempt"]["id"], attempt["attempt"]["status"],
                                        stream["streamName"], attempt_records_extracted, attempt_records_loaded,
                                        attempt_data_size, attempt_start_time, attempt_end_time,
                                        attempt_failure_summary, date, 1
                                    )
                                    cursor.execute(attempt_insert_query, attempt_insert_values)

                                    task_logger.info("values inserted in JOB_DETAIL Table")

                        else:
                            for stream in response_json["job"]["enabledStreams"]:
                                attempt_records_extracted = 0
                                attempt_records_loaded = 0
                                attempt_data_size = 0

                                try:
                                    attempt_start_time = (
                                        datetime.datetime.fromtimestamp(attempt["attempt"]["createdAt"])
                                    )
                                except:
                                    attempt_start_time = datetime.datetime.now()

                                try:
                                    attempt_end_time = datetime.datetime.fromtimestamp(attempt["attempt"]["endedAt"])
                                except:
                                    attempt_end_time = datetime.datetime.now()

                                if "failureSummary" not in attempt:
                                    attempt_failure_summary = ""
                                else:
                                    attempt_failure_summary = json.dumps(attempt["attempt"]["failureSummary"])
                                    
                                # Insert attempt details into JOB_DETAIL table for failed job attempts                                    
                                attempt_insert_query = f"""
                                    INSERT INTO {job_detail}
                                        (job_id, attempt_id, status, stream_name, records_extracted, records_loaded,
                                        data_size, created_at, ended_at, failure_summary, is_recovery)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                """
                                attempt_insert_values = (
                                    job_id, attempt["attempt"]["id"], attempt["attempt"]["status"],
                                    stream["name"], attempt_records_extracted,
                                    attempt_records_loaded, attempt_data_size, attempt_start_time,
                                    attempt_end_time, attempt_failure_summary, 1
                                )
                                cursor.execute(attempt_insert_query, attempt_insert_values)

                                task_logger.info("values inserted in job_attempts of failed job")

                connection.commit()
        connection.close()
    except Exception as e:
        raise AirflowFailException(f"Error {e} .")
    finally:
        connection.close()
