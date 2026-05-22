import os
import sys
import json
import requests
from requests.auth import HTTPBasicAuth
import datetime
from datetime import timedelta
import ast
import logging

from airflow.exceptions import AirflowException

sys.path.insert(1, "dags/airbyte")
from fetch_connection_list import (
    fetch_connection_operator_table,
    fetch_connection_operator_table_by_operator_id
)
from fetch_streams import get_inc_full_refresh_streams
from airbyte import constants
from db_connection import mysql_conn
from env_config import JOBS_TABLE, JOB_DETAIL_TABLE, AIRBYTE_SERVER

task_logger = logging.getLogger("airflow.task")

def update_jobs(ti, platform_id, platform, task_name):
    try:
        # Retrieve job configuration and details from environment variables
        jobs = os.getenv("jobs")
        job_detail = os.getenv("job_detail")
        connection = mysql_conn()  # Establish MySQL connection
        cursor = connection.cursor()  # Create a cursor for executing SQL queries

        # Pull job start date from XCom for the current task run
        job_and_configs_startdate = ti.xcom_pull(key="job_startdate", task_ids=task_name)

        # Initialize empty lists to store job IDs, config IDs, and start dates
        job_ids = []
        config_ids = []
        start_dates = []

        # Split the job start date string by "/" to extract individual parts
        parts = job_and_configs_startdate.split("/")
        if len(parts) == 3:
            job_ids.append(int(parts[0]))  # Append job ID
            config_ids.append(parts[1])  # Append config ID
            start_dates.append(parts[2])  # Append start date
        else:
            task_logger.warn(f"Invalid format: {parts}")

        # Fetch the operator_id for the given platform_id from the database
        opp = fetch_connection_operator_table(platform_id)

        # Create a list of operator_ids based on the config_ids
        opp_list = [item[0] for config_id in config_ids for item in opp if item[3] == config_id]

        # Log job IDs, config IDs, and start dates for debugging purposes
        task_logger.info(f"jobs_ids ---> {job_ids}")
        task_logger.info(f"config_ids --->{config_ids}")
        start_date_list = [ast.literal_eval(item) for item in start_dates]
        task_logger.info(f"startdate --->{start_date_list}")

        # Loop through the job IDs, operator list, and start dates
        for ids, opp, starts in zip(job_ids, opp_list, start_date_list):
            # Prepare the request to fetch job details from Airbyte's API
            auth = HTTPBasicAuth("airbyte", "password")  # Basic authentication for the Airbyte API
            url = f"{AIRBYTE_SERVER}api/v1/jobs/get"
            headers = {"accept": "application/json", "content-type": "application/json"}
            payload = {"id": ids}
            response = requests.post(url=url, headers=headers, auth=auth, json=payload)
            
            # Parse the response JSON from Airbyte API
            response_json = response.json()

            # Extract job details from the response
            job_id = response_json["job"]["id"]
            airbyte_connection_id = response_json["job"]["configId"]
            config_type = response_json["job"]["configType"]
            job_created_at = datetime.datetime.fromtimestamp(response_json["job"]["createdAt"])
            job_updated_at = datetime.datetime.fromtimestamp(response_json["job"]["updatedAt"])
            status = response_json["job"]["status"]
            last_attempt = response_json["attempts"][-1]["attempt"]

            # Extract stream names from the last job attempt
            stream_names = [stream["streamName"] for stream in last_attempt["streamStats"]]
            flattened_start_date_list = [item for sublist in start_date_list for item in sublist]
            datestream_dict = {item.split(":")[0]: item.split(":")[1] for item in flattened_start_date_list}
            
            # Initialize a list of dates for the streams
            dates_list = []
            for stream in stream_names:
                try:
                    # Match the start date for each stream based on the datestream dictionary
                    dates_list.append(datestream_dict[stream])
                except KeyError as e:
                    task_logger.info("KeyError ---->", str(e))
                    # If there's a KeyError, use the platform-specific start date or default to the operator's start date
                    start_date = datetime.datetime.today().strftime("%Y-%m-%d") if platform == constants.BRC or platform == constants.BRT else opp[6]
                    dates_list.append(start_date)

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
            # SQL query to insert the job details into the jobs table
            job_insert_query = f"""
                INSERT INTO {jobs}
                    (operator_id, job_id, job_execute_step, config_type, status, records_extracted, records_loaded, data_size,
                    execution_time_taken, created_at, updated_at, attempt_started, attempt_ended, failure_origin,
                    error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            job_insert_values = (opp, job_id, "S2", config_type, status, records_extracted, records_loaded, data_size,
                                 execution_time_taken, job_created_at, job_updated_at, start_time, end_time, 
                                 failure_origin, error_message)
            cursor.execute(job_insert_query, job_insert_values)
            task_logger.info("values inserted in JOBS Table")
            # Loop through each attempt for the job and insert the stream stats and other details

            if len(response_json["attempts"]) != 0:
                for attempt in response_json["attempts"]:
                    if "streamStats" in attempt["attempt"]:
                        if len(attempt["attempt"]["streamStats"]) != 0:
                            for stream, date in zip(attempt["attempt"]["streamStats"], dates_list):
                                if "totalStats" not in attempt["attempt"]:
                                    task_logger.info("no totalstats")
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
                                    attempt_start_time = datetime.datetime.fromtimestamp(
                                        attempt["attempt"]["createdAt"]
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

                                end_date = datetime.datetime.now()
                                start_date = datetime.datetime.strptime(date, "%Y-%m-%d")
                                
                                delta = timedelta(days=1)
                                dates = []
                                if platform not in (constants.NetRefer,constants.Referon ):
                                    pass
                                else:
                                    end_date -= delta

                                if end_date <= start_date:
                                    dates = [start_date.strftime("%Y-%m-%d")]
                                else:
                                    while start_date <= end_date:
                                        dates.append(start_date.strftime("%Y-%m-%d"))
                                        start_date += delta
                                task_logger.info(f"Dates list are ---> {dates}")

                                # Determine if the stream is a full refresh or incremental and set sync window accordingly
                                if platform in (constants.Voluum, constants.BRC, constants.BRT ):
                                    connection_id = airbyte_connection_id
                                    stream_name = stream["streamName"]
                                    task_logger.info(f"connection_id ---> {connection_id}")
                                    task_logger.info(f"type(connection_id) ---> {type(connection_id)}")
                                    
                                    _, full_refresh_streams = get_inc_full_refresh_streams(connection_id)
                                    
                                    if stream_name in full_refresh_streams:
                                        task_logger.info(f"full refresh ---> {stream_name}")
                                        dates_str = datetime.datetime.today().strftime("%Y-%m-%d")
                                    else:
                                        task_logger.info(f"inc_stream ----> {stream_name}")
                                        dates_str = ", ".join(dates)
                                else:
                                    task_logger.info("Inside the outer else block")
                                    dates_str = ", ".join(dates)
                                
                                # Insert attempt details into JOB_DETAIL table
                                attempt_insert_query = f"""
                                    INSERT INTO {job_detail}
                                        (job_id, attempt_id, status, stream_name, records_extracted, records_loaded,
                                        data_size, created_at, ended_at, failure_summary, sync_window, is_recovery)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                """
                                attempt_insert_values = (job_id, attempt["attempt"]["id"], attempt["attempt"]["status"],
                                                         stream["streamName"], attempt_records_extracted,
                                                         attempt_records_loaded, attempt_data_size, attempt_start_time,
                                                         attempt_end_time, attempt_failure_summary, dates_str, 0)
                                cursor.execute(attempt_insert_query, attempt_insert_values)
                                task_logger.info("values inserted in JOB_DETAIL Table")
                    else:
                        for stream, date in zip(response_json["job"]["enabledStreams"], dates_list):
                            attempt_records_extracted = 0
                            attempt_records_loaded = 0
                            attempt_data_size = 0
                            try:
                                attempt_start_time = datetime.datetime.fromtimestamp(attempt["attempt"]["createdAt"])
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

                            task_logger.info("type of date is {} and date is {}".format(type(date), date))
                            end_date = datetime.datetime.now()
                            start_date = datetime.datetime.strptime(date, "%Y-%m-%d")
                            
                            delta = timedelta(days=1)
                            dates = []
                            
                            if platform not in (constants.NetRefer,constants.Referon ):
                                while start_date <= end_date:
                                    dates.append(start_date.strftime("%Y-%m-%d"))
                                    start_date += delta
                            else:
                                while start_date < end_date:
                                    dates.append(start_date.strftime("%Y-%m-%d"))
                                    start_date += delta

                            # Determine if it's a full refresh or incremental stream
                            if platform in (constants.Voluum, constants.BRC, constants.BRT ):
                                connection_id = airbyte_connection_id
                                stream_name = stream["name"]  # --->stream name here
                                task_logger.info(f"connection_id ---> {connection_id}")
                                task_logger.info(f"type(connection_id) ---> {type(connection_id)}")
                                _, full_refresh_streams = get_inc_full_refresh_streams(connection_id)
                                
                                if stream_name in full_refresh_streams:
                                    task_logger.info(f"full refresh ---> {stream_name}")
                                    dates_str = datetime.datetime.today().strftime("%Y-%m-%d")
                                else:
                                    dates_str = ", ".join(dates)
                            else:
                                dates_str = ", ".join(dates)
                            task_logger.info(dates_str)
                            
                            # Insert attempt details into JOB_DETAIL table for failed job attempts
                            attempt_insert_query = f"""
                                INSERT INTO {job_detail}
                                    (job_id, attempt_id, status,stream_name, records_extracted, records_loaded, 
                                    data_size, created_at, ended_at, failure_summary, sync_window, is_recovery)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            attempt_insert_values = (job_id, attempt["attempt"]["id"], attempt["attempt"]["status"],
                                                     stream["name"], attempt_records_extracted, attempt_records_loaded,
                                                     attempt_data_size, attempt_start_time, attempt_end_time,
                                                     attempt_failure_summary, dates_str, 0)
                            cursor.execute(attempt_insert_query, attempt_insert_values)
                            task_logger.info("values inserted in job_attempts of failed job")

                connection.commit()
        connection.close()
    except Exception as e:
        raise AirflowException(f"Error {e} .")

def update_jobs_v2(ti, platform_id, platform, task_name, operator_id):
    try:
        # Retrieve job configuration and details from env_config
        connection = mysql_conn()  # Establish MySQL connection
        cursor = connection.cursor()  # Create a cursor for executing SQL queries

        # Pull job start date from XCom for the current task run
        job_and_configs_startdate = ti.xcom_pull(
            key="job_startdate", task_ids=task_name
        )

        # Split the job start date string by "/" to extract individual parts
        parts = job_and_configs_startdate.split("/")
        if len(parts) == 3:
            job_id = int(parts[0])
            config_ids = parts[1]
            start_dates = parts[2]  # Append start date
        else:
            task_logger.warn(f"Invalid format: {parts}")

        # Fetch the operator_id for the given platform_id from the database
        operator_table = fetch_connection_operator_table_by_operator_id(
            platform_id, operator_id
        )

        # Create a list of operator_ids that match the config_ids
        operator_id_list = []
        for config_id in config_ids:
            operator_id_list.append(operator_table[0][0])

        # Log job IDs, config IDs, and start dates for debugging purposes
        task_logger.info(f"Job ID ---> {job_id}")
        task_logger.info(f"config_ids ---> {config_ids}")

        cleaned_start_dates = start_dates.strip("[]").replace("'", "").split(", ")

        # Create dictionary with format {stream_name: start_date}
        start_date_dict = {
            stream.split(":")[0]: stream.split(":")[1] for stream in cleaned_start_dates
        }

        task_logger.info(f"Start dates ---> {start_date_dict}")

        # Prepare the request to fetch job details from Airbyte's API
        auth = HTTPBasicAuth(
            "airbyte", "password"
        )  # Basic authentication for the Airbyte API
        url = f"{AIRBYTE_SERVER}api/v1/jobs/get"
        headers = {"accept": "application/json", "content-type": "application/json"}
        payload = {"id": job_id}
        response = requests.post(url=url, headers=headers, auth=auth, json=payload)

        # Parse the response JSON from Airbyte API
        response_json = response.json()

        # Extract job details from the response
        job_id = response_json["job"]["id"]
        airbyte_connection_id = response_json["job"]["configId"]
        config_type = response_json["job"]["configType"]
        job_created_at = datetime.datetime.fromtimestamp(
            response_json["job"]["createdAt"]
        )
        job_updated_at = datetime.datetime.fromtimestamp(
            response_json["job"]["updatedAt"]
        )
        status = response_json["job"]["status"]
        last_attempt = response_json["attempts"][-1]["attempt"]

        # Extract stream names from the last job attempt
        stream_names = [stream["streamName"] for stream in last_attempt["streamStats"]]

        # Initialize a list of dates for the streams
        for stream in stream_names:
            if stream not in start_date_dict:
                if platform == constants.BRC or platform == constants.BRT:
                    start_date_dict[stream] = datetime.datetime.today().strftime(
                        "%Y-%m-%d"
                    )
                else:
                    start_date_dict[stream] = operator_table[0][6]

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
        # SQL query to insert the job details into the jobs table
        job_insert_query = f"""
            INSERT INTO {JOBS_TABLE}
                (operator_id, job_id, job_execute_step, config_type, status, records_extracted, records_loaded, data_size,
                execution_time_taken, created_at, updated_at, attempt_started, attempt_ended, failure_origin,
                error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        job_insert_values = (
            operator_id,
            job_id,
            "S2",
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
        # Loop through each attempt for the job and insert the stream stats and other details

        if len(response_json["attempts"]) != 0:
            for attempt in response_json["attempts"]:
                if "streamStats" in attempt["attempt"]:
                    if len(attempt["attempt"]["streamStats"]) != 0:
                        for stream in attempt["attempt"]["streamStats"]:
                            stream_name = stream["streamName"]
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

                            end_date = datetime.datetime.now()
                            start_date = datetime.datetime.strptime(
                                start_date_dict[stream_name], "%Y-%m-%d"
                            )

                            delta = timedelta(days=1)
                            dates = []
                            if platform not in (constants.NetRefer,constants.Referon):
                                pass
                            else:
                                end_date -= delta
                            
                            # Ensure end_date is not less than start_date, otherwise dates list will be empty
                            if end_date <= start_date:
                                dates = [start_date.strftime("%Y-%m-%d")]
                            else:
                                while start_date <= end_date:
                                    dates.append(start_date.strftime("%Y-%m-%d"))
                                    start_date += delta
                            task_logger.info(f"Dates list are ---> {dates}")

                            # Determine if the stream is a full refresh or incremental and set sync window accordingly
                            if platform in (constants.Voluum, constants.BRC, constants.BRT ):
                                task_logger.info(
                                    f"Connection ID ---> {airbyte_connection_id}"
                                )

                                _, full_refresh_streams = get_inc_full_refresh_streams(
                                    airbyte_connection_id
                                )

                                if stream_name in full_refresh_streams:
                                    task_logger.info(f"full refresh ---> {stream_name}")
                                    dates_str = datetime.datetime.today().strftime(
                                        "%Y-%m-%d"
                                    )
                                else:
                                    task_logger.info(f"inc_stream ----> {stream_name}")
                                    dates_str = ", ".join(dates)
                            else:
                                task_logger.info("Inside the outer else block")
                                dates_str = ", ".join(dates)

                            # Insert attempt details into JOB_DETAIL table
                            attempt_insert_query = f"""
                                INSERT INTO {JOB_DETAIL_TABLE}
                                    (job_id, 
                                    attempt_id, 
                                    status, 
                                    stream_name, 
                                    records_extracted, 
                                    records_loaded,
                                    data_size, 
                                    created_at, 
                                    ended_at, 
                                    failure_summary, 
                                    sync_window, 
                                    is_recovery)
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
                                dates_str,
                                0,
                            )
                            cursor.execute(attempt_insert_query, attempt_insert_values)
                            task_logger.info(
                                f"values inserted in JOB_DETAIL Table for {stream_name}"
                            )
                else:
                    task_logger.info("No streamStats found in the job")
                    for stream in response_json["job"]["enabledStreams"]:
                        stream_name = stream["name"]
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

                        end_date = datetime.datetime.now()
                        start_date = datetime.datetime.strptime(
                            start_date_dict[stream_name], "%Y-%m-%d"
                        )

                        delta = timedelta(days=1)
                        dates = []

                        if platform not in (constants.NetRefer,constants.Referon):
                            while start_date <= end_date:
                                dates.append(start_date.strftime("%Y-%m-%d"))
                                start_date += delta
                        else:
                            while start_date < end_date:
                                dates.append(start_date.strftime("%Y-%m-%d"))
                                start_date += delta

                        # Determine if it's a full refresh or incremental stream
                        if platform in (constants.Voluum, constants.BRC, constants.BRT ):
                            connection_id = airbyte_connection_id
                            task_logger.info(f"Connection ID ---> {connection_id}")
                            _, full_refresh_streams = get_inc_full_refresh_streams(
                                connection_id
                            )

                            if stream_name in full_refresh_streams:
                                task_logger.info(f"full refresh ---> {stream_name}")
                                dates_str = datetime.datetime.today().strftime(
                                    "%Y-%m-%d"
                                )
                            else:
                                dates_str = ", ".join(dates)
                        else:
                            dates_str = ", ".join(dates)
                        task_logger.info("Dates string ---> {dates_str}")

                        # Insert attempt details into JOB_DETAIL table for failed job attempts
                        attempt_insert_query = f"""
                            INSERT INTO {JOB_DETAIL_TABLE}
                                (job_id, 
                                attempt_id, 
                                status,stream_name, 
                                records_extracted, 
                                records_loaded, 
                                data_size, 
                                created_at, 
                                ended_at, 
                                failure_summary, 
                                sync_window, 
                                is_recovery)
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
                            dates_str,
                            0,
                        )
                        cursor.execute(attempt_insert_query, attempt_insert_values)
                        task_logger.info(
                            f"values inserted in job_attempts of failed job of {stream_name}"
                        )

            connection.commit()
        connection.close()
    except Exception as e:
        raise AirflowException(f"Error {e} .")
