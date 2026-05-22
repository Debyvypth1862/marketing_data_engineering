import os
import sys
import requests
from requests.auth import HTTPBasicAuth
import json
import logging
from datetime import datetime, timedelta
import time
import boto3
from dateutil.parser import isoparse
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException
import ast

sys.path.insert(1, "dags/airbyte")
from airbyte import constants
from fetch_streams import get_inc_full_refresh_streams
from airbyte.slack_alerts import (
    airflow_airbyte_sync_task_slack_alert,
    airflow_trigger_airbyte_sync_task_error_slack_alert,
)
from fetch_connection_list import (
    fetch_start_date_from_ACCOUNT,
    fetch_loopback_days_from_ACCOUNT,
    fetch_path_from_data_source,
    fetch_connection_operator_table,
)

from stream_state_payloads import StreamStatePayload
from Utils import Utils
from env_config import AIRBYTE_SERVER
from airbyte.db_connection import mysql_conn

status_ = None
task_logger = logging.getLogger("airflow.task")
# Initialize S3 client with credentials
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)

def move_object(source_bucket, source_key, destination_bucket, destination_key):
    try:
        # Copy the object to the destination bucket
        s3_client.copy_object(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
        )
        task_logger.info(
            f"Successfully copied {source_key} from {source_bucket} to {destination_key} in {destination_bucket}"
        )

        # Delete the object from the source bucket
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        task_logger.info(f"Successfully deleted {source_key} from {source_bucket}")

    except Exception as e:
        task_logger.info(f"Failed to move object: {e}")

def archive_all_files_in_s3_folder(bucket_name, path):
    try:
        """
        Deletes all files in the specified S3 bucket and folder path.
    
        Args:
            bucket_name (str): Name of the S3 bucket.
            path (str): Folder path in the S3 bucket.
    
        """
        bucket_path = os.getenv("Input_bucket_path")
        path_to_archive = bucket_path + path
        current_date_time = datetime.now().strftime("%Y-%m-%d_%I:%M:%S_%p")
        archive_path = f"{path}/{current_date_time}"
        
        # List all objects in the specified folder
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path_to_archive)

        # Check if there are any objects to delete
        if "Contents" in response:
            # Create a list of keys to delete
            files_to_archive = [obj["Key"] for obj in response["Contents"]]
            for file in files_to_archive:
                archive_file_key = f"{archive_path}/{file.split('/')[-1]}"
                task_logger.info(f"Archiving file: {file} to {archive_file_key}")
                move_object(
                    f"{bucket_name}",
                    file,
                    f"{os.getenv('Archive_bucket')}",
                    f"{os.getenv('Archive_bucket_path')}{archive_file_key}",
                )
            task_logger.info(f"Successfully archived all files in folder '{path}'")
        else:
            task_logger.info(f"No files found in folder '{path}' to archive.")
    except Exception as e:
        task_logger.info(f"Failed to archive files in folder '{path}': {e}")

def get_connection(connection_id):
    """
    Fetches details of a specific Airbyte connection using its ID.

    Args:
        connection_id (str): ID of the Airbyte connection.

    Returns:
        Response object from the API call.
    """
    airbyte_host = os.getenv("airbyte_server")
    endpoint = "api/v1/connections/get"
    url = airbyte_host + endpoint
    headers = {"accept": "application/json", "content-type": "application/json"}
    basic_auth = HTTPBasicAuth("airbyte", "password")
    payload = {"connectionId": connection_id}
    response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)
    return response


def get_state(connection_id):
    """
    Fetches the state of a specific Airbyte connection.

    Args:
        connection_id (str): ID of the Airbyte connection.

    Returns:
        JSON response containing the state of the connection.
    """
    airbyte_host = os.getenv("airbyte_server")
    endpoint = f"api/v1/state/get"
    base_url = airbyte_host + endpoint
    basic_auth = HTTPBasicAuth("airbyte", "password")
    headers = {"Content-Type": "application/json"}
    payload = {"connectionId": connection_id}
    response = requests.post(
        url=base_url, json=payload, headers=headers, auth=basic_auth
    )
    return response.json()


def set_state(connection_id):
    """
    Sets or updates the state of a specific Airbyte connection.

    Args:
        connection_id (str): ID of the Airbyte connection.

    """
    airbyte_host = os.getenv("airbyte_server")
    endpoint = "api/v1/state/create_or_update"
    base_url = airbyte_host + endpoint
    auth = HTTPBasicAuth("airbyte", "password")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    headers = {"Content-Type": "application/json"}
    request_body = {
        "connectionId": connection_id,
        "connectionState": {
            "connectionId": connection_id,
            "stateType": "stream",
            "streamState": [
                {
                    "streamDescriptor": {"name": "utm_code_report_stream"},
                    "streamState": {"end": end_date},
                }
            ],
        },
    }

    response = requests.post(
        url=base_url, json=request_body, headers=headers, auth=auth
    )
    if response.status_code == 200:
        task_logger.info(
            f"Successfully updated state for connection_id: {connection_id} to {end_date}"
        )
    else:
        task_logger.info(
            f"Error while updating state for connection_id: {connection_id}"
        )
        task_logger.info(f"Status code: {response.status_code}, Error: {response.text}")
    return None

def create_stream_state_payload(allresponse):
    state = {
    "connectionId": allresponse["connectionId"],
    "connectionState": {
        "stateType": "stream",
        "streamState": allresponse["streamState"]
    }
    }
    return state

def set_stream_state(connection_id, payload):
    """
    Sets or updates the stream state for a specific connection.

    Args:
        connection_id (str): ID of the Airbyte connection.
        payload (dict): Payload containing the stream state details.
    """
    airbyte_host = os.getenv("airbyte_server")
    endpoint = "api/v1/state/create_or_update"
    base_url = airbyte_host + endpoint
    auth = HTTPBasicAuth("airbyte", "password")
    headers = {"Content-Type": "application/json"}
    request_body = payload
    response = requests.post(
        url=base_url, json=request_body, headers=headers, auth=auth
    )
    if response.status_code == 200:
        task_logger.info(
            f"Successfully updated state for connection_id: {connection_id} "
        )
    else:
        task_logger.info(
            f"Error while updating state for connection_id: {connection_id}"
        )
        task_logger.info(f"Status code: {response.status_code}, Error: {response.text}")
    return None


def airbyte_api_connections_sync(ti, airbyte_connections, platform_id, platform, task_name, **kwargs):
    ti.xcom_push(key="platform_id", value=platform_id)
    ti.xcom_push(key="task_name", value=task_name)
    ti.xcom_push(key="job_execute_step", value="S1")
    
    result = fetch_path_from_data_source(airbyte_connections[0])
    bucket_name = os.getenv("Input_bucket")
    for path in result:
        task_logger.info(path)
        path = path[0]
        folder_path = os.path.dirname(path)
        archive_all_files_in_s3_folder(bucket_name, folder_path)

    _AIRBYTE_CONN = os.getenv("airbyte_conn")

    context = get_current_context()
    task_logger.info(f"context --> {context}")
    airbyte_connections = airbyte_connections[0]
    task_logger.info(f"airbyte_connections --> {airbyte_connections}")

    job_id = 0
    is_job_complete = False
    stream_state_info = []

    try:
        start_date = []
        allresponse = get_state(airbyte_connections)
        # Initialize least_date variable
        least_date_str = None

        if platform in (constants.Q,):
            date = "end"
        elif platform == constants.BRC or platform ==  constants.BRT:
            date = "cursor"
        elif platform == constants.Redtrack:
            date = "date_from"
        elif platform == constants.Apilayer:
            date = "start_date"
        else:
            date = "date"

        # Check if 'streamState' exists and has entries
        if (
            "streamState" in allresponse
            and isinstance(allresponse["streamState"], list)
            and allresponse["streamState"]
        ):
            for d in allresponse["streamState"]:
                stream_name = d["streamDescriptor"].get("name")
                stream_state = d["streamState"].get(date)

                if stream_state:
                    stream_state_info.append({stream_name: stream_state})

        if allresponse["stateType"] == "not_set":
            response = get_connection(airbyte_connections)
            response_data = response.json()
            stream_names = [
                stream["stream"]["name"]
                for stream in response_data["syncCatalog"]["streams"]
            ]
            task_logger.info(stream_names)

            for stream in stream_names:
                start_date_tuple = fetch_start_date_from_ACCOUNT(airbyte_connections)
                date = str(start_date_tuple[0][0])
                combined_datestream = stream + ":" + date
                start_date.append(combined_datestream)
                task_logger.info(start_date)
            stream_state = "Fullrefresh"
        else:
            loopback_days_tuple = fetch_loopback_days_from_ACCOUNT(airbyte_connections)
            loopback_days = (
                loopback_days_tuple[0][0]
                if loopback_days_tuple[0][0] or loopback_days_tuple[0][0] is not None
                else 0
            )
            if platform == constants.Smartico:
                loopback_days += 1
            if platform == constants.BRC or platform ==  constants.BRT:
                loopback_days = 0

            if platform in (constants.Voluum, constants.BRC, constants.BRT):
                incremental_streams, full_refresh_streams = (
                    get_inc_full_refresh_streams(airbyte_connections)
                )

                stream_states = []
                response_state = get_state(airbyte_connections)
                for item in response_state["streamState"]:
                    # added code for solving cursor_field for resolving for full refresh
                    if item["streamDescriptor"]["name"] in incremental_streams:
                        task_logger.info("inside if condition of incremental stream")
                        cursor_field = "cursor" if platform == constants.BRC or platform ==  constants.BRT else "date"
                        stream_name = item["streamDescriptor"]["name"]
                        try:
                            stream_state = item["streamState"][cursor_field]
                        except KeyError:
                            # Cursor field for voluum
                            stream_state = item["streamState"]["postbackTimestamp"]
                        stream_states.append((stream_state, stream_name))

                for stream in full_refresh_streams:
                    stream_states.append(
                        (datetime.today().strftime("%Y-%m-%d"), stream)
                    )
            else:
                dates_list = [
                    stream["streamState"][date] for stream in allresponse["streamState"]
                ]
                streams_list = [
                    stream["streamDescriptor"]["name"]
                    for stream in allresponse["streamState"]
                ]
                stream_states = zip(dates_list, streams_list)
                task_logger.info(dates_list)

            for date, stream in stream_states:
                task_logger.info(date)
                try:
                    if platform == constants.BRC or platform ==  constants.BRT:
                        try:
                            date_format_list = [
                                "%Y-%m-%dT%H:%M:%S.%f",
                                "%Y-%m-%dT%H:%M:%S.%fZ",
                                "%Y-%m-%dT%H:%M:%S",
                                "%Y-%m-%d",
                            ]
                            for format in date_format_list:
                                try:
                                    parsed_datetime = datetime.strptime(date, format)
                                    break
                                except ValueError:
                                    continue
                            else:
                                raise ValueError(
                                    f"Time data '{date}' does not match any of the expected formats"
                                )

                            # Extract the date
                            original_date = parsed_datetime.date()
                            task_logger.info(f"Original date: {original_date}")

                        except ValueError as e:
                            task_logger.error(e)

                        original_date = parsed_datetime.date()
                        combined_datestream = (
                            stream + ":" + original_date.strftime("%Y-%m-%d")
                        )
                        start_date.append(combined_datestream)
                    else:
                        original_date = datetime.strptime(date, "%Y-%m-%d")
                        modified_date = original_date - timedelta(days=loopback_days)
                        converted_date = modified_date.strftime("%Y-%m-%d")
                        combined_datestream = stream + ":" + converted_date
                        start_date.append(combined_datestream)
                except Exception as e:
                    formatted_date = datetime.strptime(date, "%Y%m%d").strftime(
                        "%Y-%m-%d"
                    )
                    combined_datestream = stream + ":" + formatted_date
                    start_date.append(combined_datestream)
            task_logger.info(start_date)
            stream_state = "Incremental"

        task_logger.info(
            f"Start syncing data for airbyte connection {airbyte_connections} ."
        )
        retries = 0
        max_retries = 5
        retry_delay = 10
        while retries < max_retries:
            trigger_sync = HttpOperator(
                task_id="start_airbyte_sync",
                http_conn_id=_AIRBYTE_CONN,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",  # Airbyte cloud requires that a user agent is defined
                    "Accept": "application/json",
                },
                endpoint=f"/api/v1/connections/sync",
                data=json.dumps({"connectionId": airbyte_connections}),
                response_filter=lambda response: {
                    "job_id": response.json()["job"]["id"],
                    "config_id": response.json()["job"]["configId"],
                },
            )
            try:
                response_values = trigger_sync.execute(context=context)
                job_id = response_values["job_id"]
                config_id = response_values["config_id"]
                break
            except Exception as e:
                error_msg = str(e)
                if (
                    "502" in error_msg
                    or "Bad Gateway" in error_msg
                    or "500" in error_msg
                    or "Internal Server Error" in error_msg
                ):
                    # Retry when we get status code as 502
                    retries += 1
                    time.sleep(retry_delay)
                    task_logger.info(
                        f"Received {error_msg} error on connection_sync for connection_id: {airbyte_connections}. Retrying {retries}/{max_retries}..."
                    )
                elif "409" in error_msg or "Conflict" in error_msg:
                    # Condition to check conflicts in airbyte sync
                    task_logger.info(
                        f"Conflict for connection_id: {airbyte_connections}"
                    )
                    # Function to get job_id and config_id for running sync
                    response = Utils.check_conflict(airbyte_connections)
                    job_id = response["job"]["id"]
                    config_id = response["job"]["configId"]
                    break  # Exit the loop once job_id and config_id are obtained
                else:
                    raise AirflowException(f"Error as {e}")
        if retries == max_retries:
            raise AirflowException(f"Max retries exceded")

        task_logger.info(
            f"Waiting job {job_id} of airbyte connection {airbyte_connections} complete."
        )

        retries = 0
        while retries < max_retries:
            wait_for_sync_to_complete = HttpSensor(
                method="POST",
                task_id="wait_for_airbyte_sync",
                http_conn_id=_AIRBYTE_CONN,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",
                    "Accept": "application/json",
                },
                request_params=json.dumps({"id": job_id}),
                endpoint="/api/v1/jobs/get",
                poke_interval=90,
                soft_fail=True,
                response_check=lambda response: airbyte_is_job_complete(response),
            )
            try:
                is_job_complete = wait_for_sync_to_complete.execute(context=context)
                break
            except Exception as e:
                error_msg = str(e)
                if (
                    "502" in error_msg
                    or "Bad Gateway" in error_msg
                    or "500" in error_msg
                    or "Internal Server Error" in error_msg
                ):
                    # Retry when we get status code as 502
                    retries += 1
                    time.sleep(retry_delay)
                    task_logger.info(
                        f"Received {error_msg} error on connection_sync for connection_id: {airbyte_connections}. Retrying {retries}/{max_retries}..."
                    )
                elif "SIGTERM" in error_msg:
                    retries += 1
                    time.sleep(retry_delay)
                    task_logger.info(
                        f"Received {error_msg} error on connection_sync for connection_id: {airbyte_connections}. Retrying {retries}/{max_retries}..."
                    )
                else:
                    raise AirflowException(f"Error as {e}")
        if retries == max_retries:
            raise AirflowException(f"Max retries exceded")
        task_logger.info(
            f"Job {job_id} of airbyte connection {airbyte_connections} complete."
        )

        # Update the state if the platform is Q
        if platform == constants.Q and status_ == "succeeded":
            set_state(connection_id=airbyte_connections)

        ti.xcom_push(key=f"job_startdate", value=f"{job_id}/{config_id}/{start_date}")
        ti.xcom_push(key=f"stream_state", value=f"{stream_state}")
        task_logger.info(f"job ids --->{job_id}")
        task_logger.info(f"Config ids --->{config_id}")
    except Exception as e:
        task_logger.info(f"stream_state_info = {stream_state_info}")
        task_logger.error(f"Error {e} .")

        if stream_state_info:
            if platform == constants.BRC:
                func_name = "brc_stream_state_payload"
            elif platform == constants.BRT:
                func_name = "brt_stream_state_payload"
            elif platform == constants.Q:
                func_name = "q_stream_state_payload"
            elif platform == constants.Redtrack:
                func_name = "redtrack_stream_state_payload"
            else:
                func_name = "stream_state_payload"
            task_logger.info(f"func_name = {func_name}")
            task_logger.info(f"airbyte_connections = {airbyte_connections}")
            task_logger.info(f"stream_state_info = {stream_state_info}")
            payload = getattr(StreamStatePayload, func_name)(
                airbyte_connections, stream_state_info
            )
            task_logger.info(f"payload = {payload}")
            set_stream_state(airbyte_connections, payload)
        airflow_trigger_airbyte_sync_task_error_slack_alert(
            {"config_id": airbyte_connections, "error_msg": str(e)}
        )
        if job_id == 0:
            raise AirflowException(
                f"Error when creating job for airbyte connection {airbyte_connections} ."
            )
        elif is_job_complete == False:
            raise AirflowException(
                f"Error when waiting {job_id} of airbyte connection {airbyte_connections} complete."
            )
        else:
            raise AirflowException(
                f"Error when check job {job_id} status of airbyte connection {airbyte_connections} complete."
            )
    else:
        if status_ in ("failed", "cancelled"):
            task_logger.info(f"stream_state_info = {stream_state_info}")
            if stream_state_info:
                if platform == constants.BRC:
                    func_name = "brc_stream_state_payload"
                elif platform == constants.BRT:
                    func_name = "brt_stream_state_payload"
                elif platform == constants.Q:
                    func_name = "q_stream_state_payload"
                elif platform == constants.Redtrack:
                    func_name = "redtrack_stream_state_payload"
                else:
                    func_name = "stream_state_payload"

                payload = getattr(StreamStatePayload, func_name)(
                    airbyte_connections, stream_state_info
                )

                set_stream_state(airbyte_connections, payload)
            raise AirflowException(f"Job has {status_}")


def airbyte_is_job_complete(response):
    global status_

    job_status_dict = json.loads(response.text)
    status_ = job_status_dict["job"]["status"]

    if status_ == "succeeded":
        return True
    elif status_ == "failed":
        airflow_airbyte_sync_task_slack_alert(job_status_dict)
        return True
    elif status_ == "cancelled":
        airflow_airbyte_sync_task_slack_alert(job_status_dict)
        return True
    else:
        return False


def trigger_airbyte_job(ti, connection_id, **kwargs):
    """
    Triggers an Airbyte job for a specific connection and checks if it succeeded.
    This function only triggers the job without setting stream state or deleting files.

    Args:
        ti: Task instance
        connection_id (str): ID of the Airbyte connection to trigger
        **kwargs: Additional keyword arguments

    Returns:
        bool: True if the job completed successfully, False otherwise
    """
    _AIRBYTE_CONN = os.getenv("airbyte_conn")
    context = get_current_context()

    task_logger.info(f"Triggering sync for Airbyte connection: {connection_id}")

    job_id = 0

    try:
        # Trigger the sync job
        retries = 0
        max_retries = 5
        retry_delay = 10

        while retries < max_retries:
            trigger_sync = HttpOperator(
                task_id="start_airbyte_sync",
                http_conn_id=_AIRBYTE_CONN,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",  # Airbyte cloud requires that a user agent is defined
                    "Accept": "application/json",
                },
                endpoint="api/v1/connections/sync",
                data=json.dumps({"connectionId": connection_id}),
                response_filter=lambda response: {
                    "job_id": response.json()["job"]["id"],
                    "config_id": response.json()["job"]["configId"],
                },
            )

            try:
                response_values = trigger_sync.execute(context=context)
                job_id = response_values["job_id"]
                task_logger.info(
                    f"Successfully triggered job {job_id} for connection {connection_id}"
                )

                # push job_id to xcom
                ti.xcom_push(key="job_id", value=job_id)

                break
            except Exception as e:
                error_msg = str(e)
                if (
                    "502" in error_msg
                    or "Bad Gateway" in error_msg
                    or "500" in error_msg
                    or "Internal Server Error" in error_msg
                ):
                    # Retry when we get status code as 502
                    retries += 1
                    time.sleep(retry_delay)
                    task_logger.info(
                        f"Received {error_msg} error on connection_sync for connection_id: {connection_id}. Retrying {retries}/{max_retries}..."
                    )
                elif "409" in error_msg or "Conflict" in error_msg:
                    # Condition to check conflicts in airbyte sync
                    task_logger.info(f"Conflict for connection_id: {connection_id}")
                    # Function to get job_id and config_id for running sync
                    response = Utils.check_conflict(connection_id)
                    job_id = response["job"]["id"]
                    # push job_id to xcom
                    ti.xcom_push(key="job_id", value=job_id)
                    break  # Exit the loop once job_id is obtained
                else:
                    raise AirflowException(f"Error triggering Airbyte job: {e}")

        if retries == max_retries:
            raise AirflowException(
                "Max retries exceeded while trying to trigger Airbyte job"
            )

        # Wait for the job to complete
        task_logger.info(
            f"Waiting for job {job_id} of Airbyte connection {connection_id} to complete."
        )

        retries = 0
        while retries < max_retries:
            wait_for_sync_to_complete = HttpSensor(
                method="POST",
                task_id="wait_for_airbyte_sync",
                http_conn_id=_AIRBYTE_CONN,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",
                    "Accept": "application/json",
                },
                request_params=json.dumps({"id": job_id}),
                endpoint="/api/v1/jobs/get",
                poke_interval=90,
                soft_fail=True,
                response_check=lambda response: airbyte_is_job_complete(response),
            )

            try:
                wait_for_sync_to_complete.execute(context=context)
                break
            except Exception as e:
                error_msg = str(e)
                if (
                    "502" in error_msg
                    or "Bad Gateway" in error_msg
                    or "500" in error_msg
                    or "Internal Server Error" in error_msg
                ):
                    # Retry when we get status code as 502
                    retries += 1
                    time.sleep(retry_delay)
                    task_logger.info(
                        f"Received {error_msg} error while waiting for job {job_id}. Retrying {retries}/{max_retries}..."
                    )
                else:
                    raise AirflowException(
                        f"Error waiting for Airbyte job to complete: {e}"
                    )

        if retries == max_retries:
            raise AirflowException(
                "Max retries exceeded while waiting for Airbyte job to complete"
            )

        # Check if the job completed successfully
        if status_ == "succeeded":
            task_logger.info(
                f"Airbyte job {job_id} for connection {connection_id} completed successfully."
            )
            return True
        else:
            task_logger.info(
                f"Airbyte job {job_id} for connection {connection_id} failed with status: {status_}"
            )
            return False

    except Exception as e:
        task_logger.error(f"Error in trigger_airbyte_job: {e}")
        raise AirflowException(f"Failed to trigger or monitor Airbyte job: {e}")


def airbyte_api_connections_sync_v2(ti, airbyte_connections, platform, platform_id, task_name):

    ti.xcom_push(key="platform_id", value=platform_id)
    ti.xcom_push(key="task_name", value=task_name)
    ti.xcom_push(key="job_execute_step", value="S1")

    result = fetch_path_from_data_source(airbyte_connections[0])
    bucket_name = os.getenv("Input_bucket")
    for path in result:
        task_logger.info(path)
        path = path[0]
        folder_path = os.path.dirname(path)
        archive_all_files_in_s3_folder(bucket_name, folder_path)

    _AIRBYTE_CONN = os.getenv("airbyte_conn")

    context = get_current_context()
    task_logger.info(f"context --> {context}")
    airbyte_connections = airbyte_connections[0]
    task_logger.info(f"airbyte_connections --> {airbyte_connections}")

    job_id = 0
    is_job_complete = False
    stream_state_info = []

    try:
        start_date = []
        allresponse = get_state(airbyte_connections)

        # Check if 'streamState' exists and has entries
        if (
            "streamState" in allresponse
            and isinstance(allresponse["streamState"], list)
            and allresponse["streamState"]
        ):
            for d in allresponse["streamState"]:
                stream_name = d["streamDescriptor"].get("name")
                if "cursor" in d["streamState"]:
                    stream_state = d["streamState"]["cursor"]
                else:
                    stream_state = next(
                        iter(d["streamState"].values())
                    )  # get the first value from this dict, which is the value of the cursor

                if stream_state:
                    stream_state_info.append({stream_name: stream_state})

        if allresponse["stateType"] == "not_set":
            response = get_connection(airbyte_connections)
            response_data = response.json()
            stream_names = [
                stream["stream"]["name"]
                for stream in response_data["syncCatalog"]["streams"]
            ]
            task_logger.info(stream_names)

            for stream in stream_names:
                start_date_tuple = fetch_start_date_from_ACCOUNT(airbyte_connections)
                date = str(start_date_tuple[0][0])
                combined_datestream = stream + ":" + date
                start_date.append(combined_datestream)
                task_logger.info(start_date)
            stream_state = "Fullrefresh"
        else:
            loopback_days_tuple = fetch_loopback_days_from_ACCOUNT(airbyte_connections)
            loopback_days = (
                loopback_days_tuple[0][0]
                if loopback_days_tuple[0][0] or loopback_days_tuple[0][0] is not None
                else 0
            )

            if platform == constants.Smartico:
                loopback_days += 1
            if platform == constants.BRC or platform == constants.BRT:
                loopback_days = 0

            stream_states = []
            incremental_streams, full_refresh_streams = (
                get_inc_full_refresh_streams(airbyte_connections)
            )

            for item in allresponse["streamState"]:
                # added code for solving cursor_field for resolving for full refresh
                if item["streamDescriptor"]["name"] in incremental_streams:
                    task_logger.info("inside if condition of incremental stream")

                    stream_name = item["streamDescriptor"]["name"]
                    if "cursor" in item["streamState"]:
                        stream_state = item["streamState"]["cursor"]
                    else:
                        stream_state = next(iter(item["streamState"].values()))
                    stream_states.append((stream_state, stream_name))

            for stream in full_refresh_streams:
                stream_states.append(
                    (datetime.today().strftime("%Y-%m-%d"), stream)
                )

            task_logger.info(f"stream_states --> {stream_states}")

            for date, stream in stream_states:
                task_logger.info(date)
                parsed_datetime = isoparse(date)
                if platform == constants.BRC or platform == constants.BRT :
                    original_date = parsed_datetime.date()
                    task_logger.info(f"Original date: {original_date}")
                    
                    combined_datestream = (
                        stream + ":" + original_date.strftime("%Y-%m-%d")
                    )
                    start_date.append(combined_datestream)
                else:
                    original_date = parsed_datetime.date()
                    task_logger.info(f"Original date: {original_date}")
                    modified_date = original_date - timedelta(days=loopback_days)
                    converted_date = modified_date.strftime("%Y-%m-%d")
                    combined_datestream = stream + ":" + converted_date
                    start_date.append(combined_datestream)

            task_logger.info(start_date)
            stream_state = "Incremental"

        task_logger.info(
            f"Start syncing data for airbyte connection {airbyte_connections} ."
        )
        retries = 0
        max_retries = 5
        retry_delay = 10
        while retries < max_retries:
            trigger_sync = HttpOperator(
                task_id="start_airbyte_sync",
                http_conn_id=_AIRBYTE_CONN,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",  # Airbyte cloud requires that a user agent is defined
                    "Accept": "application/json",
                },
                endpoint=f"/api/v1/connections/sync",
                data=json.dumps({"connectionId": airbyte_connections}),
                response_filter=lambda response: {
                    "job_id": response.json()["job"]["id"],
                    "config_id": response.json()["job"]["configId"],
                },
            )
            try:
                response_values = trigger_sync.execute(context=context)
                job_id = response_values["job_id"]
                config_id = response_values["config_id"]
                break
            except Exception as e:
                error_msg = str(e)
                if (
                    "502" in error_msg
                    or "Bad Gateway" in error_msg
                    or "500" in error_msg
                    or "Internal Server Error" in error_msg
                ):
                    # Retry when we get status code as 502
                    retries += 1
                    time.sleep(retry_delay)
                    raise_err = f"Received {error_msg} error on connection_sync for connection_id: {airbyte_connections}. Retrying {retries}/{max_retries}..."
                    task_logger.info(raise_err)
                    if (retries == max_retries):
                        raise AirflowException(error_msg)
                elif "409" in error_msg or "Conflict" in error_msg:
                    # Condition to check conflicts in airbyte sync
                    task_logger.info(
                        f"Conflict for connection_id: {airbyte_connections}"
                    )
                    # Function to get job_id and config_id for running sync
                    response = Utils.check_conflict(airbyte_connections)
                    job_id = response["job"]["id"]
                    config_id = response["job"]["configId"]
                    if (retries == max_retries):
                        raise AirflowException(error_msg)
                    break  # Exit the loop once job_id and config_id are obtained
                else:
                    raise AirflowException(f"Error as {e}")
        if retries == max_retries:
            raise AirflowException(f"Max retries exceded")

        task_logger.info(
            f"Waiting job {job_id} of airbyte connection {airbyte_connections} complete."
        )

        retries = 0
        while retries < max_retries:
            wait_for_sync_to_complete = HttpSensor(
                method="POST",
                task_id="wait_for_airbyte_sync",
                http_conn_id=_AIRBYTE_CONN,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",
                    "Accept": "application/json",
                },
                request_params=json.dumps({"id": job_id}),
                endpoint="/api/v1/jobs/get",
                poke_interval=90,
                soft_fail=True,
                response_check=lambda response: airbyte_is_job_complete(response),
            )
            try:
                is_job_complete = wait_for_sync_to_complete.execute(context=context)
                break
            except Exception as e:
                error_msg = str(e)
                if (
                    "502" in error_msg
                    or "Bad Gateway" in error_msg
                    or "500" in error_msg
                    or "Internal Server Error" in error_msg
                ):
                    # Retry when we get status code as 502
                    retries += 1
                    time.sleep(retry_delay)
                    raise_err = f"Received {error_msg} error on connection_sync for connection_id: {airbyte_connections}. Retrying {retries}/{max_retries}..."
                    task_logger.info(raise_err)
                    if (retries == max_retries):
                        raise AirflowException(raise_err)
                elif "SIGTERM" in error_msg:
                    retries += 1
                    time.sleep(retry_delay)
                    raise_err = f"Received {error_msg} error on connection_sync for connection_id: {airbyte_connections}. Retrying {retries}/{max_retries}..."
                    task_logger.info(raise_err)
                    if (retries == max_retries):
                        raise AirflowException(raise_err)
                else:
                    raise AirflowException(f"Error as {e}")
        if retries == max_retries:
            raise AirflowException(f"Max retries exceded")
        task_logger.info(
            f"Job {job_id} of airbyte connection {airbyte_connections} complete."
        )

        # Update the state if the platform is Q
        if platform == constants.Q and status_ == "succeeded":
            set_state(connection_id=airbyte_connections)

        ti.xcom_push(key=f"job_startdate", value=f"{job_id}/{config_id}/{start_date}")
        ti.xcom_push(key=f"stream_state", value=f"{stream_state}")
        task_logger.info(f"job ids --->{job_id}")
        task_logger.info(f"Config ids --->{config_id}")
    except Exception as e:
        task_logger.info(f"stream_state_info = {stream_state_info}")
        task_logger.error(f"Error {e} .")

        if stream_state_info:
            task_logger.info(f"airbyte_connections = {airbyte_connections}")
            task_logger.info(f"stream_state_info = {stream_state_info}")
            stream_state_payload = create_stream_state_payload(allresponse)
            task_logger.info(f"payload = {stream_state_payload}")
            set_stream_state(airbyte_connections, stream_state_payload)

        airflow_trigger_airbyte_sync_task_error_slack_alert(
            {"config_id": airbyte_connections, "error_msg": str(e)}
        )
        if job_id == 0:
            raise AirflowException(
                f"Error when creating job for airbyte connection {airbyte_connections} ."
            )
        elif is_job_complete == False:
            raise AirflowException(
                f"Error when waiting {job_id} of airbyte connection {airbyte_connections} complete."
            )
        else:
            raise AirflowException(
                f"Error when check job {job_id} status of airbyte connection {airbyte_connections} complete."
            )
    else:
        if status_ in ("failed", "cancelled"):
            task_logger.info(f"stream_state_info = {stream_state_info}")
            if stream_state_info:
                task_logger.info(f"airbyte_connections = {airbyte_connections}")
                task_logger.info(f"stream_state_info = {stream_state_info}")
                stream_state_payload = create_stream_state_payload(allresponse)
                task_logger.info(f"payload = {stream_state_payload}")
                set_stream_state(airbyte_connections, stream_state_payload)
            raise AirflowException(f"Job has {status_}")

def handle_airbyte_job_failure(context):
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    dag_id = context.get('dag').dag_id
    task_id = context.get('task').task_id
    log_url = task_instance.log_url

    jobs = os.getenv("jobs")
    connection = mysql_conn()
    cursor = connection.cursor()

    platform_id = task_instance.xcom_pull(task_ids=context["task"].task_id, key="platform_id")
    task_name = task_instance.xcom_pull(task_ids=context["task"].task_id, key="task_name")
    job_execute_step = task_instance.xcom_pull(task_ids=context["task"].task_id, key="job_execute_step")
    
    # Pull job start date from XCom for the current task run
    job_and_configs_startdate = task_instance.xcom_pull(key="job_startdate", task_ids=task_name)

    task_logger.info(f"Task {task_id} in DAG {dag_id} failed.")
    task_logger.info(f"Error: {exception}")
    task_logger.info(f"Check logs here: {log_url}")
    task_logger.info(f"Job Execute Step: {job_execute_step}")
    task_logger.info(f"Task Name: {task_name}")

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
    start_date_list = [ast.literal_eval(item) for item in start_dates]
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
        config_type = response_json["job"]["configType"]
        job_created_at = datetime.fromtimestamp(response_json["job"]["createdAt"])
        job_updated_at = datetime.fromtimestamp(response_json["job"]["updatedAt"])
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
                start_date = datetime.today().strftime("%Y-%m-%d") if dag_id == "BrcExecuteAllOperatorAccounts" else opp[6]
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
            start_time = datetime.fromtimestamp(last_attempt["createdAt"])
        else:
            start_time = datetime.now()

        if "endedAt" in last_attempt:
            end_time = datetime.fromtimestamp(last_attempt["endedAt"])
        else:
            end_time = datetime.now()
        
        execution_time_taken = end_time - start_time
        # SQL query to insert the job details into the jobs table
        task_logger.info("jobs table --> ", jobs)
        job_insert_query = f"""
            INSERT INTO {jobs}
                (operator_id, job_id, job_execute_step, config_type, status, records_extracted, records_loaded, data_size,
                execution_time_taken, created_at, updated_at, attempt_started, attempt_ended,
                error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        job_insert_values = (opp, job_id, job_execute_step, config_type, status, records_extracted, records_loaded, data_size,
                                execution_time_taken, job_created_at, job_updated_at, start_time, end_time, 
                                f"Task {task_id} in DAG {dag_id} failed.\nError: {exception}\nCheck logs here: {log_url}")
        try:
            dataTemp = cursor.execute(job_insert_query, job_insert_values)
            connection.commit()
            task_logger.info("job table --> ", dataTemp)
            task_logger.info("values inserted in JOBS Table")
        except Exception as e:
            task_logger.error("Error inserting job details into JOBS Table: ", str(e))
        cursor.close()
        connection.close()
    
