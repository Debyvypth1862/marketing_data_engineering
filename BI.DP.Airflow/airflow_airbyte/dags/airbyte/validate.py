import os
import sys
import logging
import time

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout

sys.path.insert(1, "dags/airbyte")
from airbyte.db_connection import mysql_conn
from airbyte.slack_alerts import operator_validation_fail_slack_alert
from airflow.operators.python import get_current_context
import constants
from datetime import datetime

task_logger = logging.getLogger("airflow.task")
task_logger.setLevel(logging.DEBUG)

if not task_logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    task_logger.addHandler(console_handler)

def check_connection(sourceId):
    """
    This function checks the connection status for a given sourceId by making a request
    to the Airbyte API's 'check_connection' endpoint. If the request times out, it retries 
    up to MAX_RETRIES times with a delay between each retry.

    Args:
        sourceId (str): The Airbyte source ID for which the connection status is being checked.
    
    Returns:
        response (object): The HTTP response object from the Airbyte API.
    """
    retries = 0
    MAX_RETRIES = 2
    RETRY_DELAY = 20  # in seconds
    while retries < MAX_RETRIES:
        airbyte_host = os.getenv("airbyte_server")
        endpoint = "api/v1/sources/check_connection"
        url = airbyte_host + endpoint
        headers = {"accept": "application/json", "content-type": "application/json"}
        basic_auth = HTTPBasicAuth("airbyte", "password")
        payload = {"sourceId": sourceId}

        try:
            # Make the API request to check the connection
            response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload, timeout=120, allow_redirects=True)
            
            # Log the response status and body if the request was successful
            if response.status_code == 200:
                response_json = response.json()
                task_logger.debug(f"Airbyte Check Connection Response for sourceId {sourceId}: {response_json}")
            else:
                task_logger.error(f"Error checking connection for sourceId {sourceId}. Status Code: {response.status_code}, Response: {response.text}")

            return response

        except Timeout:
            # If request times out, retry up to the max number of retries
            task_logger.error(f"Request timed out while checking connection for sourceId: {sourceId}. Attempt {retries + 1} of {MAX_RETRIES}.")
            retries += 1
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY)  # Wait before retrying
            else:
                task_logger.error("Max retries reached due to timeout.")
                return None
        except Exception as e:
            task_logger.error(f"An unexpected error occurred while checking connection for sourceId {sourceId}. Error: {str(e)}")
            return None



def fetch_source_ids(platform_id):
    """
    Fetches the source IDs from the database for the given platform_id. The query 
    retrieves sources that are valid and not marked as 'Invalid'.

    Args:
        platform_id (int): The platform ID to filter sources by.
    
    Returns:
        list: A list of tuples containing the source details (id, airbyte_source_id, operator_id, platform name, and endpoint).
    """
    ACCOUNT = os.getenv("ACCOUNT")
    PLATFORM = os.getenv("platform")
    connection = mysql_conn()
    cursor = connection.cursor()

    # SQL query to fetch valid sources for the given platform_id
    cursor.execute(f"""
        SELECT a.id, a.airbyte_source_id, a.operator_id, p.name, a.endpoint
        FROM {ACCOUNT} a
        INNER JOIN {PLATFORM} p
            ON a.platform_id = p.id
        WHERE (a.airbyte_source_id <> ''
            OR a.airbyte_source_id <> null)
            AND a.platform_id = {platform_id}
            AND a.validation_status <> 'Invalid'
            AND a.operator_id in (select DISTINCT operator_id  from JOB j where status <> 'succeeded' and job_execute_step = 'S1');
    """)
    result = cursor.fetchall()  # Fetch all the results
    connection.close()  # Close the database connection
    return result


def validate(ti, platform_id):
    """
    Validates the connections for all sources in the platform identified by `platform_id`.
    It checks the connection status via the Airbyte API and updates the validation status
    of each source in the database. If validation fails, a Slack alert is sent.

    Args:
        ti (TaskInstance): The Airflow task instance to push results to XCom.
        platform_id (int): The platform ID to validate sources for.
    """
    connection = mysql_conn()  # Establish a connection to MySQL database
    cursor = connection.cursor()
    validate_sources = []  # List to store validation results

    # If platform_id is None, log an error and push an empty validation result
    if platform_id is None:
        task_logger.info(f"Platform name is missing in platform table")
        ti.xcom_push(key=f"validate_operators", value=validate_sources)
        return None
    
    # Loop through each source fetched for the platform
    for source in fetch_source_ids(platform_id):
        validate_dict = {}
        source_id = source[1]
        id = source[0]
        operator_id = source[2]
        platform = source[3]
        endpoint=source[4]
        message = ""

        task_logger.debug(f"Processing source_id {source_id} for platform {platform}, operator_id {operator_id}")

        # If source_id is missing, log and continue to the next source
        if source_id is None or source_id == "":
            task_logger.info(f"Missing source: {source_id}.")
        else:
            validate_dict["Id"] = id
            validate_dict["sourceId"] = source_id
            validate_dict["operator_id"] = operator_id
            # Check the connection for the current source
            check_connection_response = check_connection(source_id)

            context = get_current_context()  # Get current Airflow task context

            if check_connection_response is not None:
                if check_connection_response.status_code == 200:
                    response_json = check_connection_response.json()

                    if "status" in response_json:
                        # If connection check returned a 'status' field, validate the result
                        status_code = response_json["status"]
                        message = response_json.get("message", "")
                        validation_status = ("Valid" if status_code == "succeeded" else "Failed")
                        task_logger.info(f"Validating source with sourceId {source_id}.")

                        if status_code == "succeeded":
                            task_logger.info(f"Valid source: {source_id}.")
                            task_logger.info("Success")
                            validation_status = "Valid"
                            is_validation_enabled = 1
                        else:
                            task_logger.info(f"Invalid source: {source_id}.")
                            validation_status = "Failed"
                            is_validation_enabled = 2
                            # Send Slack alert on failure
                            operator_validation_fail_slack_alert(context, platform, operator_id, message)

                        validate_dict["validation_status"] = validation_status
                        validate_dict["validation_message"] = message
                        validate_dict["is_validation_enabled"] = is_validation_enabled

                    else:
                        validation_status = constants.Invalid

                        try:
                            message = check_connection_response["failureReason"]["externalMessage"]
                        except:
                            message = "The check connection failed because of an internal error"

                        validate_dict["validation_status"] = validation_status
                        validate_dict["validation_message"] = message
                        validate_dict["is_validation_enabled"] = 2

                        task_logger.info(f"Error: Unexpected response format for sourceId {source_id}.")
                else:
                    # If the response status is not 200, mark the source as invalid
                    validation_status = constants.Invalid
                    message = f"Invalid response, Status_code = {check_connection_response.status_code}."

                    validate_dict["validation_status"] = validation_status
                    validate_dict["validation_message"] = message
                    validate_dict["is_validation_enabled"] = 2

                    task_logger.info(f"Error for sourceId {source_id}: {check_connection_response.text}")
                    # Send Slack alert for bad credentials
                    operator_validation_fail_slack_alert(context, platform, operator_id, "Bad Credentials")
            else:
                # If no response (request failed), mark source as invalid
                validation_status = constants.Invalid
                message = f"Error checking connection API for sourceId {source_id}. Failed to reach the {endpoint}, request timed out."

                validate_dict["validation_status"] = validation_status
                validate_dict["validation_message"] = message
                validate_dict["is_validation_enabled"] = 2

                task_logger.info(f"Error checking connection API for sourceId {source_id}. Failed to reach the {endpoint}, request timed out.")

                operator_validation_fail_slack_alert(
                    context,
                    platform,
                    operator_id,
                    f"Error checking connection API for sourceId {source_id}: Request timed out",
                )

            # Append the validation result for the current source
            validate_sources.append(validate_dict)
        
        # Push the validation results to XCom for downstream tasks
        ti.xcom_push(key=f"validate_operators", value=validate_sources)
    
    # Commit any changes to the database and close the connection
    connection.commit()
    cursor.close()
    connection.close()