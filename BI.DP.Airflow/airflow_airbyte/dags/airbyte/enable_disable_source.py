import os
import sys
import requests
from requests.auth import HTTPBasicAuth
from mysql.connector import Error
import logging

sys.path.insert(1, "dags/airbyte")
from db_connection import mysql_conn

logger = logging.getLogger(__name__)


def enable_disable_connection(connection_id, status):
    """
    Function to enable or disable a connection in Airbyte using its API.
    
    :param connection_id: The ID of the connection to enable or disable.
    :param status: The desired status of the connection ('active' or 'inactive').
    :return: The response object from the Airbyte API.
    """
    # Get the Airbyte host URL from environment variables
    airbyte_host = os.getenv("airbyte_server")
    # Construct the API endpoint to update the connection
    endpoint = f"api/v1/connections/update"
    base_url = airbyte_host + endpoint
    
    # Authentication for Airbyte API
    auth = HTTPBasicAuth("airbyte", "password")
    
    # Payload to send in the API request, containing connection status and connection ID
    payload = {"status": status, "connectionId": connection_id}
    headers = {"accept": "application/json", "content-type": "application/json"}
    
    # Make the POST request to update the connection status
    response = requests.post(url=base_url, json=payload, headers=headers, auth=auth)
    
    # Log the status code and the response from the Airbyte API
    logger.info(f"Status code: {response.status_code}")
    logger.info(response.text.encode("utf-8"))
    
    return response


def enable_disable():
    """
    Function to enable or disable connections based on their 'tlog_deleted' status in the database.
    This function checks the 'tlog_deleted' field to determine if a connection should be active or inactive.
    The connection status in the database is updated accordingly, and the Airbyte connection is enabled or disabled.
    """
    try:
        # Establish connection to the database
        connection = mysql_conn()
        ACCOUNT = os.getenv("ACCOUNT")
        
        # Query the database to get all accounts with a non-empty 'airbyte_connection_id'
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT airbyte_connection_id, connection_status, tlog_deleted
                FROM {ACCOUNT}
                WHERE airbyte_connection_id <> '';
            """)
            
            # Fetch all accounts that have a connection_id
            accounts = cursor.fetchall()
            
            # Iterate over each account to check its 'tlog_deleted' status
            for acc in accounts:
                # If 'tlog_deleted' is 1, mark the connection as 'inactive' and update the status to 'Disabled'
                if acc[2] == 1:
                    enable_disable_connection(acc[0], "inactive")
                    cursor.execute(f"""
                        UPDATE {ACCOUNT}
                        SET connection_status = 'Disabled'
                        WHERE airbyte_connection_id = '{acc[0]}';
                    """)
                # If 'tlog_deleted' is 0, mark the connection as 'active' and update the status to 'Enabled'
                elif acc[2] == 0:
                    enable_disable_connection(acc[0], "active")
                    cursor.execute(f"""
                        UPDATE {ACCOUNT}
                        SET connection_status = 'Enabled'
                        WHERE airbyte_connection_id = '{acc[0]}';
                    """)
            
            # Commit the changes to the database
            connection.commit()
            return None
    except Error as e:
        # Log any errors that occur during the process
        logger.error(f"Error: {e}")
    finally:
        # Ensure the database connection is closed
        connection.close()