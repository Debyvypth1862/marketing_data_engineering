import os
import logging
import requests
from requests.auth import HTTPBasicAuth

# Set up logging for debugging and tracking API calls
logger = logging.getLogger(__name__)

class AirbyteConnections:
    
    # Create a new connection in Airbyte
    def create_connection(connection_payload):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv("airbyte_server")
        # Define API endpoint for creating a connection
        endpoint = "api/v1/connections/create"
        base_url = airbyte_host + endpoint
        # Basic authentication for Airbyte API
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload and headers for the request
        payload = connection_payload
        headers = {"accept": "application/json", "content-type": "application/json"}
        
        # Send a POST request to create the connection
        response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)
        
        # Log the response status and content
        logger.info(f"Status code: {response.status_code}")
        logger.info(response.text.encode("utf-8"))
        
        return response

    # Retrieve the details of an existing connection
    def get_connection(connection_id):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv("airbyte_server")
        # Define API endpoint for retrieving a connection
        endpoint = "api/v1/connections/get"
        url = airbyte_host + endpoint
        # Headers and authentication for the request
        headers = {"accept": "application/json", "content-type": "application/json"}
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Payload to specify the connection ID
        payload = {"connectionId": connection_id}
        
        # Send a POST request to get the connection details
        response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)
        return response

    # Delete an existing connection from Airbyte
    def delete_connection(connection_id):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv("airbyte_server")
        # Define API endpoint for deleting a connection
        endpoint = "api/v1/connections/delete"
        url = airbyte_host + endpoint
        # Headers and authentication for the request
        headers = {"accept": "application/json", "content-type": "application/json"}
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Payload to specify the connection ID
        payload = {"connectionId": connection_id}
        
        # Send a POST request to delete the connection
        response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)
        

    # Update an existing connection configuration
    def update_connection(connection_payload):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv("airbyte_server")
        # Define API endpoint for updating a connection
        endpoint = "api/v1/connections/update"
        base_url = airbyte_host + endpoint
        # Basic authentication for Airbyte API
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload and headers for the request
        payload = connection_payload
        headers = {"accept": "application/json", "content-type": "application/json"}
        
        # Send a POST request to update the connection
        response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)
        
        # Log the response status and content
        logger.info(f"Status code: {response.status_code}")
        logger.info(response.text.encode("utf-8"))
        
        return response

    # Trigger a sync operation on an existing connection
    def trigger_connection(connection_id):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv("airbyte_server")
        # Define API endpoint for triggering sync
        endpoint = f"api/v1/connections/sync"
        base_url = airbyte_host + endpoint
        # Basic authentication for Airbyte API
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Headers and payload for the sync operation
        headers = {"accept": "application/json", "content-type": "application/json"}
        payload = {"connectionId": connection_id}
        
        # Send a POST request to trigger the connection sync
        response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)
        
        # Log the response status and content
        logger.info(f"Status code: {response.status_code}")
        logger.info(response.text.encode("utf-8"))
        
        return response

    # Update the state of a connection (e.g., paused or resumed)
    def update_connection_state(connection_payload):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv("airbyte_server")
        # Define API endpoint for creating or updating connection state
        endpoint = f"api/v1/state/create_or_update"
        base_url = airbyte_host + endpoint
        # Basic authentication for Airbyte API
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Headers and payload for the state update request
        headers = {"Content-Type": "application/json"}
        payload = connection_payload
        
        # Send a POST request to update the connection state
        response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)
        
        # Log the response status and content
        logger.info(f"Status code: {response.status_code}")
        logger.info(response.text.encode("utf-8"))

    # Retrieve a list of jobs related to a specific connection
    def get_jobs_list(connection_id):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv("airbyte_server")
        # Define API endpoint for retrieving job list
        endpoint = "api/v1/jobs/list"
        url = airbyte_host + endpoint
        # Headers with authorization for the request
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic YWlyYnl0ZTpwYXNzd29yZA==",  # Base64 encoded auth credentials
        }
        # Payload to filter jobs by connection ID and types
        data = {
            "configId": connection_id,
            "configTypes": ["sync", "reset_connection"],
            "pagination": {"pageSize": 10},
        }
        
        # Send a POST request to get the job list
        response = requests.post(url, json=data, headers=headers)
        
        # Log the response status and content (JSON)
        logger.info(response.status_code)
        logger.info(response.json())

    # Enable or disable a specific connection by updating its status
    def enable_disable_connection(connection_id, status):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv("airbyte_server")
        # Define API endpoint for updating the connection
        endpoint = f"api/v1/connections/update"
        base_url = airbyte_host + endpoint
        # Basic authentication for Airbyte API
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Payload to specify the connection ID and the new status
        payload = {"status": status, "connectionId": connection_id}
        headers = {"accept": "application/json", "content-type": "application/json"}
        
        # Send a POST request to update the connection status (enable/disable)
        response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)
        
        # Log the response status and content
        logger.info(f"Status code: {response.status_code}")
        logger.info(response.text.encode("utf-8"))
        
        return response