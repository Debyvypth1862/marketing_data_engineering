import os
import logging
import requests
import time
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout

# Set up logging for debugging and tracking API calls
logger = logging.getLogger(__name__)


class AirbyteSources:
    
    # Create a new source in Airbyte
    def create_source(source_payload):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv('airbyte_server')
        # Define the API endpoint for creating a source
        endpoint = "api/v1/sources/create"
        base_url = airbyte_host + endpoint
        # Basic authentication for Airbyte API
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload and headers for the request
        payload = source_payload
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        # Send a POST request to create the source
        response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)

        # Log the response status and content
        logger.info(f"Status code: {response.status_code}")
        logger.info(response.text)

        return response

    # Delete an existing source from Airbyte
    def delete_source(source_id):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv('airbyte_server')
        # Define the API endpoint for deleting a source
        endpoint = "api/v1/sources/delete"
        url = airbyte_host + endpoint
        # Set headers and authentication for the request
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload with the source ID
        payload = {"sourceId": source_id}
        
        # Send a POST request to delete the source
        response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)
    # Retrieve details of a specific source
    def get_source(source_id):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv('airbyte_server')
        # Define the API endpoint for getting a source's details
        endpoint = "api/v1/sources/get"
        url = airbyte_host + endpoint
        # Set headers and authentication for the request
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload with the source ID
        payload = {"sourceId": source_id}

        # Send a POST request to get the source details
        response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)
        
        # Log the response content (source details)
        logger.info(response.json())

    # Update an existing source's configuration
    def update_source(source_payload):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv('airbyte_server')
        # Define the API endpoint for updating a source
        endpoint = "api/v1/sources/update"
        base_url = airbyte_host + endpoint
        # Basic authentication for Airbyte API
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload and headers for the request
        payload = source_payload
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        # Send a POST request to update the source
        response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)

        # Log the response status and content (success message or errors)
        logger.info(f"Status code: {response.status_code}")
        logger.info(response.text)

        return response

    # Check the connection status for a specific source with retries
    def check_connection(source_id):
        retries = 0
        MAX_RETRIES = 2  # Maximum number of retries in case of failure
        RETRY_DELAY = 20  # Delay between retries in seconds
        while retries < MAX_RETRIES:
            # Retrieve Airbyte server URL from environment variables
            airbyte_host = os.getenv("airbyte_server")
            # Define the API endpoint for checking the connection
            endpoint = "api/v1/sources/check_connection"
            url = airbyte_host + endpoint
            # Set headers and authentication for the request
            headers = {"accept": "application/json", "content-type": "application/json"}
            basic_auth = HTTPBasicAuth("airbyte", "password")
            # Prepare the payload with the source ID
            payload = {"sourceId": source_id}

            try:
                # Send a POST request to check the connection
                response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload, timeout=120, allow_redirects=True)
                
                # If the response is successful, log it
                if response.status_code == 200:
                    response_json = response.json()
                    logger.debug(f"Airbyte Check Connection Response for sourceId {source_id}: {response_json}")
                else:
                    logger.error(f"Error checking connection for sourceId {source_id}. Status Code: {response.status_code}, Response: {response.text}")

                return response

            except Timeout:
                # If a timeout occurs, log the error and retry
                logger.error(f"Request timed out while checking connection for sourceId: {source_id}. Attempt {retries + 1} of {MAX_RETRIES}.")
                retries += 1
                if retries < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)  # Wait before retrying
                else:
                    logger.error("Max retries reached due to timeout.")
                    return None
            except Exception as e:
                # Handle any other unexpected errors
                logger.error(f"An unexpected error occurred while checking connection for sourceId {source_id}. Error: {str(e)}")
                return None