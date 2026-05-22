import os
import logging
import requests
from requests.auth import HTTPBasicAuth

# Set up logging for debugging and tracking API calls
logger = logging.getLogger(__name__)

class AirbyteDestinations:
    
    # Create a new destination in Airbyte
    def create_destination(destination_payload):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv('airbyte_server')
        # Define the API endpoint for creating a destination
        create_destination_endpoint = "api/v1/destinations/create"
        base_url = airbyte_host + create_destination_endpoint
        # Basic authentication for Airbyte API
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload and headers for the request
        payload = destination_payload
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        # Send a POST request to create the destination
        response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)



    # Delete an existing destination from Airbyte
    def delete_destination(destination_id):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv('airbyte_server')
        # Define the API endpoint for deleting a destination
        endpoint = "api/v1/destinations/delete"
        url = airbyte_host + endpoint
        # Set headers and authentication for the request
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload with the destination ID
        payload = {
            "destinationId": destination_id
        }
        # Send a POST request to delete the destination
        response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)


    # Retrieve details of a specific destination
    def get_destination(destination_id):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv('airbyte_server')
        # Define the API endpoint for getting a destination's details
        endpoint = "api/v1/destinations/get"
        url = airbyte_host + endpoint
        # Set headers and authentication for the request
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload with the destination ID
        payload = {"destinationId": destination_id}

        # Send a POST request to get the destination details
        response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)
        
        # Log the response content (destination details)
        logger.info(response.json())

    # Update an existing destination's configuration
    def update_destination(destination_payload):
        # Retrieve Airbyte server URL from environment variables
        airbyte_host = os.getenv('airbyte_server')
        # Define the API endpoint for updating a destination
        endpoint = "api/v1/destinations/update"
        base_url = airbyte_host + endpoint
        # Basic authentication for Airbyte API
        basic_auth = HTTPBasicAuth("airbyte", "password")
        # Prepare the payload and headers for the request
        payload = destination_payload
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }

        # Send a POST request to update the destination
        response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)
        
        # Log the response status and content (success message or errors)
        logger.info(response.text)