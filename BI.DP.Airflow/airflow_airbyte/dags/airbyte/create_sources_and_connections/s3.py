import logging
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class S3Destination:
    destination_name = "s3-destination"
    destination_definition_id = "4816b78f-1489-44c1-9060-4b19d5fa9362"

    def __init__(self, access_key_id, secret_access_key, s3_bucket_name, s3_bucket_path, s3_path_format,
                 s3_bucket_region, file_name_pattern) -> None:
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.s3_bucket_name = s3_bucket_name
        self.s3_bucket_path = s3_bucket_path
        self.s3_path_format = s3_path_format
        self.s3_bucket_region = s3_bucket_region
        self.file_name_pattern = file_name_pattern

    def s3_create_destination_payload(self, name, airbyte_workspace_id, format_type):
        payload = {
            'name': name,
            'destinationName': S3Destination.destination_name,
            'destinationDefinitionId': S3Destination.destination_definition_id,
            'workspaceId': airbyte_workspace_id
        }

        payload['connectionConfiguration'] = {
            'format': self.get_format_config(format_type),
            'access_key_id': self.access_key_id,
            'secret_access_key': self.secret_access_key,
            's3_bucket_name': self.s3_bucket_name,
            's3_bucket_path': self.s3_bucket_path,
            's3_path_format': self.s3_path_format,
            's3_bucket_region': self.s3_bucket_region,
            'file_name_pattern': self.file_name_pattern,
        }
        return payload

    def get_format_config(self, format_type):
        if format_type == 'Parquet':
            return {
                'format_type': 'Parquet',
                'page_size_kb': 1024,
                'block_size_mb': 128,
                'compression_codec': 'UNCOMPRESSED',
                'dictionary_encoding': True,
                'max_padding_size_mb': 8,
                'dictionary_page_size_kb': 1024
            }
        elif format_type == 'JSONL':
            return {
                'flattening': 'No flattening',
                'compression': {'compression_type': 'No Compression'},
                'format_type': 'JSONL'
            }
        elif format_type == 'CSV':
            return {
                'flattening': 'No flattening',
                'compression': {'compression_type': 'No Compression'},
                'format_type': 'CSV'
            }
        elif format_type == 'Avro':
            return {
                'format_type': 'Avro',
                'compression_codec': {'codec': 'no compression'}
            }
        else:
            raise ValueError(f"Unsupported format type: {format_type}")

    def s3_delete_destination(destination_id):
        url = "http://localhost:8000/api/v1/destinations/delete"
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        basic_auth = HTTPBasicAuth("airbyte", "password")
        payload = {"destinationId": destination_id}

        response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)

    def s3_get_destination(destination_id):
        url = "http://localhost:8000/api/v1/destinations/get"
        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        basic_auth = HTTPBasicAuth("airbyte", "password")
        payload = {"destinationId": destination_id}

        response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)

        logger.info(response.json())

    def s3_update_destination_payload(self, name, airbyte_workspace_id, destination_id, format_type):
        payload = {
            'name': name,
            'destinationName': S3Destination.destination_name,
            'destinationDefinitionId': S3Destination.destination_definition_id,
            'destinationId': destination_id,
            'workspaceId': airbyte_workspace_id
        }

        payload['connectionConfiguration'] = {
            'format': self.get_format_config(format_type),
            'access_key_id': self.access_key_id,
            'secret_access_key': self.secret_access_key,
            's3_bucket_name': self.s3_bucket_name,
            's3_bucket_path': self.s3_bucket_path,
            's3_path_format': self.s3_path_format,
            's3_bucket_region': self.s3_bucket_region,
            'file_name_pattern': self.file_name_pattern,
        }
        return payload
