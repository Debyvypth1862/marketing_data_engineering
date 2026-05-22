import io
import json
import logging
import os
import time
from datetime import datetime

import boto3
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger("airflow.task")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)


def get_inc_full_refresh_streams(connection_id):
    """
    Gets incremental and full_refresh streams for an airbyte connection id
    """
    airbyte_host = os.getenv("airbyte_server")
    endpoint = f"api/v1/connections/get"
    base_url = airbyte_host + endpoint
    basic_auth = HTTPBasicAuth("airbyte", "password")

    headers = {"Content-Type": "application/json"}

    payload = {"connectionId": connection_id}

    response = requests.post(
        url=base_url, json=payload, headers=headers, auth=basic_auth
    ).json()

    incremental_streams = []
    full_refresh_streams = []
    # logger.info(response) --commented out for brc testing
    for item in response["syncCatalog"]["streams"]:
        if item["config"]["syncMode"] == "incremental":
            stream = item["stream"]["name"]
            incremental_streams.append(stream)
        elif item["config"]["syncMode"] == "full_refresh":
            stream = item["stream"]["name"]
            full_refresh_streams.append(stream)

    return incremental_streams, full_refresh_streams


def read_file_from_s3(file_path, retry_count=1):
    """
    Read file from s3 with exact path
    """
    max_retries = 5

    try:
        response = s3_client.get_object(
            Bucket=os.getenv("Input_bucket"),
            Key=f"{os.getenv('Input_bucket_path')}{file_path}",
        )
        return response["Body"].read().decode("utf-8")
    except Exception as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchKey" and retry_count <= max_retries:
            logger.error(
                f" Attempt {retry_count}: The specified key does not exist in S3."
            )
            time.sleep(30)  # Sleep for 30 seconds
            return read_file_from_s3(file_path, retry_count + 1)
        else:
            logger.error(f"Error reading file from S3: {e}")


def load_full_refresh_into_s3(file_path, retry_count=1):
    """
    Load full refresh streams into the final S3 output bucket
    """
    max_retries = 5
    logger = logging.getLogger(__name__)

    input_bucket = os.getenv("Input_bucket")
    input_file_key = f"{os.getenv('Input_bucket_path')}{file_path}"

    output_bucket = os.getenv("Output_bucket_DQ")
    output_file_key = f"{os.getenv('Output_bucket_path_DQ_NC')}{file_path}"

    def convert_jsonl_to_json(jsonl_content):
        json_list = []
        for line in jsonl_content.splitlines():
            try:
                json_list.append(json.loads(line.strip()))
            except Exception as e:
                logger.error(e)

        return json.dumps(json_list)

    try:
        response = s3_client.get_object(Bucket=input_bucket, Key=input_file_key)
    except Exception as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchKey" and retry_count <= max_retries:
            logger.error(
                f" Attempt {retry_count}: Key - {input_file_key} doesn't exist in S3."
            )
            logger.info(f"Waiting for 30 seconds...")
            time.sleep(30)
            return read_file_from_s3(file_path, retry_count + 1)
        else:
            logger.error(f"Error reading file from S3: {e}")
    else:
        jsonl_content = response["Body"].read().decode("utf-8")
        json_content = convert_jsonl_to_json(jsonl_content)

    s3_client.put_object(
        Bucket=output_bucket, Key=output_file_key, Body=json_content.encode("utf-8")
    )
    print(
        f"File successfully copied from {input_bucket}/{input_file_key} to {output_bucket}/{output_file_key}"
    )


# Function to move file from one s3 bucket to another
def move_object(source_bucket, source_key, destination_bucket, destination_key):
    try:
        # Copy the object to the destination bucket
        s3_client.copy_object(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
        )
        logger.info(
            f"Successfully copied {source_key} from {source_bucket} to {destination_key} in {destination_bucket}"
        )

        # Delete the object from the source bucket
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        logger.info(f"Successfully deleted {source_key} from {source_bucket}")

    except Exception as e:
        logger.info(f"Failed to move object: {e}")


def archive_all_files_in_s3_folder(bucket_name, path):
    try:
        """
        Deletes all files in the specified S3 bucket and folder path.
    
        Args:
            bucket_name (str): Name of the S3 bucket.
            path (str): Folder path in the S3 bucket.
    
        """
        current_date_time = datetime.now().strftime("%Y-%m-%d_%I:%M:%S_%p")
        archive_path = f"{path}/{current_date_time}"
        
        # List all objects in the specified folder
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path)

        # Check if there are any objects to delete
        if "Contents" in response:
            # Create a list of keys to delete
            files_to_archive = [obj["Key"] for obj in response["Contents"]]
            for file in files_to_archive:
                archive_file_key = f"{archive_path}/{file.split('/')[-1]}"
                logger.info(f"Archiving file: {file} to {archive_file_key}")
                move_object(
                    f"{bucket_name}",
                    file,
                    f"{os.getenv('Archive_bucket')}",
                    f"{os.getenv('Archive_bucket_path')}{archive_file_key}",
                )
            logger.info(f"Successfully archived all files in folder '{path}'")
        else:
            logger.info(f"No files found in folder '{path}' to archive.")
    except Exception as e:
        logger.info(f"Failed to archive files in folder '{path}': {e}")
