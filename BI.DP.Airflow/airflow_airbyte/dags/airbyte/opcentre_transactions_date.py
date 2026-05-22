import os
import sys
import shutil
import datetime
from datetime import datetime
import pandas as pd
from requests.sessions import requote_uri
import boto3
from io import StringIO
import time
import json
import logging
from mysql.connector import Error

from airflow.models import Variable
from airflow.exceptions import AirflowException

sys.path.insert(1, "dags/airbyte")
from airbyte import constants
from airbyte.db_connection import mysql_conn
from airbyte.fetch_streams import (
    get_inc_full_refresh_streams,
    load_full_refresh_into_s3,
    move_object,
    archive_all_files_in_s3_folder
)
from airbyte.fetch_connection_list import (
    fetch_connection_data_source_table,
    fetch_Jobdetail_and_records_extracted,
)

task_logger = logging.getLogger("airflow.task")
recovery_interval = Variable.get("recovery_interval")
# s3 client for interacting with aws s3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)
session = boto3.Session(
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)
# Function to remove duplicate records from transactional file
def remove_duplicates(local_file_path, output_file_path):
    # Load the entire JSON file into a DataFrame
    with open(local_file_path, 'r') as file:
        data = json.load(file)
    
    df = pd.DataFrame(data)
    # Deduplicate based on the entire '_airbyte_data' field
    if '_airbyte_data' in df.columns:
        # Convert '_airbyte_data' dictionary to a string for easier comparison
        df['_airbyte_data_str'] = df['_airbyte_data'].apply(json.dumps)
        
        # Drop duplicates based on the string representation of '_airbyte_data'
        df_deduplicated = df.drop_duplicates(subset=['_airbyte_data_str'], keep='last')
        
        # Drop the temporary string column after deduplication
        df_deduplicated = df_deduplicated.drop(columns=['_airbyte_data_str'])
    else:
        # Fallback to deduplicate on the entire row if '_airbyte_data' is missing
        df_deduplicated = df.drop_duplicates(keep='first')
    
    # Write the deduplicated DataFrame back to a JSON file
    df_deduplicated.to_json(output_file_path, orient='records', indent=4)
    task_logger.info(f"Deduplicated file saved to {output_file_path}")

#Function to calculate recovery_window in mysql table based on job details
def rec_window(job_detail_id):
    JOB_DETAIL = os.getenv("job_detail")
    DATA_SOURCE_ITEM = os.getenv("data_source_item")
    connection = mysql_conn()
    try:
        with connection.cursor() as cursor:

            cursor.execute(f"""
                SELECT dsi.transaction_date , dsi.data_source_id
                FROM {DATA_SOURCE_ITEM} as dsi
                INNER JOIN  {JOB_DETAIL} as jd
                    ON dsi.job_detail_id = jd.id
                WHERE dsi.transaction_date BETWEEN CURDATE() - INTERVAL {recovery_interval} DAY AND CURDATE() 
                    AND dsi.data_source_id = (
                        SELECT DISTINCT(data_source_id)
                        FROM {DATA_SOURCE_ITEM}
                        WHERE job_detail_id = {job_detail_id}
                        )
                GROUP BY dsi.data_source_id, dsi.transaction_date
                HAVING SUM(dsi.records_extracted) = 0;
            """)

            result = cursor.fetchall()
            dates_dict = {}

            for date, data_source_id in result:

                if data_source_id not in dates_dict:
                    # If the job_id is not in the dictionary, add it with the date as the value
                    dates_dict[data_source_id] = [str(date)]
                else:
                    # If the job_id is already in the dictionary, append the date to the existing value
                    dates_dict[data_source_id].append(str(date))

            for data_source_id, dates in dates_dict.items():
                # converting into string before inserting
                date_str = ",".join(dates)

                insert_stmt = f"""
                    UPDATE {JOB_DETAIL}
                    SET recovery_dates = %s
                    WHERE {JOB_DETAIL}.id = %s;
                """
                cursor.execute(insert_stmt, (date_str, job_detail_id))
                task_logger.info("recovery window added")

        cursor.close()
        connection.commit()
    except Error as e:
        task_logger.error(f"Error: {e}")
    finally:
        connection.close()

# Function to read a file from s3, with retry logic in case of failure
def read_file_from_s3(file_path, retry_count=1):
    max_retries = 5
    try:
        response = s3_client.get_object(
            Bucket=os.getenv("Input_bucket"),
            Key=f"{os.getenv('Input_bucket_path')}{file_path}"
        )
        json_data = response["Body"].read().decode("utf-8")
        return pd.read_json(StringIO(json_data), lines=True)

    except Exception as e:
        error_code = e.response["Error"]["Code"]
        # retry if file is missing
        if error_code == "NoSuchKey" and retry_count <= max_retries:
            task_logger.error(f" Attempt {retry_count}: The specified key does not exist in S3.")
            time.sleep(30)
            return read_file_from_s3(file_path, retry_count + 1)
        else:
            task_logger.error(f"Error reading file from S3: {e}")

# Function to read a transaction date's specific file from s3, with retry logic in case of failure
def read_file_from_s3_date(file_path, platform, retry_count=1):
    max_retries = 5
    try:
        if platform in (constants.BRT, constants.BRC, constants.Google_Analytics, constants.Voluum, constants.Apilayer):
            task_logger.info(os.getenv("Output_bucket_path_NC"))
            task_logger.info(f"{os.getenv('Output_bucket_path_NC')}{file_path}")

            response = s3_client.get_object(
                Bucket=os.getenv("Output_bucket"),
                Key=f"{os.getenv('Output_bucket_path_NC')}{file_path}",
            )
        else:
            response = s3_client.get_object(
                Bucket=os.getenv("Output_bucket"),
                Key=f"{os.getenv('Output_bucket_path')}{file_path}",
            )

        json_data = response["Body"].read().decode("utf-8")
        data = json.loads(json_data)
        df = pd.DataFrame(data)
        return df

    except Exception as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchKey" and retry_count <= max_retries:
            task_logger.error(f" Attempt {retry_count}: The specified key does not exist in S3.")
            time.sleep(30)
            return read_file_from_s3_date(file_path, platform, retry_count + 1)
        else:
            task_logger.error(f"Error reading file from S3: {e}")
            return pd.DataFrame()

# Function to write data to S3
def write_data_to_s3(data, file_path, platform):
    if platform in (constants.BRC, constants.Google_Analytics, constants.Voluum, constants.Apilayer,constants.BRT):
        s3_client.put_object(
            Bucket=os.getenv("Output_bucket"),
            Key=f"{os.getenv('Output_bucket_path_NC')}{file_path}",
            Body=json.dumps(data).encode("utf-8"),
        )
    else:
        s3_client.put_object(
            Bucket=os.getenv("Output_bucket"),
            Key=f"{os.getenv('Output_bucket_path')}{file_path}",
            Body=json.dumps(data).encode("utf-8"),
        )

# Function to write empty data to S3 
def write_data_to_s3_recovery(file_path, platform):
    if platform in (constants.BRC, constants.Google_Analytics, constants.Voluum, constants.Apilayer,constants.BRT):
        s3_client.put_object(
            Bucket=os.getenv("Output_bucket"),
            Key=f"{os.getenv('Output_bucket_path_NC')}{file_path}",
            Body=b"",
        )
    else:
        s3_client.put_object(
            Bucket=os.getenv("Output_bucket"),
            Key=f"{os.getenv('Output_bucket_path')}{file_path}",
            Body=b"",
        )

# Function to extract date values from a DataFrame, depending on the platform
def date_values(df, platform):
    # Helper function to convert a timestamp to a string
    def convert_to_string(x):
        return str(x) if isinstance(x, pd.Timestamp) else x

    # Extracts date from each row based on the specified platform
    def extract_date(row):
        if platform in (constants.Q,):
            # Extract transaction date from nested data structure
            date = row["_airbyte_data"]["data"]["transaction_date"].split("T")[0]
            return date
        elif platform in (constants.Apilayer):
            date = row["_airbyte_data"]["start_date"]
            return date
        else:
            date = "date"  # Default value for other platforms

            
        if platform == constants.Voluum:
            try:
                return row["_airbyte_data"]["date"]  # Directly return the date if available
            except Exception as e:
                try:
                    # Attempt to parse postbackTimestamp into a date string
                    return str(
                        datetime.strptime(row["_airbyte_data"]["postbackTimestamp"], "%Y-%m-%d %I:%M:%S %p").date()
                    )
                except Exception as e:
                    task_logger.info("ERROR----->", e)
                    raise

        elif platform == constants.Redtrack:
            if "conv_time" in row["_airbyte_data"]:
                return row["_airbyte_data"]["conv_time"].split("T")[0]
            elif "track_time" in row["_airbyte_data"]:
                return row["_airbyte_data"]["track_time"].split("T")[0]
            elif "created_at" in row["_airbyte_data"]:
                return row["_airbyte_data"]["created_at"].split("T")[0]
            else:
                return datetime.now().strftime("%Y-%m-%d")
            
        elif platform == constants.BRC:
            try:
                # Extract date from post_modified_timestamp
                date = row["_airbyte_data"]["post_modified_timestamp"].split("T")[0]
                return date
            except Exception as e:
                try:
                    # Fallback to post_click_timestamp if previous extraction fails
                    date = row["_airbyte_data"]["post_click_timestamp"].split("T")[0]
                    return date
                except Exception as e:
                    task_logger.info("ERROR----->", e)
                    return None
        elif platform == constants.BRT:
            try:
                # Extract date from post_modified_timestamp
                date = row["_airbyte_data"]["updated_at"].split("T")[0]
                return date
            except Exception as e:
                try:
                    # Fallback to post_click_timestamp if previous extraction fails
                    date = row["_airbyte_data"]["created_at"].split("T")[0]
                    return date
                except Exception as e:
                    task_logger.info("ERROR----->", e)
                    return None
        else:
            # For other platforms, check if 'date' exists in _airbyte_data and return it
            return (
                row["_airbyte_data"][date]
                if "_airbyte_data" in row and date in row["_airbyte_data"]
                else None
            )

    # Filters out rows with internal server errors in the message field
    def filter_errors(row):
        if "_airbyte_data" in row and "message" in row["_airbyte_data"].get("data", {}):
            return row["_airbyte_data"].get("data", {}).get("message") != "Internal server error"
        return True

    # Convert all timestamps in the DataFrame to strings, except for _airbyte_data column
    df = df.apply(lambda x: x.map(convert_to_string) if x.name != "_airbyte_data" else x)
    
    # Apply extract_date function to each row and create a new 'Date' column
    df["Date"] = df.apply(extract_date, axis=1)

    # Filter out rows with errors for specific platforms only
    if platform not in (
            constants.MyAffiliates,
            constants.Income_Access,
            constants.SoftSwiss,
            constants.Google_Analytics,
            constants.Voluum,
            constants.BRC,
            constants.BRT,
            constants.Redtrack,
    ):
        df = df[df.apply(filter_errors, axis=1)]

    # Get unique dates from the 'Date' column and convert formats for Google Analytics platform
    date_values = df["Date"].unique().tolist()
    if platform in (constants.Google_Analytics):
        date_values = [
            datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
            for date in date_values
        ]

    return date_values


def process_data(df, platform):
    def convert_to_string(x):
        return str(x) if isinstance(x, pd.Timestamp) else x

    def extract_date(row):
        if platform in (constants.Q, ):
            date = row["_airbyte_data"]["data"]["transaction_date"].split("T")[0]
            return date
        elif platform in (constants.Apilayer):
            date = row["_airbyte_data"]["start_date"]
            return date
        else:
            date = "date"
        if platform == constants.Voluum:
            try:
                return row["_airbyte_data"]["date"]

            except Exception as e:

                try:
                    return str(
                        datetime.strptime(row["_airbyte_data"]["postbackTimestamp"], "%Y-%m-%d %I:%M:%S %p").date()
                    )

                except Exception as e:
                    task_logger.info("ERROR----->", e)
                    raise
                    
        elif platform == constants.Redtrack:
            if "conv_time" in row["_airbyte_data"]:
                return row["_airbyte_data"]["conv_time"].split("T")[0]
            elif "track_time" in row["_airbyte_data"]:
                return row["_airbyte_data"]["track_time"].split("T")[0]
            elif "created_at" in row["_airbyte_data"]:
                return row["_airbyte_data"]["created_at"].split("T")[0]
            else:
                return datetime.now().strftime("%Y-%m-%d")

        elif platform == constants.BRC:
            try:
                date = row["_airbyte_data"]["post_modified_timestamp"].split("T")[0]
                return date

            except Exception as e:
                try:
                    date = row["_airbyte_data"]["post_click_timestamp"].split("T")[0]
                    return date
                except Exception as e:
                    task_logger.info("ERROR----->", e)
                    raise
        elif platform == constants.BRC:
            try:
                date = row["_airbyte_data"]["updated_at"].split("T")[0]
                return date

            except Exception as e:
                try:
                    date = row["_airbyte_data"]["created_at"].split("T")[0]
                    return date
                except Exception as e:
                    task_logger.info("ERROR----->", e)
                    raise

        else:
            return (
                row["_airbyte_data"][date]
                if "_airbyte_data" in row and date in row["_airbyte_data"]
                else None
            )

    # Filters out rows with internal server errors 
    def filter_errors(row):
        if "_airbyte_data" in row and "message" in row["_airbyte_data"].get("data", {}):
            return row["_airbyte_data"].get("data", {}).get("message") != "Internal server error"
        return True

    task_logger.info(f"platform --->{platform}")

    # Convert all timestamps in the DataFrame to strings, except for _airbyte_data column
    df = df.apply(lambda x: x.map(convert_to_string) if x.name != "_airbyte_data" else x)
    
    # Apply extract_date function to each row and create a new 'Date' column
    df["Date"] = df.apply(extract_date, axis=1)

    # Filter out rows with errors for specific platforms only
    if platform not in (constants.MyAffiliates, constants.Income_Access, constants.SoftSwiss,
                        constants.Google_Analytics, constants.Voluum, constants.BRC, constants.Redtrack,  constants.BRT):
        df = df[df.apply(filter_errors, axis=1)]

    task_logger.info(f"platform --->{platform}")

    grouped_df = df.groupby("Date")  # Group DataFrame by 'Date'
    
    processed_data = {}  # Initialize dictionary to hold processed data

    for date, group in grouped_df:  # Iterate over each group of dates
        group = group.drop(columns=["Date"])  # Drop 'Date' column from group for processing
        
        data_dict = group.to_dict(orient="records")  # Convert group DataFrame to list of dictionaries

        # Format dates specifically for Google Analytics platform
        if platform in (constants.Google_Analytics,):
            date_obj = datetime.strptime(date, "%Y%m%d")  # Parse the original date format
            new_date_str = date_obj.strftime("%Y-%m-%d")  # Convert to desired string format (YYYY-MM-DD)
            processed_data[new_date_str] = data_dict  # Store processed data with formatted date as key
        else:
            processed_data[date] = data_dict  # Store processed data with original date as key   
    return processed_data  # Return dictionary of processed data grouped by dates

def bucket_files(bucket_name):
    """
    List files from a specific S3 bucket.
    
    Parameters:
        bucket_name (str): The name of the bucket to list files from.
    
    Returns:
        list: A list of file keys in the specified bucket.
    """
    task_logger.info(f'Input_bucket: {os.getenv("Input_bucket")}')
    task_logger.info(f'env: {os.getenv("Input_bucket_path")}')   
    # List objects in the specified S3 bucket with a prefix
    response = s3_client.list_objects(
        Bucket=os.getenv("Input_bucket"),
        Prefix=f"{os.getenv('Input_bucket_path')}{bucket_name}",
    )

    files = []  # Initialize an empty list to store file keys
    # Iterate through the contents of the response
    for content in response.get("Contents", []):
        task_logger.info(f'file:{content.get("Key")}')
        files.append(content.get("Key"))

    return files


def write_local_record(file_path, data):
    """
    Write data to a local storage file.
    
    Parameters:
        file_path (str): The path where the data should be written.
        data (dict or list): The data to write to the file.
    """
    parent = os.path.dirname(str(file_path))  # Get the parent directory of the file path
    
    # Create parent directory if it does not exist
    if not os.path.exists(parent):
        os.makedirs(parent)

    strdata = json.dumps(data)  # Convert data to JSON string format
    
    # remove last character - ] at end. this will be added once all records added to this file.
    if "]" == strdata[len(strdata) - 1]:
        strdata = strdata[:-1]

    # if first time record writing to file then do not remove first char that is [ ,
    # otherwise remove first char - [ and replace it will comma - ,
    if os.path.exists(file_path):
        if "[" == strdata[0]:
            strdata = "," + strdata[1:]

    file1 = open(file_path, "a")
    file1.write(strdata)
    file1.close()


def finalize_local_files(parent):
    """
    Finalize local JSON files by appending a closing bracket.
    
    Parameters:
        parent (str): The parent directory containing JSON files to finalize.
    """
    task_logger.info(f"finalize_local_files() parent: {parent}")

    for path, subdirs, files in os.walk(parent):
        for file in files:
            if ".json" in file:
                filepath = os.path.join(path, file)
                task_logger.info(f"finalize_local_files() filepath: {filepath}")
                file1 = open(filepath, "a")  # append mode
                file1.write("]")
                file1.close()


def get_local_location(platform, job_id,stream):
    """
    Build a local storage location from given parameters.
    
    Parameters:
        platform (str): The platform identifier.
        job_id (str or int): The job identifier.
        stream (str): The stream identifier.
    
    Returns:
        str: The constructed local storage path.
    """
    local_efs_path = os.getenv("local_efs_mount_file_path")
    return local_efs_path + platform + "_" + str(job_id) + "/"+stream+"/"


def delete_file_from_efs(directory_path):
    """
    Delete all files from EFS after use.
    
    Parameters:
        directory_path (str): The path of the directory to delete.
    """

    # Check if the directory exists
    if os.path.exists(directory_path) and os.path.isdir(directory_path):
        try:
            shutil.rmtree(directory_path)  # Remove the directory and its contents
            task_logger.info(f"Deleted directory: {directory_path}")
        except Exception as e:
            task_logger.error(f"Failed to delete {directory_path}. Reason: {e}")

    else:
        task_logger.error(f"The path {directory_path} does not exist or is not a directory.")


def get_local_transaction_filename(date):
    """
    Build transaction filename from given date for local storage.
    
    Parameters:
        date (str): The date used to construct the filename.
    
    Returns:
        str: The constructed filename (e.g., "YYYY-MM-DD.json").
    """
    return f"{date}.json"


def get_s3_transaction_filename(date):
    """
    Build transaction filename from given date for S3 storage.
    
    Parameters:
        date (str): The date used to construct the filename.
    
    Returns:
        str: The constructed filename with timestamp (e.g., "YYYY-MM-DD_HH:MM:SS.json").
    """
    time_string = datetime.now().strftime("%H:%M:%S")
    return f"{date}_{time_string}.json"


def get_transaction_parent(date):
    """
    Build transaction parent's directory name from given date for local and S3 storage.
    
    Parameters:
        date (str): The date used to construct the parent directory name.
    
    Returns:
        str: The constructed parent directory name (e.g., "YYYY/MM/DD").
    """
    return date.replace("-", "/")


def get_local_file_path(local_location, date):
    """
   Build local file path from given parameters for storing locally and uploading to S3.

   Parameters:
       local_location (str): Base local location where files are stored.
       date (str): Date used for constructing the file name.

   Returns:
       str: Full local file path for storing data.
    """
    file_name = get_local_transaction_filename(date=date)  # Get transaction filename based on date
    date_path = get_transaction_parent(date=date)  # Get parent directory structure based on date
    return os.path.join(os.path.dirname(local_location), date_path, file_name).replace("\\", "/")  
    # Construct full path and ensure forward slashes


def upload_file_local_to_s3(local_filepath, s3_file_path, platform):
    """
   Uploads a local file to an S3 location.

   Parameters:
       local_filepath (str): Path of the local file to upload.
       s3_file_path (str): Destination path in S3 where the file will be uploaded.
       platform (str): Platform identifier used to determine upload behavior.

   Raises:
       Exception: If there is an error during upload, it will be logged by task_logger.
    """
    if platform in (constants.BRC, constants.Google_Analytics, constants.Voluum, constants.Apilayer,  constants.BRT):
        with open(local_filepath, "rb") as f:
            s3_client.upload_fileobj(
                f,
                os.getenv("Output_bucket"),
                f"{os.getenv('Output_bucket_path_NC')}{s3_file_path}",
            )

    else:
        with open(local_filepath, "rb") as f:
            s3_client.upload_fileobj(
                f,
                os.getenv("Output_bucket"),
                f"{os.getenv('Output_bucket_path')}{s3_file_path}",
            )


def process_files_data(filepath, job_id, sync_window, platform,stream):
    """
        process bucket`s files and generates output files at local.`
    """
    task_logger.info(f"process_files_data() filepath:{filepath}")
    bucketpath = filepath[: filepath.rfind("/")]
    task_logger.info(f"process_files_data() bucketpath:{bucketpath}")
    files = bucket_files(bucketpath)
    task_logger.info(f"process_files_data() filecount:{len(files)}")
    global_date_values = []
    global_missing_date_values = []
    global_record_count = {"00": 0}
    local_location = get_local_location(platform=platform, job_id=job_id,stream=stream)

    if not os.path.exists(local_location):
        os.makedirs(local_location)
    task_logger.info(f"local_location: {local_location}")

    environment = os.getenv("Input_bucket_path")
    task_logger.info(f"environment: {environment}")

    for f_path in files:
        env_position = f_path.find(environment)
        task_logger.info(f"env_position: {env_position}")
        if env_position != -1:
            file_path = f_path[len(environment):]

        task_logger.info(f"process_files_data() processing file file_path: {file_path}")
        df = read_file_from_s3(file_path)

        # current_date_time = datetime.now().strftime("%Y-%m-%d_%I:%M:%S_%p")
        # insert_position = file_path.rfind("/")
        # archive_path = (
        #         file_path[:insert_position]
        #         + f"/{current_date_time}"
        #         + file_path[insert_position:]
        # )
        # move_object(
        #     f"{os.getenv('Input_bucket')}",
        #     f"{os.getenv('Input_bucket_path')}{file_path}",
        #     f"{os.getenv('Archive_bucket')}",
        #     f"{os.getenv('Archive_bucket_path')}{archive_path}",
        # )

        processed_data = process_data(df, platform)
        date_values_list = date_values(df, platform)
        # collect date value to global list
        global_date_values.extend(date_values_list)

        list_set = set(global_date_values)
        global_date_values = list(list_set)

        missing_values = [value for value in sync_window if value not in date_values_list]

        # collect missing date values to global list
        global_missing_date_values.extend(missing_values)
        list_set = set(global_missing_date_values)
        global_missing_date_values = list(list_set)
        task_logger.info(f"missing_values -->{missing_values} file-{file_path}")

        # write date wise records to local structure first.once all process completed upload to s3
        for date, data in processed_data.items():
            # Construct file path with forward slashes
            trans_local_file_path = get_local_file_path(local_location=local_location, date=date)
            # Write data to local storage
            task_logger.info(f"trans_local_file_path :{trans_local_file_path}")

            write_local_record(trans_local_file_path, data)

            datalen = len(data) if df is not None else 0

            task_logger.info(f"{date} -- data_length:{datalen}")
            num_records = global_record_count.get(date)

            if num_records is not None:
                global_record_count[date] = num_records + datalen
            else:
                global_record_count[date] = datalen
    return global_date_values, global_missing_date_values, global_record_count


def data_source_item(ti, platform, task_name, operator_id):
    try:
        ti.xcom_push(key="job_execute_step", value="S3")
        # Fetch environment variable for data_source_item table
        DATA_SOURCE_ITEM = os.getenv("data_source_item")

        # Establish a connection to MySQL
        connection = mysql_conn()
        cursor = connection.cursor()
 
        job_and_configs_startdate = ti.xcom_pull(key="job_startdate", task_ids=task_name)
        task_logger.info(f"job_and_configs_startdate --->{job_and_configs_startdate}")
 
        # Split the job and config details from the XCom string
        job_ids = []
        config_ids = []
        startdate = []

        parts = job_and_configs_startdate.split("/")
        if len(parts) == 3:
            job_ids.append(int(parts[0]))
            config_ids.append(parts[1])
            startdate.append(parts[2])
        else:
            task_logger.warn(f"Invalid format: {parts}")
 
        data_source_list = []
        data_source_list = fetch_connection_data_source_table(platform, operator_id)
        task_logger.info(f"data_source_list ----> {data_source_list}")
 
        # Process data sources for Voluum and BRC platforms
        if platform in (constants.Voluum, constants.BRC,  constants.BRT):
            task_logger.info(f"platform ----> {platform}")
            for item in data_source_list:
                #item[0] = id, item[1] = operator_id, item[2] = source_name, item[3] =airbyte_connection_id,item[4] = path from data source table
                conn_id = item[3]
                incremental_streams, full_refresh_streams = get_inc_full_refresh_streams(conn_id)

            incremental_data_source_list = []
            full_refresh_data_source_list = []

            for item in data_source_list:
                stream = item[2]
                if stream in incremental_streams:
                    incremental_data_source_list.append(item)
                elif stream in full_refresh_streams:
                    full_refresh_data_source_list.append(item)
 
            # Process full refresh data sources
            for item in full_refresh_data_source_list:
                filepath = item[4]
                task_logger.info(f"process_files_data() filepath: {filepath}")
                bucketpath = filepath[: filepath.rfind("/")]
                task_logger.info(f"process_files_data() bucketpath: {bucketpath}")
                files = bucket_files(bucketpath)

                # for full refresh stream, we need to archive all old data
                output_bucket = os.getenv("Output_bucket_DQ")
                archive_all_files_in_s3_folder(output_bucket, bucketpath)

                # Process each file in the full refresh stream
                for f_path in files:
                    task_logger.info(f"the f_path is {f_path} ")
                    path_parts = f_path.split("/")
                    new_path = "/".join(path_parts[1:])
                    load_full_refresh_into_s3(new_path)

            data_source_list = incremental_data_source_list

        valid_job_ids = []
        acc_list = []
        acc_list_for_config_ids = []
        for job_id, config_id in zip(job_ids, config_ids):
            acc_list_for_config_id = [
                (item[0], item[2], item[4], item[3])
                for item in data_source_list
                if item[3] == config_id
            ]

            if acc_list_for_config_id:
                acc_list_for_config_ids.append(acc_list_for_config_id)
                valid_job_ids.extend([job_id] * len(acc_list_for_config_id))
            else:

                task_logger.info(
                    "No matching connection ID found for config ID {} Skipping job ID {}.".format(config_id, job_id)
                )

        acc_list = [item for sublist in acc_list_for_config_ids for item in sublist]
        

        for job_id, acc in zip(valid_job_ids, acc_list):

            file_path = acc[2]
            stream_name = acc[1]
            local_location = get_local_location(platform=platform, job_id=job_id,stream=stream_name)

            job_details = fetch_Jobdetail_and_records_extracted(job_id, stream_name)
            job_detail_id = int(job_details[0][0]) if job_details else 0
            records_extracted = int(job_details[0][1]) if job_details else 0
            sync_window = job_details[0][2].split(", ") if job_details else None
            task_logger.info(f"sync_window --->{sync_window}")
            task_logger.info(f"file_path --->{file_path}")

            if records_extracted != 0 and job_detail_id != 0:
                date_values, missing_date_values, record_count = process_files_data(
                    file_path, job_id, sync_window, platform,stream_name
                )

                task_logger.info(f"date_values length---->{len(date_values)}")
                task_logger.info(f"missing_date_values length---->{len(missing_date_values)}")
                task_logger.info(f"record_count length--->{len(record_count)}")

                finalize_local_files(local_location)

                if missing_date_values:
                    for date in missing_date_values:
                        if date not in date_values and date is not None:
                            time_string = datetime.now().strftime("%H:%M:%S")
                            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            date_string  = datetime.now().strftime("%Y-%m-%d")
                            file_name = f"{date}_{date_string}_{time_string}.json"
                            date_path = date.replace("-", "/")
                            file_path = os.path.join(os.path.dirname(acc[2]), date_path, file_name).replace("\\", "/")
                            task_logger.info(file_path)
                            write_data_to_s3_recovery(file_path, platform)

                            data_source_insert_query = f"""
                                INSERT INTO {DATA_SOURCE_ITEM}
                                    (job_id, job_detail_id, data_source_id, path, transaction_date, records_extracted, 
                                    status, created_at, last_updated_at)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            data_source_insert_values = (job_id, job_detail_id, acc[0], file_path, date,
                                                         0, "pending", current_time, current_time)

                            task_logger.info(
                                f"Executing query for missing_date_values: {data_source_insert_query} with values: {data_source_insert_values}"
                            )

                            cursor.execute(data_source_insert_query, data_source_insert_values)
                            task_logger.info("values inserted in data_source_item where date is missing")

                task_logger.info("all missing date values inserted into data_source_item")
                task_logger.info(f"The date values are {date_values}")
                for date in date_values:
                    if date is not None:
                        time_string = datetime.now().strftime("%H:%M:%S")
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        date_string  = datetime.now().strftime("%Y-%m-%d")
                        file_name = f"{date}_{date_string}_{time_string}.json"
                        date_path = date.replace("-", "/")

                        # # Construct file path with forward slashes
                        file_path = os.path.join(
                            os.path.dirname(acc[2]), date_path, file_name
                        ).replace("\\", "/")

                        local_file_path = get_local_file_path(
                            local_location=local_location, date=date
                        )
                        ## deduplication using a method
                        remove_duplicates(local_file_path,local_file_path)
                        # # Write data to S3
                        task_logger.info(f"local file path: {local_file_path} s3_file_path_write --->{file_path}")

                        upload_file_local_to_s3(
                            local_filepath=local_file_path,
                            s3_file_path=file_path,
                            platform=platform,
                        )

                        task_logger.info(
                            "Processed data for job_id {} and date {} written to {}".format(job_id, date, file_path)
                        )
                        data_source_insert_query = f"""
                            INSERT INTO {DATA_SOURCE_ITEM}
                                (job_id, job_detail_id, data_source_id, path, transaction_date, records_extracted, 
                                status, created_at, last_updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """
                        data_source_insert_values = (job_id, job_detail_id, acc[0], file_path, date,
                                                        record_count[date], "pending", current_time, current_time)

                        task_logger.info(
                            f"Executing query for date_values: {data_source_insert_query} with values: {data_source_insert_values}"
                        )
                        cursor.execute(data_source_insert_query, data_source_insert_values)
                        task_logger.info("values inserted in data_source_item")

            else:
                if sync_window is not None:
                    for sync_date in sync_window:
                        if sync_date is not None:
                            time_string = datetime.now().strftime("%H:%M:%S")
                            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            date_string  = datetime.now().strftime("%Y-%m-%d")
                            file_name = f"{sync_date}_{date_string}_{time_string}.json"
                            date_path = sync_date.replace("-", "/")
                            file_path = os.path.join(os.path.dirname(acc[2]), date_path, file_name).replace("\\", "/")
                            task_logger.info("{}".format(file_path))

                            write_data_to_s3_recovery(file_path, platform)
                            task_logger.info(sync_date)

                            data_source_insert_query = f""" 
                                INSERT INTO {DATA_SOURCE_ITEM}
                                    (job_id, job_detail_id, data_source_id, path, transaction_date, records_extracted, 
                                    status, created_at, last_updated_at)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                            data_source_insert_values = (job_id, job_detail_id, acc[0], file_path, sync_date,
                                                        0, "pending", current_time, current_time)
                            task_logger.info(
                                f"Executing query for date_values: {data_source_insert_query} with values: {data_source_insert_values}"
                            )
                            task_logger.info("values inserted in data_source_item where records are missing")
                            cursor.execute(data_source_insert_query, data_source_insert_values)

                            task_logger.info("file have no records")
            delete_file_from_efs(local_location)
            connection.commit()
            rec_window(job_detail_id)
        connection.commit()
        

    except Exception as e:
        raise AirflowException(f"Error {e} .")

    finally:
        connection.close()


