import os
import json
import boto3
import time
import sys
from datetime import datetime
import logging
import polars as pl

from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException
from airflow.exceptions import AirflowException

from airbyte.db_connection import mysql_conn
from Utils import Utils
from dq2utils import (
    primary_key_validation,
    schema_validation,
    get_schema,
)
from fetch_connection_list import (
    fetch_Jobdetail_id,
    fetch_connection_data_source_table,
    fetch_recovery_connection_data_source_table,
    fetch_Jobdetail_and_records_extracted,
    insert_into_s3_file_stats,
    fetch_unprocessed_dbt_files,
    delete_unprocessed_dbt_files
)
from airbyte.fetch_streams import (
    get_inc_full_refresh_streams,
    load_full_refresh_into_s3,
)
from airbyte import constants
from slack_alerts import get_slack_flags, consoliated_dq_fail_slack_alert

task_logger = logging.getLogger("airflow.task")
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
)

#copying object from transaction_bucket to final bucket
def copy_object(source_bucket, source_key, destination_bucket, destination_key):
    try:
        s3_client.copy_object(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
        )
        task_logger.info(
            f"Successfully copied {source_key} from {source_bucket} to {destination_key} in {destination_bucket}"
        )
    except Exception as e:
        task_logger.info(f"Failed to copy object: {e}")


def write_data_to_s3(data, file_path, platform):
    if platform in (constants.BRC, constants.Google_Analytics, constants.Voluum, constants.Apilayer,  constants.BRT):
        s3_client.put_object(
            Bucket=os.getenv("Output_bucket_DQ"),
            Key=f"{os.getenv('Output_bucket_path_DQ_NC')}{file_path}",
            Body=json.dumps(data).encode("utf-8"),
        )
    else:
        s3_client.put_object(
            Bucket=os.getenv("Output_bucket_DQ"),
            Key=f"{os.getenv('Output_bucket_path_DQ')}{file_path}",
            Body=json.dumps(data).encode("utf-8"),
        )


def dq(ti, platform, task_name,is_reprocess, operator_id):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
    )
    
    try:
        platform_name = platform
        
        '''The method insert_missing_data_into_daily_average_record_count identifies missing transaction dates from the past 30 days in the AVERAGE_RECORDS_COUNT_HISTORY table, calculates the average records_extracted for each missing date (excluding zero values),
        and inserts the calculated average into the AVERAGE_RECORDS_COUNT_HISTORY table if the record doesn't already exist.'''
        Utils.insert_missing_data_into_daily_average_record_count(operator_id=operator_id)
        
        DATA_SOURCE_ITEM = os.getenv("data_source_item")
        DATA_SOURCE_DQ = os.getenv("data_source_dq")
        DATA_QUALITY_RULE = os.getenv("data_quality_rule")
        DATA_QUALITY_ISSUE = os.getenv("data_quality_issue")
        AVERAGE_RECORDS_COUNT_HISTORY = os.getenv("average_records_count_history")
        JOB = os.getenv("jobs")
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        connection = mysql_conn()
        cursor = connection.cursor(dictionary=True)

        job_and_configs_startdate = ti.xcom_pull(key="job_startdate", task_ids=task_name)
        stream_state = ti.xcom_pull(key="stream_state", task_ids=task_name)
        task_logger.info(f"job_and_configs_startdate --->{job_and_configs_startdate}")

        '''Seperate job_id ,config_id and start_date from a single string from xcom'''
        job_ids = []
        config_ids = []
        start_date = []

        parts = job_and_configs_startdate.split("/")
        if len(parts) == 3:
            job_ids.append(int(parts[0]))
            config_ids.append(parts[1])
            start_date.append(parts[2])
        else:
            task_logger.warn(f"Invalid format: {parts}")

        task_logger.info(f"job_ids are --->{job_ids}")

        data_source_list = []
        '''In each DAG we are maintaining a flag called is_reprocess, The value is True for the reprocess dags and value is False for primary dags
            based on that flag we are getting the required connection details from the DATA_SOURCE table''' 
        if is_reprocess:
            data_source_list = fetch_recovery_connection_data_source_table(platform, operator_id)
        else:
            data_source_list = fetch_connection_data_source_table(platform, operator_id)
        task_logger.info(f"data_source_list ----> {data_source_list}")

        '''This if block of code processes a list of data sources (data_source_list) based on the platform (either Voluum or BRC). 
        It divides the data sources into two categories: incremental and full refresh streams. Then, depending on whether it's a reprocess (is_reprocess), it loads data for full refresh streams into an S3 bucket. 
        Finally, it filters the data sources to include only the incremental ones for further processing'''
        if platform in (constants.Voluum, constants.BRC, constants.BRT):
            task_logger.info(f"platform ----> {platform}")
            for item in data_source_list:
                conn_id = item[3]
                inc_streams, full_refresh_streams = get_inc_full_refresh_streams(conn_id)

            incremental_data_source_list = []
            full_refresh_data_source_list = []

            for item in data_source_list:
                stream = item[2]
                if stream in inc_streams:
                    incremental_data_source_list.append(item)
                elif stream in full_refresh_streams:
                    full_refresh_data_source_list.append(item)
            # if not is_reprocess:
            #     for item in full_refresh_data_source_list:
            #         path = item[4]
            #         load_full_refresh_into_s3(path)
                
            data_source_list = incremental_data_source_list
        
        valid_job_ids = []
        acc_list = []
        acc_list_for_config_ids = []
        for job_id, config_id in zip(job_ids, config_ids):
            acc_list_for_config_id = [(item[0], item[2], item[4], item[3]) for item in data_source_list if
                                    item[3] == config_id]
            if acc_list_for_config_id:
                acc_list_for_config_ids.append(acc_list_for_config_id)
                valid_job_ids.extend([job_id] * len(acc_list_for_config_id))
            else:

                task_logger.info("No matching connection ID found for config ID {} Skipping job ID {}.".format(config_id, job_id))
                
        acc_list = [item for sublist in acc_list_for_config_ids for item in sublist]
      
        if stream_state == "Fullrefresh":
            '''This SQL query retrieves data quality rule details and associated metadata for data sources, 
            filtering for successful jobs, and extracting the stream name from the path.'''
            sql = f"""
                SELECT path,
                    0 AS avg_records_extracted,
                    d.data_quality_rule_id, 
                    i.id AS data_source_item_id,
                    r.name AS data_quality_rule_name,
                    r.threshold,
                    r.isenabled,
                    i.transaction_date,
                    j.operator_id,
                    j.job_id,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) AS stream_name,
                    r.expression,
                    r.severity,
                    r.schema_id,
                    i.data_source_id
                FROM {DATA_SOURCE_ITEM} i 
                INNER JOIN {DATA_SOURCE_DQ} d 
                    ON i.data_source_id = d.data_source_id 
                INNER JOIN {DATA_QUALITY_RULE} r 
                    ON d.data_quality_rule_id = r.id 
                INNER JOIN (
                    SELECT job_id, operator_id
                    FROM {JOB}
                    WHERE status = 'succeeded'
                ) j
                    ON i.job_id = j.job_id
                WHERE i.job_id IN (%s);
            """
        else:
            task_logger.info("incremental stream")
            '''This SQL query retrieves data quality rule details, average records extracted, and associated metadata for data sources,
             filtering for successful jobs and extracting the stream name from the path'''
            sql = f"""
                SELECT path,
                    COALESCE(daily_avg.avg_records_extracted, 0) AS avg_records_extracted,
                    d.data_quality_rule_id,
                    i.id AS data_source_item_id,
                    r.name AS data_quality_rule_name,
                    r.threshold,
                    r.isenabled,
                    i.transaction_date,
                    j.operator_id,
                    j.job_id,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(path, '/', 4), '/', -1) AS stream_name,
                    r.expression,
                    r.severity,
                    r.schema_id,
                    i.data_source_id
                FROM {DATA_SOURCE_ITEM} i 
                LEFT JOIN {DATA_SOURCE_DQ} d 
                    ON i.data_source_id = d.data_source_id 
                LEFT JOIN {DATA_QUALITY_RULE} r 
                    ON d.data_quality_rule_id = r.id 
                LEFT JOIN (
                    SELECT job_id, operator_id
                    FROM {JOB}
                    WHERE status = 'succeeded'
                ) j
                    ON i.job_id = j.job_id
                LEFT JOIN {AVERAGE_RECORDS_COUNT_HISTORY} daily_avg
                    ON i.data_source_id = daily_avg.data_source_id 
                    AND i.transaction_date = daily_avg.transaction_date
                WHERE i.job_id IN (%s);
            """
        placeholders = ",".join(["'%s'" for _ in job_ids])
        query = sql % placeholders

        cursor.execute(query, job_ids)
        result = cursor.fetchall()
        task_logger.info(result)
        result_df = pl.DataFrame(result, strict=False)
        
        # Handle empty DataFrame case
        if result_df.is_empty():
            task_logger.info("No data found for the provided job_ids. Skipping data quality checks.")
            data_file_paths = []
        else:
            data_file_paths = result_df["path"].unique().to_list()

        task_logger.info(result_df.to_dicts())

        operator_dq_rules_dict = {}
        
        primary_key_validation_status = ""
        schema_validation_status = ""

        source_bucket = os.getenv('Output_bucket')
        destination_bucket = os.getenv('Output_bucket_DQ')
        # For adding into Snowflake control table
        s3_files_stats = []

        hard_fail_flag = False

        for file_path in data_file_paths:

            hard_fail_flag = False
            task_logger.info(f"platform --->{platform_name}")

            # getting path for non-casino platforms
            if platform_name in (constants.BRC, constants.Google_Analytics, constants.Voluum, constants.Apilayer,  constants.BRT):
                output_bucket_path = os.getenv("Output_bucket_path_NC")
                output_bucket_path_dq = os.getenv("Output_bucket_path_DQ_NC")
                task_logger.info(f"Output_bucket_path_DQ --->{output_bucket_path_dq}")
            # getting  path for casino platforms'''
            else:
                output_bucket_path = os.getenv("Output_bucket_path")
                output_bucket_path_dq = os.getenv("Output_bucket_path_DQ")
            
            source_key = f"{output_bucket_path}{file_path}"
            destination_key = output_bucket_path_dq + file_path

            task_logger.info(f"path --->{file_path}")
            task_logger.info(f"Output_bucket_path --->{source_key}")
            # Fetching the object from S3 bucket
            response = s3_client.get_object(
                Bucket=os.getenv("Output_bucket"),
                Key=source_key,
            )
            #Getting the content length of the fetched object
            content_length = response.get("ContentLength")
            task_logger.info(f"content_length in bytes--->{content_length}")
            
            if content_length > 0:
                response_body = response["Body"].read().decode("utf-8")
                data = json.loads(response_body)
                df = pl.DataFrame(data, strict=False, infer_schema_length=None).unnest("_airbyte_data") 
                row_count = df.height

            # start DQ
            for row in result_df.filter(pl.col("path") == file_path).to_dicts():
                snowflake_details_dict = {}
                try:
                    data_quality_rule_id = row["data_quality_rule_id"]
                    s3_path = row["path"]
                    txn_dt = row["transaction_date"]
                    operator_id = row["operator_id"]
                    job_id = row["job_id"]
                    stream_name = row["stream_name"]
                    severity = row["severity"]
                    threshold = row["threshold"]
                    schema_id = row["schema_id"]
                    data_source_id = row["data_source_id"]
                    platform = str.lower(platform)

                    snowflake_details_dict['job_id'] = job_id
                    snowflake_details_dict['data_source_id'] = data_source_id
                    snowflake_details_dict['tracker_login_id'] = operator_id
                    snowflake_details_dict['transaction_date'] = txn_dt
                    snowflake_details_dict['path'] = s3_path
                    task_logger.info(f"snowflake_details_dict --->{snowflake_details_dict}")
                    
                    # Bypass the DQ if no data_quality_rule_id is applied for the stream
                    if not data_quality_rule_id:
                        task_logger.info("No DQ rule applied.")
                        continue
                    else:
                        # Dictionary to store failing dq rule's txn dates per operator_id
                        if operator_id not in operator_dq_rules_dict:
                            operator_dq_rules_dict[operator_id] = {
                                "job_id": job_id,
                                "ZERO_RECORD_COUNT": {},
                                "LOW_RECORD_COUNT": {},
                                "HIGH_RECORD_COUNT": {},
                                "SCHEMA_VALIDATION": {},
                                "PRIMARY_KEY_VALIDATION": {},
                            }

                        if stream_name not in operator_dq_rules_dict[operator_id]["ZERO_RECORD_COUNT"]:
                            operator_dq_rules_dict[operator_id]["ZERO_RECORD_COUNT"][stream_name] = []

                        if stream_name not in operator_dq_rules_dict[operator_id]["LOW_RECORD_COUNT"]:
                            operator_dq_rules_dict[operator_id]["LOW_RECORD_COUNT"][stream_name] = []

                        if stream_name not in operator_dq_rules_dict[operator_id]["HIGH_RECORD_COUNT"]:
                            operator_dq_rules_dict[operator_id]["HIGH_RECORD_COUNT"][stream_name] = []

                        if stream_name not in operator_dq_rules_dict[operator_id]["SCHEMA_VALIDATION"]:
                            operator_dq_rules_dict[operator_id]["SCHEMA_VALIDATION"][stream_name] = {}

                        if stream_name not in operator_dq_rules_dict[operator_id]["PRIMARY_KEY_VALIDATION"]:
                            operator_dq_rules_dict[operator_id]["PRIMARY_KEY_VALIDATION"][stream_name] = {}

                        if content_length == 0:
                            if row["data_quality_rule_name"] == "ZERO_RECORD_COUNT" and row["isenabled"] == "True":
                                if severity == "High":
                                    hard_fail_flag = True

                                task_logger.info("Failed for ZERO_RECORD_COUNT")
                                status = "Failed"

                                insert_query = f"""
                                    INSERT INTO {DATA_QUALITY_ISSUE} 
                                        (data_quality_rule_id, data_source_item_id, description, severity, status, threshold) 
                                    VALUES (%s, %s, %s, %s, %s, %s);
                                """
                                insert_values = (row["data_quality_rule_id"], row["data_source_item_id"], "Zero Record Count", severity, status, threshold)
                                cursor.execute(insert_query, insert_values)

                                operator_dq_rules_dict[operator_id]["ZERO_RECORD_COUNT"][stream_name].append(txn_dt)
                            
                            # Skip all other DQ rules for this file since content_length is 0
                            task_logger.info(f"Skipping all other DQ rules for file {file_path} due to zero content length")
                            break

                        task_logger.info(f"Rule_name --->{row['data_quality_rule_name']}")

                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        # If the rule is HIGH_RECORD_COUNT, is_enabled=True and stream is incremental
                        if row["data_quality_rule_name"] == "LOW_RECORD_COUNT" and row["isenabled"] == "True" and stream_state == "Incremental":
                            threshold_percentage = (100 - row["threshold"]) / 100
                            min_row_count = int(row["avg_records_extracted"] * threshold_percentage)
                            task_logger.info(f"min_row_count -->{min_row_count}")
                            
                            task_logger.info(f"row_count -->{row_count}")
                            validation_result = row_count >= min_row_count
                            
                            if validation_result:
                                status = "Pass"

                                insert_query = f"""
                                    INSERT INTO {DATA_QUALITY_ISSUE}
                                        (data_quality_rule_id, data_source_item_id, description, severity, status, threshold)
                                    VALUES (%s, %s, %s, %s, %s,%s);
                                """
                                insert_values = (row["data_quality_rule_id"], row["data_source_item_id"], "Low Record Count", severity, status, threshold)
                                cursor.execute(insert_query, insert_values)
                            else:
                                status = "Failed"

                                if severity == "High":
                                    hard_fail_flag = True

                                insert_query = f"""
                                    INSERT INTO {DATA_QUALITY_ISSUE}
                                        (data_quality_rule_id, data_source_item_id, description, severity, status, threshold)
                                    VALUES (%s, %s, %s, %s, %s, %s);
                                """
                                insert_values = (row["data_quality_rule_id"], row["data_source_item_id"], "Low Record Count", severity, status, threshold)
                                cursor.execute(insert_query, insert_values)

                                operator_dq_rules_dict[operator_id]["LOW_RECORD_COUNT"][stream_name].append(txn_dt)

                        # if the rule is HIGH_RECORD_COUNT, is_enabled = True and stream is incremental
                        if row["data_quality_rule_name"] == "HIGH_RECORD_COUNT" and row["isenabled"] == "True" and stream_state == "Incremental":

                            threshold_percentage = (100 + row["threshold"]) / 100
                            max_row_count = int(row["avg_records_extracted"] * threshold_percentage)
                            task_logger.info(f"max_row_count -->{max_row_count}")
                            task_logger.info(f"row_count -->{row_count}")
                            validation_result = row_count <= max_row_count

                            if validation_result:
                                status = "Pass"

                                insert_query = f"""
                                    INSERT INTO {DATA_QUALITY_ISSUE}
                                        (data_quality_rule_id, data_source_item_id, description, severity, status, threshold)
                                    VALUES (%s, %s, %s, %s, %s, %s);
                                """
                                insert_values = (row["data_quality_rule_id"], row["data_source_item_id"], "High Record Count", severity, status, threshold)
                                cursor.execute(insert_query, insert_values)
                            else:
                                status = "Failed"
                                if severity == "High":
                                    hard_fail_flag = True

                                insert_query = f"""
                                    INSERT INTO {DATA_QUALITY_ISSUE}
                                        (data_quality_rule_id, data_source_item_id, description, severity, status, threshold)
                                    VALUES (%s, %s, %s, %s, %s, %s);
                                """
                                insert_values = (row["data_quality_rule_id"], row["data_source_item_id"], "High Record Count", severity, status, threshold)
                                cursor.execute(insert_query, insert_values)

                                operator_dq_rules_dict[operator_id]["HIGH_RECORD_COUNT"][stream_name].append(txn_dt)

                        # if the rule is ZERO_RECORD_COUNT, is_enabled=True and stream is incremental
                        if row["data_quality_rule_name"] == "ZERO_RECORD_COUNT" and row["isenabled"] == "True" and stream_state == "Incremental":
                            status = "Pass"
                            insert_query = f"""
                                INSERT INTO {DATA_QUALITY_ISSUE}
                                    (data_quality_rule_id, data_source_item_id, description, severity, status, threshold)
                                VALUES (%s, %s, %s, %s, %s, %s);
                            """
                            insert_values = (row["data_quality_rule_id"], row["data_source_item_id"], "Zero Record Count", severity, status, threshold)
                            cursor.execute(insert_query, insert_values)
                                        
                        platform = str.lower(platform)
                        
                        if row["data_quality_rule_name"] == "SCHEMA_VALIDATION" or row["data_quality_rule_name"] == "PRIMARY_KEY_VALIDATION":
                            # Skip schema validation and primary key validation if schema_id is None
                            if schema_id is None or row["isenabled"] == "False":
                                task_logger.info(f"Skipping schema_val, pk_val due to schema_id being None or isenabled being False for stream -{stream_name}")
                                continue
                            
                            if platform_name == constants.Voluum and stream_name in constants.Voluum_disabled_streams:
                                task_logger.info(f"Skipping schema_val, pk_val ---->  stream -{stream_name}")
                                continue
                            # Getting schema from MASTER_JSON_TABLE based on schema_id
                            json_schema_string = get_schema(schema_id)
                            task_logger.info(json_schema_string)

                            if json_schema_string:
                                outer_schema = json.loads(json_schema_string)
                                schema_read = outer_schema["schema"]
                            else:
                                task_logger.error(f"Unable to find schema for schema_id: {schema_id}")
                        
                        #if the rule is SCHEMA_VALIDATION and is_enabled=True
                        if row["data_quality_rule_name"] == "SCHEMA_VALIDATION" and row["isenabled"] == "True":

                            schema_validation_result = schema_validation(data, schema_read)

                            if schema_validation_result:
                                task_logger.info("schema validation is success")
                                schema_validation_status = "Pass"

                                insert_query = f"""
                                    INSERT INTO {DATA_QUALITY_ISSUE}
                                        (data_quality_rule_id, data_source_item_id, description, severity, status, threshold)
                                    VALUES (%s, %s, %s, %s, %s, %s);
                                """
                                insert_values = (
                                    row["data_quality_rule_id"], row["data_source_item_id"], "Schema Validation",severity, schema_validation_status, threshold
                                )
                                cursor.execute(insert_query, insert_values)
                            else:
                                schema_validation_status = "Failed"
                                task_logger.info("Schema validation failed")
                                schema_validation_error_msg = schema_validation_result
                                #regarding hard failures
                                if severity == "High":
                                    hard_fail_flag = True
                                task_logger.info(f"schema_validation_error_msg----->{schema_validation_error_msg}")

                                insert_query = f"""
                                    INSERT INTO {DATA_QUALITY_ISSUE}
                                        (data_quality_rule_id, data_source_item_id, description, severity, status, 
                                        validation_error, threshold)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                                """
                                insert_values = (
                                    row["data_quality_rule_id"], row["data_source_item_id"], "Schema Validation", severity, schema_validation_status, 
                                    schema_validation_error_msg, threshold
                                )
                                cursor.execute(insert_query, insert_values)
                                
                                '''The below code checks if a schema validation error message exists for a specific operator and stream, 
                                and either initializes or appends the transaction date to the list of dates for that error message.'''
                                if (
                                        schema_validation_error_msg
                                        not in operator_dq_rules_dict[operator_id]["SCHEMA_VALIDATION"][stream_name]
                                ):
                                    operator_dq_rules_dict[operator_id][
                                        "SCHEMA_VALIDATION"
                                    ][stream_name][schema_validation_error_msg] = [txn_dt]
                                else:
                                    operator_dq_rules_dict[operator_id][
                                        "SCHEMA_VALIDATION"
                                    ][stream_name][schema_validation_error_msg].append(txn_dt)

                            #if the rule is PRIMARY_KEY_VALIDATION and is_enabled=True
                        if row["data_quality_rule_name"] == "PRIMARY_KEY_VALIDATION" and row["isenabled"] == "True":
                            try:
                                #getting list of primary keys from schema
                                primary_key_list = outer_schema["primary_keys"]
                            except Exception as e:
                                task_logger.info(
                                    f"Error reading schema and related details for the {platform} file from S3: {e}"
                                )
                            """
                                Using a method for primary key validation
                            """
                            
                            primary_key_is_unique, error_message = primary_key_validation(df, primary_key_list, platform)

                            if primary_key_is_unique:
                                task_logger.info("The primary key are unique")
                                task_logger.info("Primary key validation success")
                                primary_key_validation_status = "Pass"

                                insert_query = f"""
                                    INSERT INTO {DATA_QUALITY_ISSUE}
                                        (data_quality_rule_id, data_source_item_id, description, severity, status,
                                        threshold)
                                    VALUES (%s, %s, %s, %s, %s, %s);
                                """
                                insert_values = (
                                    row["data_quality_rule_id"], row["data_source_item_id"], "Primary Key Validation", severity,
                                    primary_key_validation_status, threshold
                                )
                                cursor.execute(insert_query, insert_values)
                            else:
                                pk_validation_error_msg = error_message
                                task_logger.info(pk_validation_error_msg)
                                if severity == "High":
                                    hard_fail_flag = True
                                primary_key_validation_status = "Failed"
                                task_logger.info("Primary key validation failed")

                                insert_query = f"""
                                    INSERT INTO {DATA_QUALITY_ISSUE}
                                        (data_quality_rule_id, data_source_item_id, description, severity, status,
                                        validation_error, threshold)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                                """
                                insert_values = (
                                    row["data_quality_rule_id"], row["data_source_item_id"], "Primary Key Validation", severity, 
                                    primary_key_validation_status, pk_validation_error_msg, threshold
                                )
                                cursor.execute(insert_query, insert_values)
                                
                                if (
                                        pk_validation_error_msg
                                        not in operator_dq_rules_dict[operator_id]["PRIMARY_KEY_VALIDATION"][stream_name]
                                ):
                                    operator_dq_rules_dict[operator_id][
                                        "PRIMARY_KEY_VALIDATION"
                                    ][stream_name][pk_validation_error_msg] = [txn_dt]
                                else:
                                    operator_dq_rules_dict[operator_id][
                                        "PRIMARY_KEY_VALIDATION"
                                    ][stream_name][pk_validation_error_msg].append(txn_dt)

                except Exception as e:
                    task_logger.error(f"Fail: {e}")
                    # Inserting the same data with status set to "invalid"
                    insert_query = f"""
                        INSERT INTO {DATA_QUALITY_ISSUE}
                            (data_quality_rule_id, data_source_item_id, description, severity, status, threshold)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """
                    insert_values = (row["data_quality_rule_id"], row["data_source_item_id"], e, severity, "Failed", threshold)
                    cursor.execute(insert_query, insert_values)

            copy_object(
                source_bucket=source_bucket,
                source_key=source_key,
                destination_bucket=destination_bucket,
                destination_key=destination_key
            )

            # check if s3_path is not in s3_files_stats
            if not any(item['path'] == s3_path for item in s3_files_stats):
                s3_files_stats.append(snowflake_details_dict)
                
            status = "DQComplete"
            cursor.execute(f"""
                UPDATE {DATA_SOURCE_ITEM}
                SET status = '{status}',
                    last_updated_at = '{current_time}'
                WHERE id = {row["data_source_item_id"]};
            """)
            task_logger.info(f"Updated {DATA_SOURCE_ITEM} with status {status} for id {row['data_source_item_id']}")

        insert_into_s3_file_stats(s3_files_stats=s3_files_stats)

        context = get_current_context()
        flags = get_slack_flags()
        dq_flags = {
            "ZERO_RECORD_COUNT": "dq_zero",
            "HIGH_RECORD_COUNT": "dq_high",
            "LOW_RECORD_COUNT": "dq_low",
            "PRIMARY_KEY_VALIDATION": "pk_val",
            "SCHEMA_VALIDATION": "schema_val",
        }
        # Iterate over each operator's data quality rules
        for operator_id, dq_txn_dict in operator_dq_rules_dict.items():
            pass_to_slack = ""
            job_id = dq_txn_dict.pop("job_id")

            for dq_type, stream_txn_dts in dq_txn_dict.items():
                current_msg = ""
                for stream, txn_dts in stream_txn_dts.items():
                    nested_msg = ""
                    if txn_dts:
                        if dq_type in ("ZERO_RECORD_COUNT", "HIGH_RECORD_COUNT", "LOW_RECORD_COUNT"):
                            if current_msg:
                                current_msg += "\n"
                            current_msg += f"{' - '}{stream}: {', '.join(map(str, txn_dts))}"
                        else:
                            for error_msg, dates in txn_dts.items():
                                if nested_msg:
                                    nested_msg += "\n"
                                nested_msg += f"{' ' * 4}{error_msg}: {', '.join(map(str, dates))}"

                            if nested_msg:
                                if current_msg:
                                    current_msg += "\n"
                                current_msg += f"{' - '}{stream}: \n{nested_msg}"
                #Check if the flag for the current data quality type is enabled
                if flags.get(dq_flags[dq_type], None):
                    if current_msg:
                        pass_to_slack += " ".join(map(str.capitalize, dq_type.split("_"))) + ":\n"
                        pass_to_slack += f"{current_msg}\n\n"
                else:
                    print(f"Enable '{dq_flags[dq_type]}' flag in 'slack_notification' table to enable slack alerts")

            if pass_to_slack:
                #Send a consolidated data quality failure alert to Slack
                consoliated_dq_fail_slack_alert(
                    context=context,
                    operator_id=operator_id,
                    job_id=job_id,
                    message=pass_to_slack,
                )
                time.sleep(0.5)
        
        connection.commit()
        cursor.close()
        connection.close()  
        for job_id, acc in zip(valid_job_ids, acc_list):
            file_path = acc[2]
            stream_name = acc[1]
            data_source_id = acc[0]
            job_details = fetch_Jobdetail_and_records_extracted(job_id, stream_name)
            job_detail_id = int(job_details[0][0]) if job_details else 0
            task_logger.info("Deleting unprocessed files")
            delete_unprocessed_dbt_files(operator_id, data_source_id , job_id , platform_name)
            task_logger.info("Fetching unprocessed files")
            snowflake_result = fetch_unprocessed_dbt_files(operator_id , data_source_id, job_id)
            task_logger.info("Adding recovery window")
            Utils.rec_window(job_detail_id,snowflake_result)

        task_logger.info(f"hard_fail_flag----->{hard_fail_flag}")
        
        #Failing the current task if hard_fail_flag is True
        if hard_fail_flag:
            raise AirflowFailException("Failing the current task because hard_fail_flag is True")
    except AirflowFailException:
        sys.tracebacklimit = 0
        raise AirflowFailException("Failing the current task as there are some hard failures")
    except Exception as e:
        raise AirflowException (f"Error {e} .")