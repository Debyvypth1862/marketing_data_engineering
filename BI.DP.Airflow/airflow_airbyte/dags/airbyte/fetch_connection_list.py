import snowflake.connector
import mysql.connector
from mysql.connector import Error
import sys
sys.path.insert(3,"dags/airbyte")
import os
from db_connection import mysql_conn
from airbyte import constants
from datetime import datetime
import logging
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.base_hook import BaseHook
import unicodedata
from airflow.models import Variable
import boto3

# Retrieve Snowflake account and set up logger for task logs
ACCOUNT = os.getenv("ACCOUNT")
task_logger = logging.getLogger("airflow.task")

def fetch_unprocessed_dbt_files(operator_id, data_source_id , job_id):
    recovery_interval = Variable.get("recovery_interval")
   
    # Establish Snowflake connection
    try:
        snowflake_conn = snowflake.connector.connect(
            user=os.getenv("DBT_USER"),
            password=os.getenv("DBT_PASSWORD"),
            account=os.getenv("DBT_ACCOUNT"),
            warehouse=os.getenv("DBT_WAREHOUSE"),
            role=os.getenv("DBT_ROLE")
        )
    except snowflake.connector.errors.Error as e:
        task_logger.error(f"Failed to connect to Snowflake: {e}")
        return None
 
    # Get Snowflake table details
    db = os.getenv("snowflake_db_dbt")
    schema = os.getenv("snowflake_schema_dbt")
    tbl = os.getenv("snowflake_table_dbt")
 
    # Get today's date in 'YYYY-MM-DD' format
    today_date = datetime.today().strftime('%Y-%m-%d')
 
    # Construct the snowflake select query with parameterized inputs
    query = f"""
    SELECT transaction_date, data_source_id
    FROM {db}.{schema}.{tbl}
    WHERE IS_PROCESSED = FALSE
      AND PICKED_FOR_REPROCESS = FALSE
      AND transaction_date >= DATEADD(DAY, -%s, CURRENT_DATE)
      AND TRACKER_LOGIN_ID = %s
      AND DATA_SOURCE_ID = %s
      AND JOB_ID <> %s
    """
 
    # Construct the snowflake update query
    update_query = f"""
    UPDATE {db}.{schema}.{tbl}
    SET PICKED_FOR_REPROCESS = TRUE , STATUS = 'Invalid'
    WHERE IS_PROCESSED = FALSE
      AND PICKED_FOR_REPROCESS = FALSE
      AND transaction_date >= DATEADD(DAY, -%s, CURRENT_DATE)
      AND TRACKER_LOGIN_ID = %s
      AND DATA_SOURCE_ID = %s
      AND JOB_ID <> %s
    """
 
    try:
        # Execute the queries in Snowflake using parameterized execution to prevent SQL injection
        cursor = snowflake_conn.cursor()
       
        # Fetch unprocessed records
        cursor.execute(query, (recovery_interval, operator_id, data_source_id, job_id))
        snowflake_result = cursor.fetchall()
        task_logger.info(f'snowflake_result for fetch_unprocessed_dbt_files - {snowflake_result}')
        # Update records to mark them for reprocessing
        # cursor.execute(update_query, (recovery_interval, operator_id, data_source_id, job_id))
        snowflake_conn.commit()  # Explicit commit after the update
 
        return snowflake_result
    except snowflake.connector.errors.Error as e:
        task_logger.error(f"Error executing query: {e}")
        return []
    finally:
        # Ensure resources are cleaned up
        cursor.close()
        snowflake_conn.close()


def delete_unprocessed_dbt_files(operator_id, data_source_id , job_id , platform_name):
    recovery_interval = Variable.get("recovery_interval")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
    )
    session = boto3.Session(
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
    )   
    
    # Establish Snowflake connection
    try:
        snowflake_conn = snowflake.connector.connect(
            user=os.getenv("DBT_USER"),
            password=os.getenv("DBT_PASSWORD"),
            account=os.getenv("DBT_ACCOUNT"),
            warehouse=os.getenv("DBT_WAREHOUSE"),
            role=os.getenv("DBT_ROLE")
        )
    except snowflake.connector.errors.Error as e:
        task_logger.error(f"Failed to connect to Snowflake: {e}")
        return None
 
    # Get Snowflake table details
    db = os.getenv("snowflake_db_dbt")
    schema = os.getenv("snowflake_schema_dbt")
    tbl = os.getenv("snowflake_table_dbt")
 
    # Get today's date in 'YYYY-MM-DD' format
    today_date = datetime.today().strftime('%Y-%m-%d')
 
    # Construct the snowflake select query with parameterized inputs
    query = f"""
    SELECT path
    FROM {db}.{schema}.{tbl}
    WHERE IS_PROCESSED = FALSE
      AND PICKED_FOR_REPROCESS = FALSE
      AND transaction_date >= DATEADD(DAY, -%s, CURRENT_DATE)
      AND TRACKER_LOGIN_ID = %s
      AND DATA_SOURCE_ID = %s
      AND JOB_ID <> %s
    """
 
    try:
        # Execute the queries in Snowflake using parameterized execution to prevent SQL injection
        cursor = snowflake_conn.cursor()
       
        # Fetch unprocessed records
        cursor.execute(query, (recovery_interval, operator_id, data_source_id, job_id))
        snowflake_result = cursor.fetchall()
        task_logger.info(f'snowflake_result for delete_unprocessed_dbt_files - {snowflake_result}')
        
    except snowflake.connector.errors.Error as e:
        task_logger.error(f"Error executing query: {e}")
    finally:
        # Ensure resources are cleaned up
        cursor.close()
        snowflake_conn.close()        
    
    try:    
        #getting path for non-casino platforms
        if platform_name in (constants.BRC, constants.Google_Analytics, constants.Voluum, constants.Apilayer, constants.BRT):
            output_bucket_path_dq = os.getenv("Output_bucket_path_DQ_NC")
        #getting  path for casino platforms'''
        else:
            output_bucket_path_dq = os.getenv("Output_bucket_path_DQ")
            
        for path in snowflake_result:
            destination_bucket = os.getenv('Output_bucket_DQ')
            destination_key = output_bucket_path_dq + path[0]
            s3_client.delete_object(Bucket=destination_bucket, Key=destination_key)
            task_logger.info(f"Successfully deleted {destination_key} from {destination_bucket}")
    except Exception as e:
        task_logger.error(f'Error - {e}')           
                            

        
# Function to insert data into Snowflake table from S3 paths
def insert_into_s3_file_stats(s3_files_stats):
    task_logger.info(s3_files_stats)
    # Check if there are no S3 paths to insert
    if not s3_files_stats:
        task_logger.info("insert_into_s3_file_stats received empty s3_files_stats. No Snowflake inserts done. Exiting function.")
        return None

    # Establish Snowflake connection
    try:
        snowflake_conn = snowflake.connector.connect(
            user=os.getenv("DBT_USER"),
            password=os.getenv("DBT_PASSWORD"),
            account=os.getenv("DBT_ACCOUNT"),
            warehouse= os.getenv("DBT_WAREHOUSE"),
            role=os.getenv("DBT_ROLE")
        )
    except Exception as e:
        task_logger.error(f"Failed to connect to Snowflake: {e}")
        raise

    # Get Snowflake table details
    db = os.getenv("snowflake_db_dbt")
    schema = os.getenv("snowflake_schema_dbt")
    tbl = os.getenv("snowflake_table_dbt")

    # Get today's date in 'YYYY-MM-DD' format
    today_date = datetime.today().strftime('%Y-%m-%d')

    # Construct the SQL insert query
    query = f"INSERT INTO {db}.{schema}.{tbl} (PATH,JOB_ID,DATA_SOURCE_ID,TRACKER_LOGIN_ID,TRANSACTION_DATE,S3_FILE_ARRIVAL_DATE) VALUES "

    # Loop through the paths and sanitize them
    for stats in s3_files_stats:
        try:
            task_logger.info(stats)
            # Normalize the path to handle invalid characters
            normalized_path = unicodedata.normalize('NFKD', stats['path']).encode('utf-8', 'replace').decode('utf-8')
            task_logger.info(normalized_path)
            query += f"('{normalized_path}',{stats['job_id']},{stats['data_source_id']},{stats['tracker_login_id']}, TO_DATE('{stats['transaction_date']}','YYYY-MM-DD'), TO_DATE('{today_date}', 'YYYY-MM-DD')),"
        except Exception as e:
            task_logger.error(f"Error-> {e}")
            raise

    # Finalize the query
    query = query.rstrip(',') + ";"

    # Log the query for debugging
    task_logger.info(f"INSERT INTO S3_FILES_STATS query = {query}")

    try:
        # Execute the query in Snowflake
        cursor = snowflake_conn.cursor()
        cursor.execute(query)
    except Exception as e:
        # Log any errors encountered during execution
        task_logger.error(f"Error - {e}")
        raise
    else:
        # Log successful insertion
        task_logger.info("Snowflake INSERT INTO s3_files_stats complete!")
    finally:
        # Ensure resources are cleaned up
        cursor.close()
        snowflake_conn.commit()
        snowflake_conn.close()

# Function to fetch operator ID from MySQL based on connection ID
def fetch_operator_id_from_ACCOUNT(connection_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT operator_id
        FROM {ACCOUNT}
        WHERE airbyte_connection_id = '{connection_id}';
    """)
    result = cursor.fetchall()
    print(result)
    cursor.close()
    connection.close()
    return result

# Function to fetch start date from MySQL based on connection ID
def fetch_start_date_from_ACCOUNT(connection_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT start_date
        FROM {ACCOUNT}
        WHERE airbyte_connection_id = '{connection_id}';""")
    result = cursor.fetchall()
    print(result)
    cursor.close()
    connection.close()
    return result

# Function to fetch platform ID based on platform name
def fetch_platform_id_from_platform(platform_name):
    connection = mysql_conn()
    platform= os.getenv("platform")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT Id
        FROM {platform}
        WHERE name = '{platform_name}';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()

    # Return the platform ID if found, otherwise None
    if result:
        return result[0][0]
    else:
        return None

# Function to fetch loopback days from MySQL based on connection ID
def fetch_loopback_days_from_ACCOUNT(connection_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT loopback_days
        FROM {ACCOUNT}
        WHERE airbyte_connection_id = '{connection_id}';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to fetch connection details from MySQL by platform ID
def fetch_connection_ids_by_platform(platform_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT airbyte_connection_id, username, name, operator_id
        FROM {ACCOUNT}
        WHERE platform_id = {platform_id}
            AND airbyte_connection_id <> ''
            AND connection_status = 'Enabled'
            AND validation_status = 'Valid';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()

    # Return empty lists if no results found
    if not result:
        return [],[],[]
    conn_list,username_list,operator_list,oppid_list = map(list,zip(*result))

    return conn_list,username_list,operator_list,oppid_list

def fetch_connid_oppid_by_platform(platform_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    PLATFORM = os.getenv("platform")
    if platform_id == 999 :
        cryptoback = "AND publ_username in ('Cryptoback')"
        platform = ""
    else:
        cryptoback = "AND publ_username not in ('Cryptoback')"
        platform = f"""AND platform_id = {platform_id}"""
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT acc.airbyte_connection_id, acc.username, acc.name, acc.operator_id, acc.password, pl.id, pl.name
        FROM {ACCOUNT} acc
        JOIN {PLATFORM} pl on pl.id = acc.platform_id
        WHERE 1=1
            {platform}
            AND airbyte_connection_id <> ''
            AND connection_status = 'Enabled'
            AND validation_status = 'Valid'
            AND tlog_deleted = 0
            {cryptoback}
            ;
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

def fetch_connid_oppid_by_platform_and_name(platform_id, name):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT airbyte_connection_id, username, name, operator_id, password
        FROM {ACCOUNT}
        WHERE platform_id = {platform_id}
            AND lower(name) = '{name}'
            AND airbyte_connection_id <> ''
            AND connection_status = 'Enabled'
            AND validation_status = 'Valid'
            AND tlog_deleted = 0;
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to fetch a list of operator IDs from MySQL based on platform ID
def fetch_connection_operator_list(platform_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT operator_id
        FROM {ACCOUNT}
        WHERE platform_id = {platform_id}
            AND airbyte_connection_id <> ''
            AND connection_status = 'Enabled'
            AND validation_status = 'Valid';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()

    # Return an empty list if no results are found
    if not result:
        return []
    opp_list= [item[0] for item in result]
    return opp_list

def fetch_connection_operator_table(platform_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT operator_id, name, username, airbyte_connection_id, created_at, last_updated, start_date
        FROM {ACCOUNT}
        WHERE platform_id = {platform_id}
            AND airbyte_connection_id <> ''
            AND connection_status = 'Enabled'
            AND validation_status = 'Valid';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()

    return result

def fetch_connection_operator_table_by_operator_id(platform_id, operator_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT operator_id, name, username, airbyte_connection_id, created_at, last_updated, start_date
        FROM {ACCOUNT}
        WHERE platform_id = {platform_id}
            AND operator_id = {operator_id}
            AND airbyte_connection_id <> ''
            AND connection_status = 'Enabled'
            AND validation_status = 'Valid';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()

    return result

# Function to fetch recovery connection operator details for a given platform
def fetch_recovery_connection_operator_table(platform_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT operator_id, name, username, recovery_airbyte_connection_id, created_at, last_updated
        FROM {ACCOUNT}
        WHERE platform_id = {platform_id}
            AND recovery_airbyte_connection_id <> ''
            AND connection_status = 'Enabled'
            AND validation_status = 'Valid';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to fetch data path from MySQL for new extraction jobs
def fetch_data_path_from_mysql():
    connection = mysql_conn()
    cursor = connection.cursor()
    DATA_SOURCE = os.getenv("data_source")
    cursor.execute(f"""
        SELECT operator_id, job_id, data_path, platform_name, account_id, stream_name, created_at, last_updated
        FROM {DATA_SOURCE}
        WHERE extraction_status = 'new';
    """)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# Function to fetch connection details based on platform name and operator ID
def fetch_connection_data_source_table(platform_name, operator_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    DATA_SOURCE = os.getenv("data_source")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT id, operator_id, source_name, airbyte_connection_id, path
        FROM {DATA_SOURCE}
        WHERE platform_name = '{platform_name}'
            AND operator_id = {operator_id}
            AND airbyte_connection_id <> '';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to fetch recovery connection details based on platform name and operator ID
def fetch_recovery_connection_data_source_table(platform_name, operator_id):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    DATA_SOURCE = os.getenv("data_source")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT id, operator_id, source_name, recovery_airbyte_connection_id, path
        FROM {DATA_SOURCE}
        WHERE platform_name = '{platform_name}'
            AND operator_id = {operator_id}
            AND recovery_airbyte_connection_id <> '';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to fetch connection details for task IDs based on platform name
def fetch_connection_for_task_ids(platform_name):
    connection = mysql_conn()
    ACCOUNT= os.getenv("ACCOUNT")
    PLATFORM = os.getenv("platform")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT p.name, a.name, a.username, a.operator_id
        FROM {ACCOUNT} a
        JOIN {PLATFORM} p
        WHERE a.platform_id = p.Id
            AND p.name = '{platform_name}';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()

    return result

# Function to fetch job details and records extracted for a given job ID and stream name
def fetch_Jobdetail_and_records_extracted(job_id,stream_name):
    connection = mysql_conn()
    job= os.getenv("jobs")
    job_detail= os.getenv("job_detail")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT id, records_extracted, sync_window
        FROM (
            SELECT j.job_id, d.attempt_id, d.id, j.operator_id, d.stream_name, d.records_extracted, d.status, d.sync_window,
                   RANK() OVER (PARTITION BY j.job_id ORDER BY d.attempt_id desc) AS attempt_rank
            FROM {job} j
            JOIN {job_detail} d
                ON j.job_id = d.job_id
        ) ranked
        WHERE attempt_rank = 1
            AND job_id = {job_id}
            AND stream_name = '{stream_name}'
            AND status = 'succeeded';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to fetch Job Detail ID based on Job ID
def fetch_Jobdetail_id(job_id):
    connection = mysql_conn()
    job= os.getenv("jobs")
    job_detail= os.getenv("job_detail")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT id
        FROM {job_detail}
        where job_id = {job_id}
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to fetch recovery connection IDs and transactions date from MySQL
def fetch_connid_and_transactions_date():
    connection = mysql_conn()
    cursor = connection.cursor()
    cursor.execute("""
        SELECT *
        FROM vw_get_recovery_date;
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()

    return result

# Function to update recovery status for a specific data source and job
def update_recovery_status(data_source_id, job_id):
    connection = mysql_conn()
    cursor = connection.cursor()
    DATA_SOURCE_ITEM = os.getenv("data_source_item")
    DATA_QUALITY_ISSUE = os.getenv("data_quality_issue")
    cursor.execute(f"""
            UPDATE {DATA_SOURCE_ITEM} dsi
            INNER JOIN {DATA_QUALITY_ISSUE} dqi
                ON dsi.id = dqi.data_source_item_id
            SET dsi.status = 'recovery_attempeted'
            WHERE dqi.status = 'Failed'
                AND dsi.data_source_id = {data_source_id}
                AND job_id = {job_id};
    """)
    connection.commit()
    cursor.close()
    connection.close()

# Function to fetch recovery source info based on platform ID
def fetch_recovery_source_info(platform_id):
    pyconnection = mysql_conn()

    ACCOUNT = os.getenv("ACCOUNT")
    JOB = os.getenv("jobs")
    JOB_DETAIL = os.getenv("job_detail")
    PLATFORM = os.getenv('platform')
    try:
        with pyconnection.cursor() as pycursor:
            pycursor.execute(f"""
                WITH cte AS (   
                    SELECT
                        j.job_id,
                        j.operator_id,
                        jd.is_recovery,
                        ac.id,
                        ac.platform_id,
                        ac.username,
                        ac.`password`,
                        ac.`name`,
                        ac.api_key,
                        ac.start_date,
                        ac.endpoint,
                        ac.airbyte_source_id,
                        ac.recovery_airbyte_source_id,
                        ac.airbyte_connection_id,
                        ac.recovery_airbyte_connection_id,
                        ac.loopback_days,
                        ac.validation_status,
                        ac.account_status,
                        ac.connection_status,
                        ac.tlog_deleted,
                        p.name AS platform_name,
                        CASE
                            WHEN jd.recovery_dates IS NOT NULL AND jd.recovery_dates <> ''
                            THEN CONCAT('"', jd.stream_name, '" :', jd.recovery_dates)
                            ELSE NULL
                        END AS recovery_dates,
                        RANK() OVER (PARTITION BY j.operator_id ORDER BY jd.is_recovery ASC, j.job_id DESC) AS job_rank
                    FROM
                        {JOB} AS j
                    INNER JOIN
                        {JOB_DETAIL} AS jd ON j.job_id = jd.job_id
                    INNER JOIN
                        {ACCOUNT} AS ac ON j.operator_id = ac.operator_id
                    INNER JOIN
                        {PLATFORM} AS p ON ac.platform_id = p.id
                    WHERE
                        ac.platform_id = {platform_id}
                    )
                    SELECT
                        cte.platform_name,
                        cte.id,
                        cte.platform_id,
                        cte.operator_id,
                        cte.username,
                        cte.`password`,
                        cte.`name`,
                        cte.api_key,
                        cte.start_date,
                        cte.endpoint,
                        cte.airbyte_source_id,
                        cte.recovery_airbyte_source_id,
                        cte.airbyte_connection_id,
                        cte.recovery_airbyte_connection_id,
                        cte.loopback_days,
                        cte.validation_status,
                        cte.account_status,
                        cte.connection_status,
                        cte.tlog_deleted,
                        GROUP_CONCAT(cte.recovery_dates SEPARATOR ', ') AS recovery_dates
                    FROM
                        cte
                    WHERE
                        cte.job_rank = 1
                        AND cte.recovery_dates IS NOT NULL
                        AND cte.recovery_dates <> ''
                    GROUP BY
                        cte.id,
                        cte.platform_id,
                        cte.operator_id,
                        cte.username,
                        cte.`password`,
                        cte.`name`,
                        cte.api_key,
                        cte.start_date,
                        cte.endpoint,
                        cte.airbyte_source_id,
                        cte.recovery_airbyte_source_id,
                        cte.airbyte_connection_id,
                        cte.recovery_airbyte_connection_id,
                        cte.loopback_days,
                        cte.validation_status,
                        cte.account_status,
                        cte.connection_status,
                        cte.tlog_deleted;
            """)
            result = pycursor.fetchall()
            column_names = [description[0] for description in pycursor.description]
            row_list = []
            for row in result:
                result_columns = dict(zip(column_names, row))
                row_list.append(result_columns)
            pyconnection.commit()
            pycursor.close()
            pyconnection.close()
            return row_list
    except Error as e:
        print(f"Error: {e}")

# Function to fetch path based on connection ID from data source
def fetch_path_from_data_source(connection_id):
    connection = mysql_conn()
    data_source= os.getenv("data_source")
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT path
        FROM {data_source}
        WHERE airbyte_connection_id = '{connection_id}';
    """)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

def fetch_data_source_id_using_platform_and_name(platform_name, source_name, connection=None):
    if connection is None:
        connection = mysql_conn()
    DATA_SOURCE = os.getenv("data_source")
    cursor = connection.cursor()
    query= f"""
        SELECT id
        FROM {DATA_SOURCE}
        WHERE platform_name = '{platform_name}' AND source_name = '{source_name}';
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()

    # Return the data_source_id if found, otherwise None
    if result:
        return result[0][0]
    else:
        return None