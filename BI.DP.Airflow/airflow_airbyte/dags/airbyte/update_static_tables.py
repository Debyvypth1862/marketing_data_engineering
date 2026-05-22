import os
import sys
from datetime import datetime
from mysql.connector import Error
import requests
from requests.auth import HTTPBasicAuth
import logging

sys.path.insert(1, "dags/airbyte")
from replace_spl_char import to_camel_case
from db_connection import mysql_conn
from airbyte import constants

task_logger = logging.getLogger("airflow.task")

PLATFORM = os.getenv("platform")
ACCOUNT = os.getenv("ACCOUNT")
DATA_SOURCE = os.getenv("data_source")
DATA_SOURCE_ITEM = os.getenv("data_source_item")
DATA_SOURCE_DQ = os.getenv("data_source_dq")
DATA_QUALITY_RULE = os.getenv("data_quality_rule")
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

'''The get_connection method retrieves connection details from the Airbyte server using the provided connection_id, 
making a POST request to the Airbyte API and returning the response.'''
def get_connection(connection_id):
    airbyte_host = os.getenv("airbyte_server")
    endpoint = "api/v1/connections/get"
    url = airbyte_host + endpoint
    headers = {"accept": "application/json", "content-type": "application/json"}
    basic_auth = HTTPBasicAuth("airbyte", "password")
    payload = {"connectionId": connection_id}
    response = requests.post(url=url, headers=headers, auth=basic_auth, json=payload)
    return response

'''The fetch_platform_and_operator method retrieves a list of platform and operator details from the database, including connection IDs, by joining the ACCOUNT and PLATFORM tables and filtering out 
records with empty or null Airbyte connection IDs and operator IDs already present in the DATA_SOURCE table'''
def fetch_platform_and_operator():
    connection = mysql_conn()
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT operator_id, p.name, airbyte_connection_id, a.name, username, a.recovery_airbyte_connection_id, a.name
        FROM {ACCOUNT} a
        JOIN {PLATFORM} p
            ON a.platform_id = p.Id
        WHERE (a.airbyte_connection_id <> Null OR a.airbyte_connection_id <> '') 
            AND a.operator_id NOT IN (SELECT operator_id FROM {DATA_SOURCE});
    """)
    opp_list = cursor.fetchall()
    connection.close()
    return opp_list

'''The get_schema_id method retrieves the schema ID from the MASTER_JSON_SCHEMA table based on provided platform, stream, and operator parameters, first checking for a match with all three parameters, 
and if no match is found, attempting to find a match using only platform and stream.'''
def get_schema_id(platform, stream, operator):
    try:
        connection = mysql_conn()
        cursor = connection.cursor()

        # First query to check for platform, stream, and operator
        platform = str.lower(platform)
        stream = str.lower(stream)
        if operator == None:
            task_logger.info("Inside operator = None case ")
            select_query = """
                SELECT id
                FROM MASTER_JSON_SCHEMA
                WHERE lower(platform) = %s
                    AND lower(stream) = %s;
            """
            cursor.execute(select_query, (platform, stream))
            myresult = cursor.fetchall()
            cursor.close()
            if myresult:
                return myresult[0][0]
            else:
                task_logger.error("No matching schema found for the given parameters.")
                return None
        else:
            operator = str.lower(operator)
            task_logger.info("inside operator is not null")
            task_logger.info(f"details from code are platform: {platform}, stream: {stream}, operator: {operator}")
            select_query = """
                SELECT id
                FROM MASTER_JSON_SCHEMA
                WHERE lower(platform) = %s
                    AND lower(stream) = %s
                    AND lower(operator) = %s;
            """
            cursor.execute(select_query, (platform, stream, operator))
            myresult = cursor.fetchall()

            # If no result found, check for platform and stream only
            if not myresult:
                task_logger.info("inside not result and check for platform and stream")
                select_query = """
                    SELECT id
                    FROM MASTER_JSON_SCHEMA
                    WHERE lower(platform) = %s
                        AND lower(stream) = %s;
                """
                cursor.execute(select_query, (platform, stream))
                myresult = cursor.fetchall()

            cursor.close()
            if myresult:
                return myresult[0][0]
            else:
                task_logger.error("No matching schema found for the given parameters.")
                return None
    except Error as e:
        task_logger.error(f"Error: {e}")
        return None  # Handle the error appropriately
    finally:
        connection.close()

'''The update_schema_id_in_data_quality_rule method updates the schema_id in the DATA_QUALITY_RULE based om the schems_id present in the MASTER_JSON_SCHEMA table'''
def update_schema_id_in_data_quality_rule():
    connection = mysql_conn()
    update_cursor = connection.cursor()
    # This query updates the schema_id present in DATA_QUALITY_RULE_ID properly
    try:
        query = """
            UPDATE DATA_QUALITY_RULE dqr
            JOIN DATA_SOURCE_DQ dsq
                ON dsq.data_quality_rule_id = dqr.id
            JOIN DATA_SOURCE ds
                ON ds.id = dsq.data_source_id
            JOIN ACCOUNT a
                ON a.operator_id = ds.operator_id
            LEFT JOIN MASTER_JSON_SCHEMA mjs1
                ON lower(mjs1.platform) = lower(ds.platform_name)
                AND lower(mjs1.stream) = lower(ds.source_name)
                AND lower(mjs1.operator) = lower(a.name)
            LEFT JOIN MASTER_JSON_SCHEMA mjs2
                ON lower(mjs2.platform) = lower(ds.platform_name)
                AND lower(mjs2.stream) = lower(ds.source_name)
                AND mjs2.operator IS NULL
            SET dqr.schema_id = COALESCE(mjs1.id, mjs2.id)
            WHERE dqr.description IN ('Schema Validation', 'Primary Key Validation');
        """
        update_cursor.execute(query)
        connection.commit()
        task_logger.info("Successfully updated schema_id in DATA_QUALITY_RULE table")
        update_cursor.close()
    except Error as e:
        task_logger.info(f"Unable to update the schema_id in DATA_QUALITY_RULE error is: {e}")
    finally:
        connection.close()


new_rules = ["SCHEMA_VALIDATION", "PRIMARY_KEY_VALIDATION"]


def construct_query(new_rules):
    # Initialize the CTE and main query parts
    cte = f"""
        WITH DataSourceRules AS (
            SELECT ds.id AS data_source_id, dqr.name AS rule_name
            FROM
                {DATA_SOURCE} ds
            LEFT JOIN {DATA_SOURCE_DQ} dsd
                ON ds.id = dsd.data_source_id
            LEFT JOIN {DATA_QUALITY_RULE} dqr
                ON dsd.data_quality_rule_id = dqr.id
        ),
        MissingRules AS (
            SELECT dsr.data_source_id
            FROM DataSourceRules dsr
            GROUP BY dsr.data_source_id
            HAVING
    """

    # Construct the HAVING clause dynamically based on the list of rules
    having_clauses = []
    for rule in new_rules:
        clause = f"SUM(CASE WHEN dsr.rule_name = '{rule}' THEN 1 ELSE 0 END) = 0"
        having_clauses.append(clause)
    
    having_clause = " OR ".join(having_clauses)

    # Combine all parts to form the final query
    query = f"""
        {cte} {having_clause})
    SELECT DISTINCT ds.id, platform_name, source_name
    FROM {DATA_SOURCE} ds
    INNER JOIN MissingRules mr
        ON ds.id = mr.data_source_id;
    """
    return query


def add_new_rule():
    connection = mysql_conn()
    cursor = connection.cursor()
    query = construct_query(new_rules)
    cursor.execute(query)
    data_source_ids = cursor.fetchall()

    for ds_id in data_source_ids:
        print(f"data_source_id----->{ds_id[0]}")
        print(f"platform_name----->{ds_id[1]}")
        print(f"stream----->{ds_id[2]}")
        data_source_id = ds_id[0]
        platform_name = ds_id[1]
        stream = ds_id[2]
        platform_camelcase = to_camel_case(platform_name)
        platform = str.lower(platform_camelcase)
        cursor.execute(f"""
            INSERT INTO {DATA_QUALITY_RULE} 
                (data_quality_dimension_id, name, description, threshold, isenabled, severity) 
            VALUES (1 , 'SCHEMA_VALIDATION', 'Schema Validation', 0, 'True', 'High');
        """)

        data_quality_rule_id = cursor.lastrowid
        cursor.execute(f"""
            INSERT INTO {DATA_SOURCE_DQ}
                (data_source_id, data_quality_rule_id, created_at, last_updated)
            VALUES ({data_source_id}, {data_quality_rule_id}, '{current_time}', '{current_time}');
        """)

        cursor.execute(f"""
            INSERT INTO {DATA_QUALITY_RULE}
                (data_quality_dimension_id, name, description, threshold, isenabled, severity)
            VALUES (1, 'PRIMARY_KEY_VALIDATION', 'Primary Key Validation', 0, 'True', 'High');
        """)

        data_quality_rule_id = cursor.lastrowid
        cursor.execute(f"""
            INSERT INTO {DATA_SOURCE_DQ}
                (data_source_id,data_quality_rule_id,created_at,last_updated)
            VALUES ({data_source_id}, {data_quality_rule_id}, '{current_time}', '{current_time}');
        """)

        connection.commit()
    connection.close()

'''This function updates the data source and inserts related data quality rules into the database for each operator and stream.'''
def update_data_source():
    connection = mysql_conn()
    cursor = connection.cursor()
    operators = fetch_platform_and_operator()

    for opp in operators:
        connection_id = opp[2]
        response = get_connection(connection_id)
        if response is not None:
            if response.status_code == 200:
                response_json = response.json()
                stream_names = [stream["stream"]["name"] for stream in response_json["syncCatalog"]["streams"]]
                if opp[1] == constants.Inhouse:
                    platform_name = constants.Buffalo_Partners
                else:
                    platform_name = opp[1]
                for stream in stream_names:
                    namespace = to_camel_case(f"{platform_name}/{opp[3]}/{opp[4]}_{opp[0]}")
                    path = f"{namespace}/{stream}/raw_data.jsonl"

                    cursor.execute(f"""
                        INSERT INTO {DATA_SOURCE}
                            (operator_id, platform_name, source_name, airbyte_connection_id, 
                            recovery_airbyte_connection_id, path, created_at, last_updated)
                        VALUES ({opp[0]}, '{platform_name}', '{stream}', '{opp[2]}', '{opp[5]}', '{path}', 
                        '{current_time}', '{current_time}');
                    """)
                    data_source_id = cursor.lastrowid

                    cursor.execute(f"""
                        INSERT INTO {DATA_QUALITY_RULE}
                            (data_quality_dimension_id, name, description, expression, threshold, isenabled, severity)
                        VALUES (1, 'LOW_RECORD_COUNT', 'Low Record Count', '5', 30, 'True', 'Low');
                    """)
                    data_quality_rule_id = cursor.lastrowid

                    cursor.execute(f"""
                        INSERT INTO {DATA_SOURCE_DQ}
                            (data_source_id,data_quality_rule_id,created_at,last_updated)
                        VALUES ({data_source_id}, {data_quality_rule_id}, '{current_time}', '{current_time}');
                    """)

                    cursor.execute(f"""
                        INSERT INTO {DATA_QUALITY_RULE}
                            (data_quality_dimension_id, name, description, expression, threshold, isenabled, severity)
                        VALUES (1, 'HIGH_RECORD_COUNT', 'High Record Count', '5', 30, 'True', 'Low');
                    """)
                    data_quality_rule_id = cursor.lastrowid

                    cursor.execute(f"""
                        INSERT INTO {DATA_SOURCE_DQ}
                            (data_source_id,data_quality_rule_id,created_at,last_updated)
                        VALUES ({data_source_id}, {data_quality_rule_id}, '{current_time}', '{current_time}');
                    """)

                    cursor.execute(f"""
                        INSERT INTO {DATA_QUALITY_RULE}
                            (data_quality_dimension_id, name, description, expression, threshold, isenabled, severity)
                        VALUES (1, 'ZERO_RECORD_COUNT', 'Zero Record Count', '0', 0, 'True', 'Low');
                    """)
                    data_quality_rule_id = cursor.lastrowid

                    cursor.execute(f"""
                        INSERT INTO {DATA_SOURCE_DQ}
                            (data_source_id, data_quality_rule_id, created_at, last_updated)
                        VALUES ({data_source_id}, {data_quality_rule_id}, '{current_time}', '{current_time}');
                    """)
                    operator = opp[6]
                    platform = str.lower(platform_name)

                    task_logger.info(f"Platform is {platform} and stream is {stream}")

                    schema_id = None  # initiaizing schema id for check
                    if platform == str.lower(constants.BRC) and stream in constants.BRC_incremental_streams:
                        task_logger.info(f"Platform is {platform} and stream is {stream}")
                        schema_id = get_schema_id(platform, stream, operator)
                        
                        if schema_id:
                            task_logger.info("fetched schema_id successfully")
                        else:
                            task_logger.error("unable to fetch schema_id")

                    elif platform == str.lower(constants.BRT) and stream in constants.BRT_incremental_streams:
                        task_logger.info(f"Platform is {platform} and stream is {stream}")
                        schema_id = get_schema_id(platform, stream, operator)
                        
                        if schema_id:
                            task_logger.info("fetched schema_id successfully")
                        else:
                            task_logger.error("unable to fetch schema_id")

                    elif platform == str.lower(constants.Voluum) and stream in constants.Voluum_incremental_streams:
                        task_logger.info(f"Platform is {platform} and stream is {stream}")
                        schema_id = get_schema_id(platform, stream, operator)

                        if schema_id:
                            task_logger.info("fetched schema_id successfully")
                        else:
                            task_logger.error("unable to fetch schema_id")
                    else:
                        if platform not in [str.lower(constants.BRT),str.lower(constants.BRC), str.lower(constants.Voluum)]:
                            task_logger.info(f"Platform is {platform} and stream is {stream}")
                            schema_id = get_schema_id(platform, stream, operator)

                            if schema_id:
                                task_logger.info("fetched schema_id successfully")
                            else:
                                task_logger.error("unable to fetch schema_id")

                    task_logger.info(schema_id)
                    if schema_id:
                        cursor.execute(f"""
                            INSERT INTO {DATA_QUALITY_RULE}
                                (data_quality_dimension_id, name, description, schema_id, threshold, isenabled, severity)
                            VALUES (1, 'SCHEMA_VALIDATION', 'Schema Validation', '{schema_id}', 0, 'True', 'High');
                        """)

                        data_quality_rule_id = cursor.lastrowid
                        cursor.execute(f"""
                            INSERT INTO {DATA_SOURCE_DQ}
                                (data_source_id, data_quality_rule_id, created_at, last_updated)
                            VALUES ({data_source_id}, {data_quality_rule_id}, '{current_time}', '{current_time}');
                        """)

                        cursor.execute(f"""
                            INSERT INTO {DATA_QUALITY_RULE}
                                (data_quality_dimension_id, name, description, schema_id, threshold, isenabled, severity)
                            VALUES (1, 'PRIMARY_KEY_VALIDATION', 'Primary Key Validation', '{schema_id}', 0, 'True', 
                            'High');
                        """)

                        data_quality_rule_id = cursor.lastrowid
                        cursor.execute(f"""
                            INSERT INTO {DATA_SOURCE_DQ}
                                (data_source_id, data_quality_rule_id, created_at, last_updated)
                            VALUES ({data_source_id}, {data_quality_rule_id}, '{current_time}', '{current_time}');
                        """)

                    connection.commit()
            else:
                task_logger.error(response)
        else:
            task_logger.error("Received None Response from api/v1/connections/get api")
    connection.close()
    add_new_rule()
    update_schema_id_in_data_quality_rule()
