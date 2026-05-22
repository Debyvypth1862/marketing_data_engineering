import re
from mysql.connector import Error
from dbconnection import mysql_conn  # Imports a custom MySQL connection utility
from datetime import datetime
import os
import logging
import json
import time
import requests
from requests.auth import HTTPBasicAuth
from airbyte import constants  

# Set up logging for tracking operations
logger = logging.getLogger(__name__)

class Utils:
    # Fetches source information for accounts that have a status of 'New' or 'Updated'
    @classmethod
    def fetch_source_info(cls):
        connection = mysql_conn()  # Establish a MySQL connection
        ACCOUNT = os.getenv('ACCOUNT')  # Fetch account name from environment
        PLATFORM = os.getenv('platform')  # Fetch platform name from environment

        try:
            # Execute SQL to join the ACCOUNT table with the PLATFORM table to fetch relevant details
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT a.*, p.name AS platform_name
                    FROM {ACCOUNT} a
                    JOIN {PLATFORM} p
                    ON a.platform_id = p.id
                    WHERE a.account_status IN ('New', 'Updated');
                """)
                result = cursor.fetchall()  # Fetch all the records
                column_names = [description[0] for description in cursor.description]  # Get column names
                row_list = []

                # Convert each row into a dictionary
                for row in result:
                    result_columns = dict(zip(column_names, row))
                    row_list.append(result_columns)
                return row_list  # Return list of source information
        except Error as e:
            logger.info(f"Error: {e}")  # Log error if any
        finally:
            connection.close()  # Close the connection

    # Fetch source information for a specific account based on ID
    @classmethod
    def fetch_source_info_by_id(cls, id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            query = f"""
                SELECT * 
                FROM {ACCOUNT} 
                WHERE id = %s;
            """
            with connection.cursor() as cursor:
                cursor.execute(query, (id,))  # Parameterized query
                result = cursor.fetchall()  # Fetch the result
                return result  # Return the fetched result
        except Error as e:
            logger.info(f"Error fetching source info by ID: {e}")  # Log error if any
            return None
        finally:
            connection.close()

    # Fetch source information for a list of account IDs
    @classmethod
    def fetch_source_info_by_id_list(cls, id_list):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            id_tuple = tuple(id_list)  # Convert the list to a tuple for SQL IN query
            query = f"""
                SELECT * 
                FROM {ACCOUNT} 
                WHERE id IN ({', '.join(['%s'] * len(id_tuple))});
            """
            with connection.cursor() as cursor:
                cursor.execute(query, id_tuple)  # Execute the query with the ID list
                result = cursor.fetchall()  # Fetch the result
                return result  # Return the fetched result
        except Error as e:
            logger.info(f"Error fetching source info by ID list: {e}")  # Log error if any
            return None
        finally:
            connection.close()

    # Fetch source info for a list of connections, enabling or disabling based on connection data
    @classmethod
    def fetch_source_info_by_id_enable_disable(cls, connection_data_list):
        connection = mysql_conn()

        try:
            source_info_list = []  # List to hold the source info

            # Iterate over the connection data list to fetch source info for each ID
            for connection_data in connection_data_list:
                id = connection_data.get('Id')
                source_info = cls.fetch_source_info_by_id(id)

                if source_info:
                    source_info_list.append(source_info)

            return source_info_list  # Return the source information list
        except Error as e:
            logger.info(f"Error: {e}")  # Log error if any
            return None
        finally:
            connection.close()

    # Fetch list of Airbyte connection IDs for a list of account IDs
    @classmethod
    def fetch_connection_id_list(cls, id_list):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            connection_id_tuple = tuple(id_list)  # Convert the list of IDs to a tuple

            with connection.cursor() as cursor:
                query = f"""
                    SELECT airbyte_connection_id 
                    FROM {ACCOUNT} 
                    WHERE id IN ({', '.join(['%s'] * len(connection_id_tuple))});
                """
                cursor.execute(query, connection_id_tuple)  # Execute the query
                results = cursor.fetchall()

                # Extract Airbyte connection IDs from the results
                airbyte_connection_ids = [result['airbyte_connection_id'] for result in results]
                return airbyte_connection_ids  # Return list of connection IDs
        except Error as e:
            logger.info(f"Error: {e}")  # Log error if any
            return None
        finally:
            connection.close()

    # Fetch a single Airbyte connection ID for a specific account ID
    @classmethod
    def fetch_connection_id(cls, id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT airbyte_connection_id
                    FROM {ACCOUNT}
                    WHERE id = %s""", (id,)
                )
                result = cursor.fetchone()  # Fetch a single result
            if result:
                return result['airbyte_connection_id']  # Return the connection ID if found
            else:
                return None  # Return None if not found
        except Error as e:
            logger.info(f"Error: {e}")  # Log error if any
            return None
        finally:
            connection.close()

    # Update the source ID for a specific operator in the database
    @classmethod
    def add_source_id(cls, source_id, operator_id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET airbyte_source_id = %s
                    WHERE operator_id = %s""", (source_id, operator_id)
                )
                connection.commit()  # Commit the update to the database
                logger.info("sourceId updated successfully.")
        except Exception as e:
            logger.info(f"Error updating {ACCOUNT} table: {e}")  # Log error if any
        finally:
            connection.close()

    # Update the recovery source ID for a specific operator
    @classmethod
    def add_recovery_source_id(cls, source_id, operator_id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET recovery_airbyte_source_id = %s
                    WHERE operator_id = %s""", (source_id, operator_id)
                )
                connection.commit()  # Commit the update
                logger.info("sourceId updated successfully.")
        except Exception as e:
            logger.info(f"Error updating {ACCOUNT} table: {e}")  # Log error if any
        finally:
            connection.close()

    # Update the validation status for a specific Airbyte source ID
    @classmethod
    def update_validation_status(cls, airbyte_source_id, validation_status):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Set validation enabled status based on the validation status value
                if validation_status == 'Valid':
                    is_validation_enabled = 1
                elif validation_status == constants.Invalid:
                    is_validation_enabled = 2
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Update the validation status in the database
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET validation_status = %s, validation_date_time = %s, is_validation_enabled = %s
                    WHERE airbyte_source_id = %s""",
                    (validation_status, current_time, is_validation_enabled, airbyte_source_id)
                )
                connection.commit()  # Commit the update
                logger.info("Validation status updated successfully.")
        except Exception as e:
            logger.info(f"Error updating validation status: {e}")  # Log error if any
        finally:
            connection.close()

    # Update the validation status for a missing API key based on operator ID
    @classmethod
    def update_validation_status_for_missing_api_key(cls, operator_id, validation_status):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                if validation_status == 'Valid':
                    is_validation_enabled = 1
                elif validation_status == constants.Invalid:
                    is_validation_enabled = 2
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Update validation status based on the operator ID
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET validation_status = %s, validation_date_time = %s, is_validation_enabled = %s
                    WHERE operator_id = %s""", (validation_status, current_time, is_validation_enabled, operator_id)
                )
                connection.commit()
                logger.info("Validation status updated successfully.")
        except Exception as e:
            logger.info(f"Error updating validation status: {e}")
        finally:
            connection.close()

    @classmethod
    def update_validation_data(cls, id, validation_status, message):
        connection = mysql_conn()
        ACCOUNT_VALIDATION = os.getenv('ACCOUNT_VALIDATION')

        try:
            with connection.cursor() as cursor:
                # Get current timestamp
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Insert validation data into the ACCOUNT_VALIDATION table
                cursor.execute(f"""
                    INSERT INTO {ACCOUNT_VALIDATION}
                        (account_id, validation_status, validation_message, validation_date_time)
                    VALUES (%s, %s, %s, %s)""", (id, validation_status, message, current_time)
                )
                # Commit changes
                connection.commit()
                logger.info("Validation data updated successfully.")
        except Exception as e:
            logger.info(f"Error updating validation data: {e}")
        finally:
            connection.close()

    @classmethod
    def update_validation_message(cls, operator_id, message):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Log the validation message for debugging
                logger.info(message)
                # Update validation_message in the ACCOUNT table for the specified operator_id
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET validation_message = %s
                    WHERE operator_id = %s""", (message, operator_id)
                )
                # Commit changes
                connection.commit()
                logger.info("Validation message updated successfully.")
        except Exception as e:
            logger.info(f"Error updating validation message: {e}")
        finally:
            connection.close()

    @classmethod
    def add_connection_id(cls, connection_id, operator_id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Update airbyte_connection_Id for the specified operator_id
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET airbyte_connection_Id = %s
                    WHERE operator_id = %s""", (connection_id, operator_id)
                )
                # Commit changes
                connection.commit()
                logger.info("connectionId updated successfully.")
        except Exception as e:
            logger.info(f"Error updating {ACCOUNT} table: {e}")
        finally:
            connection.close()

    @classmethod
    def add_recovery_connection_id(cls, connection_id, operator_id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Update recovery_airbyte_connection_Id for the specified operator_id
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET recovery_airbyte_connection_Id = %s
                    WHERE operator_id = %s""", (connection_id, operator_id)
                                 )
                # Commit changes
                connection.commit()
                logger.info("connectionId updated successfully.")
        except Exception as e:
            logger.info(f"Error updating {ACCOUNT} table: {e}")
        finally:
            connection.close()

    @classmethod
    def update_connection_status(cls, connection_id, connection_status):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Get current timestamp
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Update connection status and the timestamp for the specified connection_id
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET connection_status = %s, status_updated_at = %s
                    WHERE airbyte_connection_id = %s""", (connection_status, current_time, connection_id)
                                 )
                # Commit changes
                connection.commit()
                logger.info("Connection status updated successfully.")
        except Exception as e:
            logger.info(f"Error updating connection status: {e}")
        finally:
            connection.close()

    @classmethod
    def update_created_at(cls, airbyte_source_id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Get current timestamp
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Update the created_at timestamp for the specified airbyte_source_id
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET created_at = %s
                    WHERE airbyte_source_id = %s""", (current_time, airbyte_source_id)
                                 )
                # Commit changes
                connection.commit()
                logger.info("created_at updated successfully.")
        except Exception as e:
            logger.info(f"Error updating created_at: {e}")
        finally:
            connection.close()

    @classmethod
    def update_last_updated(cls, airbyte_source_id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Get current timestamp
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Update the last_updated timestamp for the specified airbyte_source_id
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET last_updated = %s
                    WHERE airbyte_source_id = %s""", (current_time, airbyte_source_id)
                                 )
                # Commit changes
                connection.commit()
                logger.info("last_updated updated successfully.")
        except Exception as e:
            logger.info(f"Error updating last_updated: {e}")
        finally:
            connection.close()

    @classmethod
    def update_tlog_deleted_date(cls, airbyte_source_id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Get current timestamp
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Update the tlog_deleted_date timestamp for the specified airbyte_source_id
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET tlog_deleted_date = %s
                    WHERE airbyte_source_id = %s""", (current_time, airbyte_source_id)
                                 )
                # Commit changes
                connection.commit()
                logger.info("tlog_deleted_date updated successfully.")
        except Exception as e:
            logger.info(f"Error updating tlog_deleted_date: {e}")
        finally:
            connection.close()

    @classmethod
    def update_account_status(cls, airbyte_source_id, account_status):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Update account_status for the specified airbyte_source_id
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET account_status = %s
                    WHERE airbyte_source_id = %s""", (account_status, airbyte_source_id)
                                 )
                # Commit changes
                connection.commit()
                logger.info(account_status)
                logger.info("account status updated successfully.")
        except Exception as e:
            logger.info(f"Error updating account status: {e}")
        finally:
            connection.close()

    @classmethod
    def to_camel_case(cls, namespace_format):
        # Convert the namespace format string into camel case format
        words = namespace_format.split("/")
        camel_case_words = [word.title().replace(" ", "_") for word in words]
        camel_case_str = "/".join(camel_case_words)
        # Remove non-alphanumeric characters except for "-", "_", ".", "/"
        pattern = re.compile('[^a-zA-Z0-9-_./]')
        result_string = pattern.sub('', camel_case_str)
        return result_string

    @classmethod
    def connection_name_format(cls, connection_name):
        # Format connection name into camel case by splitting at "_"
        words = connection_name.split("_")
        camel_case_words = [word.title().replace(" ", "_") for word in words]
        camel_case_str = "_".join(camel_case_words)
        # Remove non-alphanumeric characters except for "-", "_", "."
        pattern = re.compile('[^a-zA-Z0-9-_.]')
        result_string = pattern.sub('', camel_case_str)
        return result_string
    @classmethod
    def fetch_recovery_source_info(cls, platform_id):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')
        JOB = os.getenv('JOB')
        JOB_DETAIL = os.getenv('JOB_DETAIL')

        try:
            with connection.cursor() as cursor:
                # Execute a complex SQL query with a Common Table Expression (CTE) to fetch recovery source info
                cursor.execute(f"""
                    with cte as (
                        SELECT j.job_id,
                            j.operator_id,
                            ac.id,
                            ac.platform_id,
                            ac.username,
                            ac.`password`,
                            ac.`status`,
                            ac.`name`,
                            ac.affiliate_login_url,
                            ac.manager,
                            ac.brt_account_id,
                            ac.postback,
                            ac.brt_password,
                            ac.password_check,
                            ac.api_key,
                            ac.start_date,
                            ac.endpoint,
                            ac.airbyte_source_id,
                            ac.recovery_airbyte_source_id,
                            ac.airbyte_connection_id,
                            ac.recovery_airbyte_connection_id,
                            ac.loopback_days,
                            ac.validation_status,
                            ac.validation_message,
                            ac.validation_date_time,
                            ac.account_status,
                            ac.created_at,
                            ac.tlog_deleted_date,
                            ac.last_updated,
                            ac.connection_status,
                            ac.status_updated_at,
                            ac.tlog_deleted,
                            jd.recovery_dates,
                            RANK() OVER (PARTITION BY j.operator_id ORDER BY j.job_id DESC) as job_rank
                        FROM {JOB} as j
                        INNER JOIN {JOB_DETAIL} as jd
                        ON j.job_id = jd.job_id
                        INNER JOIN {ACCOUNT} as ac
                        ON j.operator_id = ac.operator_id
                    )
                    SELECT cte.id,
                        cte.platform_id,
                        cte.operator_id,
                        cte.username,
                        cte.`password`,
                        cte.`status`,
                        cte.`name`,
                        cte.affiliate_login_url,
                        cte.manager,
                        cte.brt_account_id,
                        cte.postback,
                        cte.brt_password,
                        cte.password_check,
                        cte.api_key,
                        cte.start_date,
                        cte.endpoint,
                        cte.airbyte_source_id,
                        cte.recovery_airbyte_source_id,
                        cte.airbyte_connection_id,
                        cte.recovery_airbyte_connection_id,
                        cte.loopback_days,
                        cte.validation_status,
                        cte.validation_message,
                        cte.validation_date_time,
                        cte.account_status,
                        cte.created_at,
                        cte.tlog_deleted_date,
                        cte.last_updated,
                        cte.connection_status,
                        cte.status_updated_at,
                        cte.tlog_deleted,
                        GROUP_CONCAT(cte.recovery_dates separator ',' ) AS recovery_dates
                    FROM cte
                    WHERE job_rank = 1 
                        AND cte.recovery_dates IS NOT NULL 
                        AND cte.platform_id = {platform_id}
                    GROUP BY cte.job_id,
                            cte.operator_id,
                            cte.id,
                            cte.platform_id,
                            cte.username,
                            cte.`password`,
                            cte.`status`,
                            cte.`name`,
                            cte.affiliate_login_url,
                            cte.manager,
                            cte.brt_account_id,
                            cte.postback,
                            cte.brt_password,
                            cte.password_check,
                            cte.api_key,
                            cte.start_date,
                            cte.endpoint,
                            cte.airbyte_source_id,
                            cte.recovery_airbyte_source_id,
                            cte.airbyte_connection_id,
                            cte.recovery_airbyte_connection_id,
                            cte.loopback_days,
                            cte.validation_status,
                            cte.validation_message,
                            cte.validation_date_time,
                            cte.account_status,
                            cte.created_at,
                            cte.tlog_deleted_date,
                            cte.last_updated,
                            cte.connection_status,
                            cte.status_updated_at,
                            cte.tlog_deleted;
                """)
                # Fetch all the results
                result = cursor.fetchall()
                # Get column names for result mapping
                column_names = [description[0] for description in cursor.description]
                row_list = []
                for row in result:
                    # Map each row to column names as a dictionary
                    result_columns = dict(zip(column_names, row))
                    row_list.append(result_columns)
                return row_list

        except Error as e:
            # Log error if any occurs during query execution
            logger.info(f"Error: {e}")
        finally:
            connection.close()

    @classmethod
    def update_account_status_for_valid_source(cls):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Update account status for sources that are 'Valid' and have no connection IDs
                query = f"""
                    UPDATE {ACCOUNT}
                    SET account_status = %s
                    WHERE validation_status = 'Valid'
                        AND (airbyte_connection_id = '' OR airbyte_connection_id IS NULL) 
                        AND (airbyte_source_id <> '' or airbyte_source_id is not null)
                """
                cursor.execute(query, ('Updated',))
                connection.commit()
                logger.info("account_status updated successfully to updated.")
        except Exception as e:
            # Log any errors that occur
            logger.error(f"Error updating account_status: {e}")
        finally:
            connection.close()

    @classmethod
    def get_incorrect_sources(cls):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Select operator_id and airbyte_source_id where account_status is 'New' or 'Updated'
                cursor.execute(f"""
                    SELECT operator_id, airbyte_source_id
                    FROM {ACCOUNT}
                    WHERE account_status IN ('New', 'Updated');
                """)
                result = cursor.fetchall()
                # Get column names to map to row data
                column_names = [description[0] for description in cursor.description]
                row_list = []

                for row in result:
                    # Convert rows to dictionaries with column names as keys
                    result_columns = dict(zip(column_names, row))
                    row_list.append(result_columns)

                return row_list
        except Exception as e:
            logger.info(f"Error: {e}")
        finally:
            connection.close()

    @classmethod
    def update_account_status_for_invalid_sources(cls, operator_id, account_status):
        connection = mysql_conn()
        ACCOUNT = os.getenv('ACCOUNT')

        try:
            with connection.cursor() as cursor:
                # Update account status for the given operator_id
                cursor.execute(f"""
                    UPDATE {ACCOUNT}
                    SET account_status = %s
                    WHERE operator_id = %s""", (account_status, operator_id)
                                 )
                connection.commit()
                logger.info(account_status)
                logger.info("account status updated successfully.")
        except Exception as e:
            logger.info(f"Error updating account status: {e}")
        finally:
            connection.close()

    @classmethod
    def get_s3_path(cls, operator_id):
        connection = mysql_conn()
        DATA_SOURCE = os.getenv("data_source")

        try:
            with connection.cursor() as cursor:
                # Fetch S3 path based on the operator_id
                cursor.execute(f"""
                    SELECT path
                    FROM {DATA_SOURCE}
                    WHERE operator_id = {operator_id};
                """)
                result = cursor.fetchall()
                # Get column names to map to row data
                column_names = [description[0] for description in cursor.description]
                row_list = []
                for row in result:
                    # Convert rows to dictionaries with column names as keys
                    result_columns = dict(zip(column_names, row))
                    row_list.append(result_columns)
                return row_list
        except Exception as e:
            logger.info(f"Error: {e}")
        finally:
            connection.close()

    @classmethod
    def fetch_path_from_data_source(cls, connection_id):
        connection = mysql_conn()
        data_source = os.getenv("data_source")
        cursor = connection.cursor()
        # Fetch the path for a given recovery_airbyte_connection_id
        cursor.execute(f"""
            SELECT path 
            FROM {data_source} 
            WHERE recovery_airbyte_connection_id = '{connection_id}';
        """)
        result = cursor.fetchall()
        cursor.close()
        connection.close()
        return result

    @classmethod
    def check_conflict(cls, conn_id):
        airbyte_host = os.getenv("airbyte_server")
        endpoint = "api/v1/jobs/get_last_replication_job"
        base_url = airbyte_host + endpoint
        basic_auth = HTTPBasicAuth("airbyte", "password")
        payload = {"connectionId": conn_id}
        headers = {"accept": "application/json", "content-type": "application/json"}
        
        try:
            # Make an API request to check for conflict using the Airbyte server
            response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)
            if response.status_code == 200:
                # If request is successful, return the response JSON
                response_json = response.json() 
                return response_json
            else:
                logger.info(f"Failed to get_last_replication_job DAG . Status Code: {response.status_code}")
        except Exception as e:
            logger.info(f"An error occurred: {e}")