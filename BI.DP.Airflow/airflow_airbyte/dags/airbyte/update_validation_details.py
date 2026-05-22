import os
import sys

sys.path.insert(1, "dags/airbyte")
import logging
from datetime import datetime
from airbyte.db_connection import mysql_conn

task_logger = logging.getLogger("airflow.task")
ACCOUNT = os.getenv("ACCOUNT")

'''The update_validation_data method inserts validation status, message, 
and timestamp for a given id into the ACCOUNT_VALIDATION table, logging success or any errors that occur during the process.'''
def update_validation_data(id, validation_status, message):
    connection = mysql_conn()
    ACCOUNT_VALIDATION = os.getenv("ACCOUNT_VALIDATION")
    try:
        with connection.cursor() as cursor:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(f"""
                INSERT INTO {ACCOUNT_VALIDATION}
                    (account_id, validation_status, validation_message, validation_date_time)
                VALUES (%s, %s, %s, %s)
            """, (id, validation_status, message, current_time)
            )
            connection.commit()
            task_logger.info("Validation data updated successfully.")
    except Exception as e:
        task_logger.info(f"Error updating validation data: {e}")
    finally:
        connection.close()

'''The update_validation_details method updates the validation status, message, and other related information for each source in the ACCOUNT table based on the validation data pulled from previous tasks, 
and also calls update_validation_data to log the validation results.'''
def update_validation_details(ti, task_names):
    connection = mysql_conn()
    cursor = connection.cursor()
    validation_list = ti.xcom_pull(key="validate_operators", task_ids=task_names)
    print(validation_list)
    for validate_list in validation_list:
        for validate in validate_list:
            source_id = validate["sourceId"]
            id = validate["Id"]
            message = validate["validation_message"]
            validation_status = validate["validation_status"]
            is_validation_enabled = validate["is_validation_enabled"]
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(id)

            cursor.execute(f"""
                UPDATE {ACCOUNT}
                SET Validation_status = %s, validation_message = %s, is_validation_enabled = %s, 
                    validation_date_time = %s
                WHERE airbyte_source_id = %s;
                """, (validation_status, message, is_validation_enabled, current_time,source_id)
            )
            update_validation_data(id, validation_status, message)

        connection.commit()
    cursor.close()
    connection.close()
