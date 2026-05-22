import os
import sys
import logging
import mysql.connector
import snowflake.connector

sys.path.insert(1, "dags/airbyte")
from Utils import Utils
from db_connection import mysql_conn

logger = logging.getLogger(__name__)

'''transferring details from snowflake to ACCOUNT table'''
def data_transfer():
    connection = mysql_conn()
   
    ACCOUNT = os.getenv("ACCOUNT")
    data_to_transfer = Utils.fetch_snowflake_data()
    
    # Create and insert values in temp table
    Utils.create_temp_table(connection)
    Utils.insert_into_temp_table(data_to_transfer, connection)

    # Add/insert rows 
    Utils.insert_into_operator(ACCOUNT, connection)
    logger.info('inside insert query')
    
    # Delete duplicates from operator table
    Utils.delete_duplicate_records(ACCOUNT, connection)

    # Modify rows 
    Utils.modify_operator(ACCOUNT, connection)
    logger.info('inside modify query')
    
    # Delete rows
    Utils.delete_operator_rows(ACCOUNT, connection)
    logger.info('inside delete query')

    connection.commit()
    connection.close()
