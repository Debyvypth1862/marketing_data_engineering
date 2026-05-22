import mysql.connector
from mysql.connector import Error
import snowflake.connector
import snowflake.connector.errors
import sys
sys.path.insert(3,"dags/airbyte")
import os
import logging

logger = logging.getLogger(__name__)

"""
Establishes a connection to the MySQL database using credentials from environment variables.

Returns:
    connection (mysql.connector.connect object) if successful, or None if there's an error.
"""
def mysql_conn():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("host"),
            database=os.getenv("database"),
            user=os.getenv("user"),
            password=os.getenv("password")
        )
        return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        return None
    
def snowflake_conn(query):
    """
    Executes a query on Snowflake database and returns the result.

    Args:
        query (str): SQL query to execute

    Returns:
        result (list): Query results as list of tuples, or None if there's an error.
    """
    connection = None
    cursor = None
    try:
        connection = snowflake.connector.connect(
            user=os.getenv("DBT_USER"),
            password=os.getenv("DBT_PASSWORD"),
            account=os.getenv("DBT_ACCOUNT"),
            warehouse=os.getenv("DBT_WAREHOUSE"),
            role=os.getenv("DBT_ROLE")
        )
        logger.info("Successfully connected to Snowflake database")
        
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        logger.info(f"Query executed successfully, returned {len(result)} rows")
        return result
        
    except snowflake.connector.errors.Error as e:
        logger.error(f"Failed to execute Snowflake query: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()