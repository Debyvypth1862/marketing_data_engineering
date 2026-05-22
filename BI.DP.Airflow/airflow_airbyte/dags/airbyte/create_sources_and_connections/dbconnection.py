import mysql.connector
from mysql.connector import Error
import sys
sys.path.insert(3,"dags/airbyte")
import os
import logging

_host = os.getenv("host")
_database = os.getenv("database")
_user = os.getenv("user")
_password = os.getenv("password")
logger = logging.getLogger(__name__)


def mysql_conn():
    """
    Establishes a connection to the MySQL database using credentials from environment variables.
    
    Returns:
        connection (mysql.connector.connect object) if successful, or None if there's an error.
    """
    try:
        # Establish the connection using parameters from environment variables
        connection = mysql.connector.connect(
            host=_host,
            database=_database,
            user=_user,
            password=_password
        )
        
        # If the connection is successful, return the connection object
        return connection
    except Error as e:
        # Log an error message if the connection fails
        logger.info(f"Error connecting to MySQL: {e}")
        
        # Return None if the connection cannot be established
        return None