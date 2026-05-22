import logging

from jsonschema import Draft7Validator
from mysql.connector import Error
import polars as pl
from airbyte import constants
from db_connection import mysql_conn

task_logger = logging.getLogger("airflow.task")


def primary_key_validation(df: pl.DataFrame, primary_key_list, platform):
    """
    Validates the primary key for a given row of data based on the platform and provided primary key columns.

    Args:
        df (polars.DataFrame): The dataframe to validate.
        primary_key_list (list): A list of column names to use for the primary key.
        platform (str): The platform to validate against (e.g., 'q', 'brc', etc.).

    Returns:
        tuple: A tuple containing validation result, and the error message.
    """
    try:
        # Platform: SoftSwiss
        if platform == str.lower(constants.SoftSwiss):
            flattened_data = [flatten_data(row) for row in df["data"]]
            final_df = pl.DataFrame(flattened_data)
            # Add other fields from the original DataFrame
            for col in df.columns:
                if col != "data":
                    final_df = final_df.with_columns(pl.lit(df[col]).alias(col))
        # these platform has a "data" nested inside "_airbyte_data"
        elif platform in [
            str.lower(constants.Q),
            str.lower(constants.Cellxpert),
            str.lower(constants.Sweep),
            str.lower(constants.EGO),
            str.lower(constants.Mexos),
            str.lower(constants.Smartico),
            str.lower(constants.Buffalo_Partners),
            str.lower(constants.NetRefer),
            str.lower(constants.Referon),
            str.lower(constants.Alanbase),
        ]:
            final_df = df.unnest("data")
        else:
            final_df = df

        error_message = None
        # Check if any column in the primary key list is not available in the data
        unavailable_columns = []
        data_columns = final_df.columns
        for col in primary_key_list:
            if col not in data_columns:
                unavailable_columns.append(col)
        if unavailable_columns:
            error_message = f"Columns {','.join(unavailable_columns)} are not available in the data. Failing primary key validation."
            return False, error_message

        primary_key_is_unique = (
            final_df.group_by(primary_key_list)
            .agg(pl.len())
            .filter(pl.col("len") > 1)
            .is_empty()
        )
        if not primary_key_is_unique:
            error_message = f"Primary key validation because of duplicate values. Showing top 5 duplicate values: {final_df.group_by(primary_key_list).agg(pl.len()).filter(pl.col('len') > 1).sort('len', descending=True).head(5).to_dicts()}"
        return primary_key_is_unique, error_message

    except Exception as e:
        task_logger.info(e)
        task_logger.info("problem in primary key validation")
        return False, str(e)


def flatten_data(data_list):
    result = {}
    for item in data_list:
        name = item.get("name")
        value = item.get("value")
        if name:
            result[name] = value
    return result


def schema_validation(data, schema_read):
    """
    Validates the data against the provided schema.

    Args:
        data (dict): The data to validate.
        schema_read (dict): The schema to validate against.

    Returns:
        str: Error message if validation fails, or True if validation passes.
    """
    task_logger.info("Inside method schema_validation")

    # Initialize Draft7Validator with the provided schema
    validator = Draft7Validator(schema_read)
    errors = list(validator.iter_errors(data))

    # Check for validation errors
    if errors:
        for error in errors:
            path = list(error.path)
            if error.validator == "required":
                return f"Required column is missing: {error.message}"
            elif error.validator == "additionalProperties":
                return f"Additional property error: {error.message}"
            elif error.validator == "type":
                if path:
                    column = path[-1]
                    return f"Data type mismatch for column '{column}': {error.message}"
                else:
                    return f"{error.message}"
            else:
                return f"{error.message}"
    else:
        task_logger.info("no error")
        return True


def get_schema(schema_id):
    """
    Fetches the schema for a given schema ID from the database.

    Args:
        schema_id (int): The schema ID to retrieve.

    Returns:
        str: The schema data as a string, or None if not found.
    """
    try:
        connection = mysql_conn()
        with connection.cursor() as cursor:
            task_logger.info(f"schema id --->{schema_id}")
            cursor.execute(f"""
                SELECT data_schema
                FROM MASTER_JSON_SCHEMA
                WHERE id = {schema_id};
            """)
            myresult = cursor.fetchall()
            if myresult:
                return myresult[0][0]
            else:
                return None
    except Error as e:
        task_logger.error(f"Error: {e}")
    finally:
        connection.close()
