"""
Helper functions for reprocess DAGs.

This module provides common utilities for all platform reprocess DAGs including:
- Enhanced error logging and callbacks
- Wrapper functions with parameter validation
- Standardized error messages

Usage:
    from airbyte.reprocess_helpers import handle_recovery_failure, update_recovery_sources_wrapper

    operator_task = PythonOperator(
        task_id=stage_1,
        python_callable=update_recovery_sources_wrapper,
        on_failure_callback=handle_recovery_failure,
        op_kwargs={"source_info": res, "operator_id": operator_id},
        params={"operator_id": operator_id},
        provide_context=True,
        ...
    )
"""

import logging
from airbyte.create_sources_and_connections.zero_recovery import update_recovery_sources

logger = logging.getLogger(__name__)


def handle_recovery_failure(context):
    """
    Custom failure callback for recovery tasks that provides clear logging
    without causing additional errors.

    Args:
        context: Airflow task context dictionary

    Logs:
        - Task details (ID, DAG run, operator ID)
        - Exception type and message
        - All arguments that were passed to the task
    """
    task_instance = context.get('task_instance')
    exception = context.get('exception')

    logger.error("=" * 80)
    logger.error("RECOVERY TASK FAILED")
    logger.error("=" * 80)
    logger.error(f"Task ID: {task_instance.task_id}")
    logger.error(f"Operator ID: {context.get('params', {}).get('operator_id', 'Unknown')}")
    logger.error(f"Dag Run: {task_instance.dag_id} - {task_instance.run_id}")
    logger.error(f"Exception Type: {type(exception).__name__}")
    logger.error(f"Exception Message: {str(exception)}")
    logger.error("=" * 80)

    # Log the arguments that were passed
    ti = context['ti']
    if ti.op_kwargs:
        logger.error("Arguments passed to task:")
        for key, value in ti.op_kwargs.items():
            if isinstance(value, dict):
                logger.error(f"  {key}: (dict with {len(value)} keys)")
                for k, v in value.items():
                    logger.error(f"    {k}: {v}")
            else:
                logger.error(f"  {key}: {value}")

    logger.error("This failure will NOT prevent other operators from processing.")
    logger.error("=" * 80)


def update_recovery_sources_wrapper(source_info, operator_id, **context):
    """
    Wrapper function that logs arguments before calling update_recovery_sources
    and provides clear error messages if something fails.

    This wrapper:
    1. Extracts TaskInstance from context
    2. Logs all input parameters for debugging
    3. Calls update_recovery_sources with correct signature
    4. Provides detailed error messages on failure

    Args:
        source_info (dict): Recovery source information from database
        operator_id (int): Operator ID being processed
        **context: Airflow context including 'ti' (TaskInstance)

    Returns:
        Result from update_recovery_sources

    Raises:
        TypeError: If function signature doesn't match
        Exception: Any other error from update_recovery_sources
    """
    ti = context.get('ti')

    logger.info("=" * 80)
    logger.info("STARTING RECOVERY FOR OPERATOR")
    logger.info("=" * 80)
    logger.info(f"Operator ID: {operator_id}")
    logger.info(f"Task Instance: {ti.task_id if ti else 'None'}")
    logger.info(f"Source Info Keys: {list(source_info.keys()) if isinstance(source_info, dict) else 'Not a dict'}")

    # Log all source_info details
    if isinstance(source_info, dict):
        logger.info("Source Info Details:")
        for key, value in source_info.items():
            logger.info(f"  {key}: {value}")

    logger.info("Arguments being passed to update_recovery_sources:")
    logger.info(f"  - ti (TaskInstance): {ti}")
    logger.info(f"  - source_info: dict with {len(source_info)} keys" if isinstance(source_info, dict) else f"  - source_info: {type(source_info)}")
    logger.info(f"  - operator_id: {operator_id}")
    logger.info("=" * 80)

    try:
        # Call the actual recovery function with correct parameters
        # IMPORTANT: update_recovery_sources requires ti as first parameter
        result = update_recovery_sources(ti=ti, source_info=source_info, operator_id=operator_id)

        logger.info("=" * 80)
        logger.info("RECOVERY COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Operator ID: {operator_id}")
        logger.info(f"Result: {result}")
        logger.info("=" * 80)

        return result

    except TypeError as e:
        logger.error("=" * 80)
        logger.error("MISSING OR WRONG ARGUMENTS ERROR")
        logger.error("=" * 80)
        logger.error(f"Operator ID: {operator_id}")
        logger.error(f"Error: {str(e)}")
        logger.error("This usually means the function signature doesn't match what we're passing")
        logger.error("Expected: update_recovery_sources(ti, source_info, operator_id)")
        logger.error("=" * 80)
        raise

    except Exception as e:
        logger.error("=" * 80)
        logger.error("ERROR DURING RECOVERY")
        logger.error("=" * 80)
        logger.error(f"Operator ID: {operator_id}")
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Error Message: {str(e)}")
        logger.error("=" * 80)
        raise
