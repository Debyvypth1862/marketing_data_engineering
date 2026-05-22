"""
Redtrack Conversion Upload DAG

This DAG handles daily uploads of conversion data to Redtrack API for multiple platforms.
It processes conversions for Bing, Google, Refills, Taboola, and high_value campaigns.

Schedule: Daily at 6:00 AM PST (2:00 PM UTC)
Purpose: Upload purchase_value, purchase_value_reporting, and high_value conversions from Snowflake to Redtrack

Required Airflow Variables:
- redtrack_api_key: API key for Redtrack API
- slack_webhook_conversion_upload: Slack webhook URL for notifications
- snowflake_user: Snowflake username
- snowflake_password: Snowflake password
- snowflake_account: Snowflake account identifier
- bing_campaign_id: Redtrack campaign ID for Bing (also used for Bing high_value)
- google_campaign_id: Redtrack campaign ID for Google (also used for Google high_value)
- refills_default_campaign_id: Default Redtrack campaign ID for Refills Google
- refills_facebook_campaign_id: Default Redtrack campaign ID for Refills Facebook
- taboola_campaign_id: Redtrack campaign ID for Taboola

Features:
- Parallel processing of independent upload tasks
- Automatic retry logic with exponential backoff
- Idempotent operations using control tables
- Detailed Slack notifications with statistics
- Error handling and logging
- Creates SPREE_HIGH_VALUE and SPREE_HIGH_VALUE_BING tables from views before high_value uploads
"""

import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airbyte.slack_alerts import task_id_slack_failure_alert
from airbyte.conversion_upload_module import (
    upload_bing_purchase_value,
    upload_google_purchase_value,
    upload_google_purchase_value_reporting,
    upload_refills_google_purchase_value,
    upload_refills_facebook_purchase_value,
    upload_taboola_purchase_value,
    upload_bing_high_value,
    # upload_google_high_value  # COMMENTED OUT - Running manually
)

logger = logging.getLogger("airflow.task")

# DAG configuration
dag_id = "RedtrackConversionUpload"

# ============================================================================
# REQUIRED AIRFLOW VARIABLES
# ============================================================================
# Set these variables in Airflow UI (Admin -> Variables) before running the DAG
#
# API Configuration:
#   - redtrack_api_key: Redtrack API key
#   - slack_webhook_conversion_upload: Slack webhook URL for notifications
#
# Snowflake Configuration:
#   - snowflake_user: Snowflake username
#   - snowflake_password: Snowflake password
#   - snowflake_account: Snowflake account identifier
#
# Campaign IDs:
#   - bing_campaign_id: Redtrack campaign ID for Bing
#   - google_campaign_id: Redtrack campaign ID for Google and high_value
#   - refills_default_campaign_id: Default campaign ID for Refills Google
#   - refills_facebook_campaign_id: Default campaign ID for Refills Facebook
#   - taboola_campaign_id: Redtrack campaign ID for Taboola
#
# CLI Commands to set variables:
# airflow variables set redtrack_api_key "your_api_key_here"
# airflow variables set slack_webhook_conversion_upload "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
# airflow variables set snowflake_user "your_username"
# airflow variables set snowflake_password "your_password_here"
# airflow variables set snowflake_account "your-account-id"
# airflow variables set bing_campaign_id "your_campaign_id"
# airflow variables set google_campaign_id "your_campaign_id"
# airflow variables set refills_default_campaign_id "your_campaign_id"
# airflow variables set refills_facebook_campaign_id "your_campaign_id"
# airflow variables set taboola_campaign_id "your_campaign_id"
# ============================================================================

# List of required variables
REQUIRED_VARIABLES = [
    'redtrack_api_key',
    'slack_webhook_conversion_upload',
    'snowflake_user',
    'snowflake_password',
    'snowflake_account',
    'bing_campaign_id',
    'google_campaign_id',
    'refills_default_campaign_id',
    'refills_facebook_campaign_id',
    'taboola_campaign_id'
]

def validate_airflow_variables(**context):
    """
    Validate that all required Airflow Variables are set.
    This runs as a task at the beginning of the DAG execution.
    """
    logger.info("Validating required Airflow Variables...")
    missing_variables = []

    for var_name in REQUIRED_VARIABLES:
        try:
            Variable.get(var_name)
            logger.info(f"✓ Variable '{var_name}' is set")
        except KeyError:
            missing_variables.append(var_name)
            logger.error(f"✗ Variable '{var_name}' is missing")

    if missing_variables:
        error_msg = f"""
================================================================================
ERROR: Missing Required Airflow Variables for DAG '{dag_id}'
================================================================================

The following variables are not set:
{chr(10).join(f'  - {var}' for var in missing_variables)}

Please set these variables using one of these methods:

1. Via Airflow UI:
   Admin -> Variables -> Add (+)

2. Via Airflow CLI:
{chr(10).join(f'   airflow variables set {var} "your_value_here"' for var in missing_variables)}

See the REQUIRED AIRFLOW VARIABLES section in this file for details and examples.
================================================================================
        """
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"✅ All {len(REQUIRED_VARIABLES)} required Airflow Variables are configured")
    return True

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_id_slack_failure_alert,
}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="Upload conversion data to Redtrack for Bing, Google, Refills, Taboola, and high value",
    schedule_interval="0 14 * * *",  # 2:00 PM UTC = 6:00 AM PST
    start_date=datetime(2025, 10, 10),
    catchup=False,
    max_active_runs=1,
    tags=["redtrack", "conversion_upload", "daily"]
) as dag:

    # Start task
    start = DummyOperator(
        task_id="start",
        dag=dag
    )

    # Validate required Airflow Variables
    validate_vars = PythonOperator(
        task_id="validate_airflow_variables",
        python_callable=validate_airflow_variables,
        provide_context=True,
        dag=dag
    )

    # Bing purchase_value upload task
    bing_upload = PythonOperator(
        task_id="bing_purchase_value_upload",
        python_callable=upload_bing_purchase_value,
        provide_context=True,
        dag=dag
    )

    # Google purchase_value upload task
    google_pv_upload = PythonOperator(
        task_id="google_purchase_value_upload",
        python_callable=upload_google_purchase_value,
        provide_context=True,
        dag=dag
    )

    # Google purchase_value_reporting upload task
    google_pvr_upload = PythonOperator(
        task_id="google_purchase_value_reporting_upload",
        python_callable=upload_google_purchase_value_reporting,
        provide_context=True,
        dag=dag
    )

    # Refills Google purchase_value upload task
    refills_google_upload = PythonOperator(
        task_id="refills_google_purchase_value_upload",
        python_callable=upload_refills_google_purchase_value,
        provide_context=True,
        dag=dag
    )

    # Refills Facebook purchase_value upload task
    refills_facebook_upload = PythonOperator(
        task_id="refills_facebook_purchase_value_upload",
        python_callable=upload_refills_facebook_purchase_value,
        provide_context=True,
        dag=dag
    )

    # Taboola purchase_value upload task
    taboola_upload = PythonOperator(
        task_id="taboola_purchase_value_upload",
        python_callable=upload_taboola_purchase_value,
        provide_context=True,
        dag=dag
    )

    # Create SPREE_HIGH_VALUE table from view for Google
    # COMMENTED OUT - Running manually via test/high_value_conversion_upload.py
    # create_high_value_table = SnowflakeOperator(
    #     task_id="create_high_value_table",
    #     sql="""
    #     CREATE OR REPLACE TABLE EXP.SPREE.SPREE_HIGH_VALUE AS
    #     SELECT *
    #     FROM EXP.SPREE.V_SPREE_HIGH_VALUE
    #     """,
    #     snowflake_conn_id="snowflake_conn",
    #     dag=dag
    # )

    # Google high_value upload task
    # COMMENTED OUT - Running manually via test/high_value_conversion_upload.py
    # google_high_value_upload = PythonOperator(
    #     task_id="google_high_value_upload",
    #     python_callable=upload_google_high_value,
    #     provide_context=True,
    #     dag=dag
    # )

    # Create SPREE_HIGH_VALUE_BING table from view for Bing
    create_bing_high_value_table = SnowflakeOperator(
        task_id="create_bing_high_value_table",
        sql="""
        CREATE OR REPLACE TABLE EXP.SPREE.SPREE_HIGH_VALUE_BING AS
        SELECT *
        FROM EXP.SPREE.V_SPREE_HIGH_VALUE_BING
        """,
        snowflake_conn_id="snowflake_conn",
        dag=dag
    )

    # Bing high_value upload task
    bing_high_value_upload = PythonOperator(
        task_id="bing_high_value_upload",
        python_callable=upload_bing_high_value,
        provide_context=True,
        dag=dag
    )

    # End task
    end = DummyOperator(
        task_id="end",
        trigger_rule="all_done",
        dag=dag
    )

    # Define task dependencies
    # Validate variables first, then run all upload tasks in parallel
    # high_value uploads depend on creating the tables first, but runs in parallel with other uploads
    # Note: google_high_value is commented out - running manually
    start >> validate_vars >> [bing_upload, google_pv_upload, google_pvr_upload, refills_google_upload, refills_facebook_upload, taboola_upload, create_bing_high_value_table]
    # create_high_value_table >> google_high_value_upload  # COMMENTED OUT
    create_bing_high_value_table >> bing_high_value_upload
    [bing_upload, google_pv_upload, google_pvr_upload, refills_google_upload, refills_facebook_upload, taboola_upload, bing_high_value_upload] >> end
