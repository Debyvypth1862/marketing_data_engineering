#!/usr/bin/env python3
"""
Play55 Pipeline DAG
Pipeline: API → MySQL → Airbyte Sync → dbt Transform

This DAG orchestrates the Play55 data pipeline leveraging Airflow native operators:
1. Extract: Fetch sales data from Play55 API using HTTP Hook
2. Load to MySQL: Insert data using MySQL Hook
3. Sync: Trigger Airbyte sync from MySQL to Snowflake
4. Transform: Run dbt model using Astronomer Cosmos

All credentials and configurations are managed via Airflow Connections and Variables.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Set, List, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup

# Astronomer Cosmos imports for dbt
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Add helpers directory to path for importing play55_api module
DAGS_DIR = Path(__file__).parent
HELPERS_DIR = DAGS_DIR / "airbyte" / "helpers"
DBT_PROJECT_DIR = DAGS_DIR / "dbt" / "play55"
sys.path.insert(0, str(HELPERS_DIR))

from play55_api import (
    Play55ApiClient,
    get_date_range,
    get_existing_hashes,
    fetch_and_prepare_records,
    insert_records_with_cursor,
    prepare_record_values,
    get_insert_sql,
    TABLE_NAME,
)
from play55_quality_check import (
    get_gsheet_client,
    get_gsheet_record_counts,
    get_snowflake_record_counts,
    compare_counts,
    format_quality_report,
    send_slack_notification,
)


# ====== Airflow Variables (set these in Airflow UI or via CLI) ======
# play55_api_base_url: <your-api-base-url>
# play55_api_auth_token: <your-auth-token>
# play55_airbyte_connection_id: <your-airbyte-connection-uuid>
# play55_dbt_project_path: /opt/airflow/dags/dbt/play55
# play55_always_today: true
# play55_lookback_days: 3  (0=today only, 1=yesterday+today, 2=last 3 days, etc.)

# ====== Airflow Connections (set these in Airflow UI or via CLI) ======
# mysql_play55: MySQL connection for bi_data database
# airflow-call-to-airbyte: HTTP connection to Airbyte API
# snowflake_conn: Snowflake connection for Cosmos/dbt


# ====== Default Args ======

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# ====== Task Functions ======

def check_required_variables(**context) -> Dict:
    """
    Check that all required Airflow variables are configured.
    Fails the task if any required variable is missing.

    Returns dict with variable status for XCom.
    """
    from airflow.exceptions import AirflowException

    # Required variables (no defaults - must be explicitly set)
    required_vars = [
        "play55_airbyte_connection_id",
    ]

    # Optional variables with defaults (just log their values)
    optional_vars = {
        "play55_api_base_url": "<your-api-base-url>",
        "play55_api_auth_token": "<your-auth-token>",
        "play55_dbt_project_path": str(DBT_PROJECT_DIR),
        "play55_always_today": "true",
        "play55_lookback_days": "3",
    }

    # DBT variables (optional, used by Cosmos profile)
    dbt_vars = [
        "DBT_DATABASE",
        "DBT_SCHEMA",
        "DBT_WAREHOUSE",
        "DBT_ROLE",
    ]

    missing_vars = []
    configured_vars = {}

    # Check required variables
    print("=" * 60)
    print("CHECKING REQUIRED VARIABLES")
    print("=" * 60)

    for var_name in required_vars:
        try:
            value = Variable.get(var_name)
            configured_vars[var_name] = "***SET***"
            print(f"✓ {var_name}: configured")
        except KeyError:
            missing_vars.append(var_name)
            print(f"✗ {var_name}: MISSING (required)")

    # Check optional variables
    print("\n" + "=" * 60)
    print("CHECKING OPTIONAL VARIABLES (with defaults)")
    print("=" * 60)

    for var_name, default_val in optional_vars.items():
        try:
            value = Variable.get(var_name)
            configured_vars[var_name] = "***SET***"
            print(f"✓ {var_name}: configured")
        except KeyError:
            configured_vars[var_name] = f"using default: {default_val}"
            print(f"○ {var_name}: using default")

    # Check DBT environment variables
    print("\n" + "=" * 60)
    print("CHECKING DBT ENVIRONMENT VARIABLES")
    print("=" * 60)

    for var_name in dbt_vars:
        value = os.getenv(var_name)
        if value:
            configured_vars[var_name] = "***SET***"
            print(f"✓ {var_name}: configured")
        else:
            print(f"○ {var_name}: not set (will use default)")

    # Fail if required variables are missing
    if missing_vars:
        error_msg = f"Missing required Airflow variables: {', '.join(missing_vars)}"
        print(f"\n{'=' * 60}")
        print(f"ERROR: {error_msg}")
        print(f"{'=' * 60}")
        raise AirflowException(error_msg)

    print(f"\n{'=' * 60}")
    print("ALL REQUIRED VARIABLES CONFIGURED")
    print(f"{'=' * 60}")

    return {
        "status": "success",
        "configured_vars": configured_vars,
        "missing_vars": missing_vars,
    }


def extract_and_load_to_mysql(**context) -> Dict:
    """
    Extract data from Play55 API and load into MySQL using Airflow hooks.

    Uses:
    - Airflow Variables for API configuration
    - MySqlHook for database operations (sales data)
    - SnowflakeHook for control table operations (pairs tracking)

    Returns dict with execution stats for XCom.
    """
    # Get configuration from Airflow Variables
    api_base_url = Variable.get("play55_api_base_url", default_var="<your-api-base-url>")
    api_auth_token = Variable.get("play55_api_auth_token", default_var="<your-auth-token>")
    always_today = Variable.get("play55_always_today", default_var="true").lower() == "true"
    # Lookback days: fetch data from N days ago to today to catch late-arriving records
    lookback_days = int(Variable.get("play55_lookback_days", default_var="3"))

    # Use MySqlHook for MySQL database connection (sales data)
    mysql_hook = MySqlHook(mysql_conn_id="mysql_play55")
    print(f"Connecting to MySQL via Airflow connection 'mysql_play55'...")
    mysql_conn = mysql_hook.get_conn()
    mysql_cursor = mysql_conn.cursor()

    # Use SnowflakeHook for control table operations
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    print(f"Connecting to Snowflake via Airflow connection 'snowflake_conn'...")
    snowflake_conn = snowflake_hook.get_conn()
    snowflake_cursor = snowflake_conn.cursor()

    try:
        # Get existing hashes for deduplication (from MySQL)
        existing_hashes = get_existing_hashes(mysql_cursor)
        print(f"Existing records in MySQL database: {len(existing_hashes)}")

        # Fetch and prepare new records from API (with Snowflake control table tracking)
        new_records = fetch_and_prepare_records(
            api_base_url=api_base_url,
            api_auth_token=api_auth_token,
            existing_hashes=existing_hashes,
            always_today=always_today,
            lookback_days=lookback_days,
            snowflake_cursor=snowflake_cursor,
        )

        if not new_records:
            print("No new records to insert.")
            return {
                "total_fetched": 0,
                "new_records": 0,
                "duplicates_skipped": 0
            }

        # Log sample data before insert
        print(f"\n{'=' * 60}")
        print(f"SAMPLE DATA BEFORE INSERT (first 5 records)")
        print(f"{'=' * 60}")
        for i, record in enumerate(new_records[:5]):
            print(f"\n--- Record {i + 1} ---")
            for key, value in record.items():
                print(f"  {key}: {value}")
        if len(new_records) > 5:
            print(f"\n... and {len(new_records) - 5} more records")
        print(f"{'=' * 60}\n")

        # Insert new records into MySQL using cursor
        inserted = insert_records_with_cursor(mysql_cursor, new_records)
        mysql_conn.commit()

        print(f"Successfully inserted {inserted} new records into MySQL.")

        return {
            "total_fetched": len(new_records),
            "new_records": inserted,
            "duplicates_skipped": 0
        }

    finally:
        mysql_cursor.close()
        mysql_conn.close()
        snowflake_cursor.close()
        snowflake_conn.close()


def check_extraction_results(**context) -> bool:
    """
    Check if extraction produced any new records.
    Used to decide if downstream tasks should run.
    """
    ti = context['ti']
    stats = ti.xcom_pull(task_ids='extract_and_load_to_mysql')

    if stats and stats.get('new_records', 0) > 0:
        print(f"New records inserted: {stats['new_records']}. Proceeding with sync.")
        return True
    else:
        print("No new records. Skipping downstream tasks.")
        return False


def quality_check_gsheet_vs_snowflake(**context) -> Dict:
    """
    Compare record counts between Google Sheet and Snowflake.
    Runs after Airbyte sync to validate data consistency.

    Uses:
    - Google Sheets API (gspread) for reading sheet data
    - SnowflakeHook for reading Snowflake data

    Returns dict with comparison results for XCom.
    """
    # Get configuration from Airflow Variables
    gsheet_credentials = Variable.get("play55_gsheet_credentials")
    spreadsheet_id = Variable.get("play55_gsheet_spreadsheet_id", default_var="<your-spreadsheet-id>")
    sheet_name = Variable.get("play55_gsheet_sheet", default_var="<your-sheet-name>")
    days_back = int(Variable.get("play55_qc_days_back", default_var="7"))

    print(f"{'=' * 60}")
    print("QUALITY CHECK: Google Sheet vs Snowflake")
    print(f"{'=' * 60}")
    print(f"Spreadsheet ID: {spreadsheet_id}")
    print(f"Sheet: {sheet_name}")
    print(f"Comparing records from last {days_back} days (same date range for both sources)")
    print(f"{'=' * 60}")

    # Connect to Google Sheets
    print("\nConnecting to Google Sheets...")
    gs_client = get_gsheet_client(gsheet_credentials)
    gsheet_counts = get_gsheet_record_counts(gs_client, spreadsheet_id, sheet_name, days_back=days_back)
    print(f"Retrieved counts for {len(gsheet_counts)} dates from Google Sheet (last {days_back} days)")

    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    sf_conn = snowflake_hook.get_conn()
    sf_cursor = sf_conn.cursor()

    try:
        snowflake_counts = get_snowflake_record_counts(sf_cursor, days_back)
        print(f"Retrieved counts for {len(snowflake_counts)} dates from Snowflake (last {days_back} days)")

        # Compare counts
        result = compare_counts(gsheet_counts, snowflake_counts)

        # Print formatted report
        report = format_quality_report(result)
        print(f"\n{report}")

        # Send results to Slack
        slack_webhook = Variable.get(
            "play55_slack_webhook_url",
            default_var=""
        )
        send_slack_notification(slack_webhook, result)

        return result

    finally:
        sf_cursor.close()
        sf_conn.close()


# ====== DAG Definition ======

with DAG(
    dag_id="play55_pipeline",
    default_args=default_args,
    description="Play55 API → MySQL → Snowflake → dbt pipeline (Airflow-native)",
    schedule_interval="0 12 * * *",  # 4:00 AM PST (12:00 PM UTC)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["play55", "etl", "aposta_premia"],
    doc_md="""
    ## Play55 Pipeline DAG (Airflow-Native)

    This DAG orchestrates the complete Play55 data pipeline using Airflow native operators and hooks.

    ### Pipeline Steps:
    1. **Extract & Load**: Fetch sales data from Play55 API → Insert into MySQL (`play55_vendas`)
    2. **Sync**: Trigger Airbyte sync (MySQL → Snowflake STG.APOSTA_PREMIA.PLAY55_VENDAS)
    3. **Transform**: Run dbt model (STG → RAW.APOSTA_PREMIA.PLAY55_VENDAS)

    ### Required Airflow Variables:
    | Variable | Description | Default |
    |----------|-------------|---------|
    | `play55_api_base_url` | Play55 API base URL | `<your-api-base-url>` |
    | `play55_api_auth_token` | Play55 API auth token (Basic) | (see code) |
    | `play55_airbyte_connection_id` | Airbyte connection UUID | (required) |
    | `play55_dbt_project_path` | Path to dbt project | `/opt/airflow/dags/dbt/play55` |
    | `play55_always_today` | Use today's date for API query | `true` |
    | `play55_lookback_days` | Days to look back (catches late-arriving data) | `3` |

    ### Required Airflow Connections:
    | Connection ID | Type | Description |
    |--------------|------|-------------|
    | `mysql_play55` | MySQL | MySQL database with `play55_vendas` table |
    | `airflow-call-to-airbyte` | HTTP | Airbyte API server |
    | `snowflake_conn` | Snowflake | Snowflake for dbt/Cosmos |

    ### Schedule
    Runs hourly to capture sales data.
    """,
) as dag:

    # ====== Task 0: Check Required Variables ======
    check_variables_task = PythonOperator(
        task_id="check_required_variables",
        python_callable=check_required_variables,
        doc_md="""
        Validates that all required Airflow variables are configured.

        **Required Variables:**
        - `play55_airbyte_connection_id`: Airbyte connection UUID

        **Optional Variables (with defaults):**
        - `play55_api_base_url`
        - `play55_api_auth_token`
        - `play55_dbt_project_path`
        - `play55_always_today`

        Fails the DAG if required variables are missing.
        """,
    )

    # ====== Task 1: Extract from API & Load to MySQL ======
    extract_load_task = PythonOperator(
        task_id="extract_and_load_to_mysql",
        python_callable=extract_and_load_to_mysql,
        doc_md="""
        Fetches sales data from Play55 API and inserts into MySQL.

        - Uses `MySqlHook` with connection `mysql_play55`
        - API credentials from Airflow Variables
        - Deduplicates using SHA256 hash
        - Returns stats via XCom
        """,
    )

    # ====== Task 2: Trigger Airbyte Sync ======
    with TaskGroup(group_id="airbyte_sync") as sync_task_group:
        sync_mysql_to_stg = AirbyteTriggerSyncOperator(
            task_id="sync_mysql_to_snowflake_stg",
            airbyte_conn_id="airflow-call-to-airbyte",
            connection_id="{{ var.value.play55_airbyte_connection_id }}",
            asynchronous=False,
            timeout=3600,  # 1 hour timeout
            wait_seconds=3,
            doc_md="""
            Triggers Airbyte sync job:
            - Source: MySQL `play55_vendas` table
            - Destination: Snowflake STG.APOSTA_PREMIA.PLAY55_VENDAS
            """,
        )

        sync_mysql_other_tables = AirbyteTriggerSyncOperator(
            task_id="sync_mysql_other_tables",
            airbyte_conn_id="airflow-call-to-airbyte",
            connection_id="91c88b01-5a2a-4a25-af71-46047a3206ec",
            asynchronous=False,
            timeout=3600,  # 1 hour timeout
            wait_seconds=3,
            doc_md="""
            Triggers Airbyte sync job:
            - Source: MySQL (other tables)
            - Destination: Snowflake
            """,
        )

    # ====== Task 3: Quality Check (Google Sheet vs Snowflake) ======
    quality_check_task = PythonOperator(
        task_id="quality_check_gsheet_vs_snowflake",
        python_callable=quality_check_gsheet_vs_snowflake,
        doc_md="""
        Compares record counts between Google Sheet and Snowflake after Airbyte sync.

        **Google Sheet:** `New-AP-Dashboard` / `FB/TT_Play55_endpoint`
        **Snowflake:** `STG.APOSTA_PREMIA.PLAY55_VENDAS`

        **Required Variables:**
        - `play55_gsheet_credentials`: Google service account JSON
        - `play55_gsheet_spreadsheet_id`: Google Spreadsheet ID (from URL)

        **Optional Variables:**
        - `play55_gsheet_sheet` (default: `FB/TT_Play55_endpoint`)
        - `play55_qc_days_back` (default: `7`)
        """,
    )

    # ====== Task 4: Transform with dbt using Cosmos ======
    transform_task = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path=Variable.get("play55_dbt_project_path", default_var=str(DBT_PROJECT_DIR)),
        ),
        profile_config=ProfileConfig(
            profile_name="play55",
            target_name="prod",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="snowflake_conn",
                profile_args={
                    "database": os.getenv("DBT_DATABASE", "RAW"),
                    "schema": os.getenv("DBT_SCHEMA", "APOSTA_PREMIA"),
                    "warehouse": "REPORTING",
                    "role": os.getenv("DBT_ROLE", "CDP_ROLE"),
                },
            ),
        ),
        render_config=RenderConfig(
            select=["play55_vendas"],  # Only run the play55_vendas model
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": False,
            "dbt_cmd_flags": ["--debug"],
            "env": {
                "DBT_PLUGINS_DISABLED": "true",  # Disable dbt plugin discovery to avoid import errors
            },
        },
    )

    # ====== Task Dependencies ======
    check_variables_task >> extract_load_task >> sync_task_group >> transform_task >> quality_check_task
