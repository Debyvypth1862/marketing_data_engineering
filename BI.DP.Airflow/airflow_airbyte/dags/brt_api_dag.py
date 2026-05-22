#!/usr/bin/env python3
"""
BRT API DBT Pipeline DAG
Pipeline: Run dbt transformations using Astronomer Cosmos

This DAG runs dbt models for the BRT API data using a single dbt run command.
dbt handles all parallelism internally with threads configuration.

Performance optimizations:
- Single dbt run command (minimal Airflow overhead)
- dbt threads for parallel Snowflake query execution
- Snowflake query tags for monitoring and cost attribution
- Fast DAG parsing (only 3 tasks)
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Astronomer Cosmos imports for dbt
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.operators.local import DbtRunLocalOperator
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import ExecutionMode, InvocationMode

from airbyte.slack_alerts import task_id_slack_failure_alert

# DBT project path
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/brt_api/brt_api"

# ====== Performance Configuration ======
# Number of parallel threads for dbt to run models concurrently in Snowflake
# Increase this for more parallelism (depends on warehouse size)
DBT_THREADS = int(os.getenv("DBT_THREADS", "16"))

# Common environment variables for dbt
DBT_ENV_VARS = {
    "DBT_PACKAGES_INSTALL_PATH": "/opt/airflow/dags/dbt/brt_api/dbt",
}

# ====== Default Args ======
default_args = {
    "owner": "airflow",
    "on_failure_callback": task_id_slack_failure_alert,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_profile_config():
    """Create Snowflake profile config with performance optimizations."""
    return ProfileConfig(
        profile_name="brt_api",
        target_name="prod",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_conn",
            profile_args={
                "database": os.getenv("DBT_DATABASE", "EXP"),
                "schema": os.getenv("DBT_SCHEMA", "BRT"),
                "warehouse": os.getenv("DBT_WAREHOUSE", "CDP_PROD"),
                "role": os.getenv("DBT_ROLE", "CDP_ROLE"),
                "threads": DBT_THREADS,  # Parallel query execution in Snowflake
                "query_tag": "dbt_brt_api",  # For Snowflake query monitoring
            },
        ),
    )


def get_execution_config():
    """Create execution config."""
    return ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        invocation_mode=InvocationMode.SUBPROCESS,
    )


# ====== DAG Definition ======
with DAG(
    dag_id="brt_api_dag",
    default_args=default_args,
    description="BRT API dbt transformation pipeline using single dbt run",
    schedule_interval="0 11 * * *",  # 3:00 AM PST (11:00 AM UTC)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["brt_api", "dbt", "cosmos"],
    doc_md="""
## BRT API DBT Pipeline DAG (Single dbt run)

This DAG runs dbt transformations for BRT API data using a single `dbt run` command.
dbt handles all model dependencies and parallelism internally.

### Model Structure (120 models total):
- **60 BRT_OFFER_*** models (15 entities × 4 layers: ephemeral → view → scd → final)
- **60 BRT_PARENT_*** models (15 entities × 4 layers: ephemeral → view → scd → final)

### Pipeline Architecture:
```
start ──▶ dbt_run_all ──▶ end
```

dbt internally executes with threads=8, running up to 8 models in parallel
while respecting all model dependencies (ref relationships).

### Required Airflow Connections:
| Connection ID | Type | Description |
|--------------|------|-------------|
| `snowflake_conn` | Snowflake | Snowflake connection for Cosmos/dbt |

### Performance Optimizations:
- **Single task**: Minimal Airflow scheduling overhead
- **dbt threads=8**: Up to 8 models run in parallel within Snowflake
- **Internal dependency management**: dbt handles AB1→STG→SCD→Final chains
- **Query tagging**: Track queries in Snowflake with `dbt_brt_api` tag

### Schedule
Runs daily at 11:00 AM UTC (3:00 AM PST).
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    # ====== Single dbt run Task ======
    # Runs all models in the project with dbt handling parallelism internally
    # threads=16 means up to 16 models execute concurrently in Snowflake
    dbt_run_all = DbtRunLocalOperator(
        task_id="dbt_run_all",
        project_dir=DBT_PROJECT_DIR,
        profile_config=get_profile_config(),
        install_deps=False,  # Use pre-installed packages
        env=DBT_ENV_VARS,
    )

    # ====== Task Dependencies ======
    start >> dbt_run_all >> end
