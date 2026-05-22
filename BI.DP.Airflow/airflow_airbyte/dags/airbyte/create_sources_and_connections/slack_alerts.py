import time
import sys
import logging
import os

from airflow.models.connection import Connection
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException, AirflowException
sys.path.insert(1,"dags/airbyte")
from db_connection import mysql_conn

airbyte_host = os.getenv("airbyte_opcentre_domain")
airflow_host = os.getenv("airflow_opcentre_domain")
workspace_id = os.getenv("workspace_id")
slack_notification = os.getenv("slack_notification")
app_base_url = os.getenv("app_base_url")
logger = logging.getLogger(__name__)


def get_slack_flags():
    """
        Get slack flags from SLACK_NOTIFICATION table
    """
    connection = mysql_conn()
    cursor = connection.cursor()
    try:
        cursor.execute(f"""
            SELECT *
            FROM {slack_notification};
        """)
        result_set = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
    except Exception as e:
        print(e)
        return {}
    else:
        cat_idx = column_names.index("category")
        is_enabled_idx = column_names.index("is_enabled")

        flags = {}
        for row in result_set:
            name, value = row[cat_idx], row[is_enabled_idx]
            flags[name] = value
        return flags
    finally:
        cursor.close()
        connection.close()


flags = get_slack_flags()


def get_operator_id(job_id):
    """
        Get operator_id for a job_id from JOB table
    """
    try:
        connection = mysql_conn()
    except Exception as e:
        print(e)
    else:
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT operator_id
            FROM JOB
            WHERE job_id = {job_id};
        """)
        result = cursor.fetchall()
        try:
            return result[0][0]
        except:
            # Fetching operator_id when a job is cancelled manually results in an IndexError
            return None
    finally:
        connection.close()


def airflow_airbyte_sync_task_slack_alert(response):
    """
        Send Airbyte sync slack alerts
    """
    if flags.get("operator", None) == 1:
        context = get_current_context()
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        airflow_logs_url = context.get('task_instance').log_url
        exec_dt = str(context.get('logical_date'))

        job_id = response['job']['id']
        conn_id = response['job']['configId']
        status = response['job']['status']

        slack_msg = (
            f'*`Task Name`* {task_id}\n'
            f'*`DAG ID`* {dag_id}\n'
            f'*`Airflow Start DateTime`* {exec_dt}\n'
            f'*`Airbyte Connection ID`* {conn_id}\n'
            f'*`Airbyte Job ID`* {job_id}\n'
            f'*`Airbyte Job Status`* {status}'
        )

        if response['job']['status'] == 'succeeded':
            emoji = 'heavy_check_mark'
            title = 'Airbyte Sync Succeeded.'
            color = '#00FF00'
        elif response['job']['status'] == 'failed':
            emoji = 'x'
            title = 'Airbyte Sync Failed.'
            color = '#FF0000'
        else:
            emoji = 'heavy_exclamation_mark'
            title = 'Airbyte Sync Incomplete.'
            color = '#FFFF00'

        attachments = [{
            'mrkdwn_in': ['text', 'pretext'],
            'color': color,
            'pretext': f':{emoji}: *{title}*',
            'text': slack_msg,
            'actions': [
                {
                    'type': 'button',
                    'name': 'view log',
                    'text': 'Airbyte Logs',
                    'url': f'{airbyte_host}workspaces/{workspace_id}/connections/{conn_id}/job-history',
                    'style': 'primary'
                }, {
                    'type': 'button',
                    'name': 'view log',
                    'text': 'Airflow Logs',
                    'url': f'{airflow_host}{str(airflow_logs_url).split("/")[-1]}',
                    'style': 'primary'
                }
            ],
            'fallback': title
        }]

        operator_id = get_operator_id(job_id)
        # Add web log URL button
        if response['job']['status'] == 'failed' and operator_id is not None:
            attachments[0]['actions'].append({
                'type': 'button',
                'name': 'view log',
                'text': 'Job Fail Logs',
                'url': f"{app_base_url}logs?type=jobfail&jobid={job_id}&operatorId={operator_id}",
                'style': 'primary'
            })

        slack_alert = SlackWebhookOperator(
            task_id='airflow_airbyte_sync_task_slack_alert',
            slack_webhook_conn_id='cryptoback_slack_alert' if 'Cryptoback' in dag_id else 'slack_webhook',
            message='',
            attachments=attachments
        )
        logger.info("Sending Airbyte sync slack alert")
        slack_alert.execute(context=context)
    else:
        logger.info("Enable `operator` flag in `slack_notification` table to enable Airbyte sync slack alerts")


def airflow_trigger_airbyte_sync_task_error_slack_alert(response):
    """
        Send Airbyte sync error slack alerts
    """
    if flags.get("operator", None) == 1:
        context = get_current_context()
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        exec_dt = str(context.get('logical_date'))
        airflow_logs_url = context.get('task_instance').log_url

        conn_id = response['config_id']
        error_msg = response['error_msg']

        slack_msg = (
            f'\n*`Task Name`* {task_id}'
            f'\n*`Dag ID`* {dag_id}'
            f'\n*`Airflow Start DateTime`* {exec_dt}'
            f'\n*`Airbyte Connection ID`* {conn_id}'
            f'\n*`Airflow logs`*'
            f'\n```{error_msg}```'
        )

        emoji = 'x'
        title = 'Airbyte Sync Error'

        workspace_id = os.getenv("workspace")
        attachments = [{
            'mrkdwn_in': ['text', 'pretext'],
            'color': '#FF0000',
            'pretext': f':{emoji}: *{title}*',
            'text': slack_msg,
            'actions': [
                {
                    'type': 'button',
                    'name': 'view log',
                    'text': 'Airbyte Logs',
                    'url': f'{airbyte_host}workspaces/{workspace_id}/connections/{conn_id}',
                    'style': 'primary'
                },
                {
                    'type': 'button',
                    'name': 'view log',
                    'text': 'Airflow Logs',
                    'url': f'{airflow_host}{str(airflow_logs_url).split("/")[-1]}',
                    'style': 'primary'
                }
            ],
            'fallback': title
        }]

        slack_alert = SlackWebhookOperator(
            task_id='airflow_airbyte_sync_task_error_slack_alert',
            slack_webhook_conn_id='cryptoback_slack_alert' if 'Cryptoback' in dag_id else 'slack_webhook',
            message='',
            attachments=attachments
        )
        logger.info("Sending Airbyte sync error slack alert")
        slack_alert.execute(context=context)
    else:
        logger.info("Enable `operator` flag in `slack_notification` table to enable Airbyte sync slack alerts")


def task_id_slack_success_alert(context):
    """
        Send slack alert if a task succeeds
    """
    if flags.get("jobs", None) == 1:
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        exec_dt = str(context.get('logical_date'))
        airflow_logs_url = context.get('task_instance').log_url

        slack_msg = (
            f'*`Task Name`* {task_id}'
            f'\n*`Dag ID`* {dag_id}'
            f'\n*`Task Start Datetime`* {exec_dt}'
        )

        emoji = 'tada'
        title = 'Airflow Task Succeeded'

        attachments = [{
            'mrkdwn_in': ['text', 'pretext'],
            'pretext': f":{emoji}: *Airflow Task Succeeded.*",
            'text': slack_msg,
            'actions': [
                {
                    'type': 'button',
                    'name': 'view log',
                    'text': 'Airflow Logs',
                    'url': f'{airflow_host}{str(airflow_logs_url).split("/")[-1]}',
                    'style': 'primary',
                }
            ],
            'color': '#FF0000' ,
            'fallback': title
        }]

        slack_alert = SlackWebhookOperator(
            task_id='airflow_airbyte_sync_task_slack_alert',
            slack_webhook_conn_id='cryptoback_slack_alert' if 'Cryptoback' in dag_id else 'slack_webhook',
            message='',
            attachments=attachments
        )

        slack_alert.execute(context=context)
    else:
        pass


def task_id_slack_failure_alert(context):
    """
        Send slack alert if a task fails
    """
    if flags.get("jobs", None) == 1:
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        airflow_logs_url = context.get('task_instance').log_url
        exec_dt = str(context.get('logical_date'))

        slack_msg = (
            f'*`Task Name`* {task_id}\n'
            f'*`DAG ID`* {dag_id}\n'
            f'*`Task Start Datetime`* {exec_dt}\n'
        )

        emoji = 'x'
        title = 'Airflow Task Failed'

        attachments = [{
            'mrkdwn_in': ['text', 'pretext'],
            'color': '#FF0000',
            'pretext': f':{emoji}: *{title}*',
            'text': slack_msg,
            'actions': [
                {
                    'type': 'button',
                    'name': 'view log',
                    'text': 'Airflow Logs',
                    'url': f'{airflow_host}{str(airflow_logs_url).split("/")[-1]}',
                    'style': 'primary',
                }
            ],
            'fallback': title
        }]

        slack_alert = SlackWebhookOperator(
            task_id='airflow_airbyte_sync_task_slack_alert',
            slack_webhook_conn_id='cryptoback_slack_alert' if 'Cryptoback' in dag_id else 'slack_webhook',
            message='',
            attachments=attachments
        )
        logger.info("Sending Airflow task fail slack alert")
        slack_alert.execute(context=context)
    else:
        pass


def dq_fail_slack_alert(context, operator_id, dq_type, job_id, txn_dts):
    """
        Send slack alerts if DQ fails
    """
    if dq_type == "ZERO_RECORD_COUNT":
        dq_flag = "dq_zero"
    elif dq_type == "HIGH_RECORD_COUNT":
        dq_flag = "dq_high"
    else:
        dq_flag = "dq_low"

    if flags.get(dq_flag, None) == 1:
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        exec_dt = str(context.get('logical_date'))

        slack_msg = (
            f'*`Operator ID`* {operator_id}\n'
            f'*`Task ID`* {task_id}\n'
            f'*`DAG ID`* {dag_id}\n'
            f'*`Task Start Datetime`* {exec_dt}\n'
        )

        emoji = 'x'
        title = f'DQ Failed: {operator_id} - {dq_type}'

        attachments = [{
            'mrkdwn_in': ['text', 'pretext'],
            'color': '#FF0000',
            'pretext': f':{emoji}: *{title}*',
            'text': slack_msg,
            'fields': [
                {
                        'title': "Transaction Dates",
                        'value': txn_dts,
                        'short': False
                }
            ],
            'actions': [
                {
                    'type': 'button',
                    'name': 'view log',
                    'text': 'DQ Logs',
                    'url': f"{app_base_url}logs?type=dqfail&jobid={job_id}&operatorId={operator_id}",
                    'style': 'primary',
                }
            ],
            'fallback': title
        }]

        slack_alert = SlackWebhookOperator(
            task_id='OpCentre_DQ',
            slack_webhook_conn_id='cryptoback_slack_alert' if 'Cryptoback' in dag_id else 'slack_webhook',
            message='',
            attachments=attachments
        )

        try:
            slack_alert.execute(context=context)
        except AirflowException:
            t = 5
            logger.info(f"Slack API rate limit reached: waiting for {t} seconds")
            time.sleep(t)
            slack_alert.execute(context=context)

    else:
        logger.info(f"Enable `{dq_flag}` flag in `slack_notification` table to enable DQ slack alerts")


def operator_validation_fail_slack_alert(context, platform, operator_id, error_msg):
    """
        Send slack alert if an operator's validation is failed
    """
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    logger.info("-"* 50)
    logger.info(context.get('logical_date'))
    exec_dt = str(context.get('logical_date'))

    slack_msg = (
        f'*`Platform`* {platform}\n'
        f'*`Operator ID`* {operator_id}\n'
        f'*`Task ID`* {task_id}\n'
        f'*`DAG ID`* {dag_id}\n'
        f'*`Task Start Datetime`* {exec_dt}\n'
        f'*`Error`* {error_msg}'
    )

    emoji = 'x'
    title = f'Operator Validation Failed'

    attachments = [{
        'mrkdwn_in': ['text', 'pretext'],
        'color': '#FF0000',
        'pretext': f':{emoji}: *{title}*',
        'text': slack_msg,
        'actions': [
            {
                'type': 'button',
                'name': 'view log',
                'text': 'Operator Validation Fail Logs',
                'url': f"{app_base_url}logs?type=opValidationFail&operatorId={operator_id}",
                'style': 'primary',
            }
        ],
        'fallback': title
    }]

    slack_alert = SlackWebhookOperator(
        task_id='Add_modify_delete_Airbyte_sources_and_connections',
        slack_webhook_conn_id='cryptoback_slack_alert' if 'Cryptoback' in dag_id else 'slack_webhook',
        message='',
        attachments=attachments
    )
    logger.info(f"Sending Operator validation fail slack alert- platform: {platform}, operator_id: {operator_id}")
    try:
        slack_alert.execute(context=context)
    except AirflowException:
        t = 5
        logger.info(f"Slack API rate limit reached: waiting for {t} seconds")
        time.sleep(t)
        slack_alert.execute(context=context)


def pk_schema_validation_fail_slack_alert(context, operator_id, job_id, validation_type, txn_dts):
    """
        Slack alerts for Primary key, schema validation failure
    """
    if validation_type == "PRIMARY_KEY_VALIDATION":
        val_flag = "pk_val"
    elif validation_type == "SCHEMA_VALIDATION":
        val_flag = "schema_val"

    if flags.get(val_flag, None) == 1:
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        exec_dt = str(context.get('logical_date'))

        slack_msg = (
            f'*`Operator ID`* {operator_id}\n'
            f'*`Task ID`* {task_id}\n'
            f'*`DAG ID`* {dag_id}\n'
            f'*`Task Start Datetime`* {exec_dt}\n'
        )

        emoji = 'x'
        title = f'Validation failed: {validation_type} - {operator_id}'

        attachments = [{
            'mrkdwn_in': ['text', 'pretext'],
            'color': '#FF0000',
            'pretext': f':{emoji}: *{title}*',
            'text': slack_msg,
            'fields': [
                {
                    'title': "Transaction Dates",
                    'value': txn_dts,
                    'short': False
                }
            ],
            'fallback': title
        }]

        slack_alert = SlackWebhookOperator(
            task_id='OpCentre_DQ',
            slack_webhook_conn_id='cryptoback_slack_alert' if 'Cryptoback' in dag_id else 'slack_webhook',
            message='',
            attachments=attachments
        )

        try:
            slack_alert.execute(context=context)
        except AirflowException:
            t = 5
            logger.info(f"Slack API rate limit reached: waiting for {t} seconds")
            time.sleep(t)
            slack_alert.execute(context=context)

    else:
        logger.info(f"Enable `{val_flag}` flag in `slack_notification` table to enable slack alerts")


def consoliated_dq_fail_slack_alert(context, operator_id, job_id, message):
    """
        Slack alerts for DQ failure (consolidated)
    """
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    exec_dt = str(context.get('logical_date'))

    slack_msg = (
        f'*`Operator ID`* {operator_id}\n'
        f'*`Task ID`* {task_id}\n'
        f'*`DAG ID`* {dag_id}\n'
        f'*`Task Start Datetime`* {exec_dt}\n'
    )

    emoji = 'x'
    title = f'DQ Failed: {operator_id}'

    attachments = [{
        'mrkdwn_in': ['text', 'pretext'],
        'color': '#FF0000',
        'pretext': f':{emoji}: *{title}*',
        'text': slack_msg,
        'fields': [
            {
                    'title': "Message",
                    'value': message,
                    'short': False
            }
        ],
        'actions': [
            {
                'type': 'button',
                'name': 'view log',
                'text': 'DQ Logs',
                'url': f"{app_base_url}logs?type=dqfail&jobid={job_id}&operatorId={operator_id}",
                'style': 'primary',
            }
        ],
        'fallback': title
    }]

    slack_alert = SlackWebhookOperator(
        task_id='OpCentre_DQ',
        slack_webhook_conn_id='cryptoback_slack_alert' if 'Cryptoback' in dag_id else 'slack_webhook',
        message='',
        attachments=attachments
    )

    try:
        slack_alert.execute(context=context)
    except AirflowException:
        t = 5
        logger.info(f"Slack API rate limit reached: waiting for {t} seconds")
        time.sleep(t)
        slack_alert.execute(context=context)