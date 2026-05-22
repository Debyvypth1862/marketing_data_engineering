import os
import sys
import logging
import json
from datetime import datetime
import boto3
import time

from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator

sys.path.insert(1, "dags/airbyte/create_sources_and_connections")
from mexos import Mexos
from cellxpert import Cellxpert
from alanbase import Alanbase
from sweep import Sweep
from voluum import Voluum
from buffalopartner import BuffaloPartner
from ego import Ego
from income_access import IncomeAccess
from myaffiliates import MyAffiliates
from netrefer import NetRefer
from referon import Referon
from q import SourceQ
from smartico import Smartico
from softswiss import Softswiss
from google_analytics4 import GoogleAnalytics4
from airbytesources import AirbyteSources
from airbyte import constants
from utils import Utils
from slack_alerts import (
    airflow_airbyte_sync_task_slack_alert,
    airflow_trigger_airbyte_sync_task_error_slack_alert
)

task_logger = logging.getLogger("airflow.task")
status_ = None
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key")
)


def delete_all_files_in_folder(bucket_name, path):
    try:
        # List all objects in the specified folder
        bucket_path = os.getenv('Input_bucket_path')
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_path + path)

        # Check if there are any objects to delete
        if "Contents" in response:
            # Create a list of keys to delete
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]

            # Batch delete all objects
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects_to_delete})
            task_logger.info(f"Successfully deleted all files in folder '{path}'")
        else:
            task_logger.info(f"No files found in folder '{path}' to delete.")
    except Exception as e:
        task_logger.info(f"Failed to delete files in folder '{path}': {e}")


def rename_object(source_bucket, source_key, destination_bucket, destination_key):
    try:
        s3_client.copy_object(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={
                'Bucket': source_bucket,
                'Key': source_key
            }
        )
        task_logger.info(
            f"Successfully renamed {source_key} from {source_bucket} to {destination_key} in {destination_bucket}"
        )

        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        task_logger.info(f"Successfully deleted {source_key} from {source_bucket}")

    except Exception as e:
        task_logger.info(e)


def update_recovery_sources(ti, source_info, operator_id):
    try:
        airbyte_workspace_id = os.getenv('workspace_id')
        start_date = source_info["start_date"]
        end_date = Variable.get('end_date')

        # Dictionary to store validation results
        source_instance = None
        operator_id = source_info['operator_id']

        if source_info["platform_name"] == constants.Cellxpert:
            endpoint = source_info["endpoint"]
            platform = constants.Cellxpert
            adve_name = source_info["name"]
            tlog_id = str(source_info["operator_id"])
            username = source_info["username"]
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            tlog_password = source_info["password"]
            loopback_days = source_info['loopback_days']
            tlog_username = source_info["username"]
            dates = source_info["recovery_dates"]
            parts = dates.split(', "')

            # Initialize variables to store the streams
            dynamic_variables_report_stream = []
            registration_report_stream = []
            ftd_registration_report_stream =[]

            # Loop through the parts and extract dates for each stream
            for part in parts:
                if "dynamic_variables_report_stream" in part:
                    dynamic_variables_report_stream.append(part.split(":")[1])
                elif "registration_report_stream" in part:
                    registration_report_stream.append(part.split(":")[1])
                elif "ftd_registration_report_stream" in part:
                    ftd_registration_report_stream.append(part.split(":")[1])

            non_empty_streams_exist = (
                dynamic_variables_report_stream
                or registration_report_stream or ftd_registration_report_stream
            )

            current_date = datetime.now().strftime('%Y-%m-%d')

            # Replace empty lists with the current date only if there is at least one non-empty stream
            if non_empty_streams_exist:
                if not dynamic_variables_report_stream:
                    dynamic_variables_report_stream.append(current_date)
                if not registration_report_stream:
                    registration_report_stream.append(current_date)
                if not ftd_registration_report_stream:
                    ftd_registration_report_stream.append(current_date)

            dynamic_output = "{}".format(','.join(dynamic_variables_report_stream))
            registration_output = "{}".format(','.join(registration_report_stream))
            ftd_registration_output = "{}".format(','.join(ftd_registration_report_stream))

            task_logger.info(dynamic_output)
            task_logger.info(registration_output)
            task_logger.info(ftd_registration_output)
            # Create the source instance
            source_instance = Cellxpert(
                endpoint, start_date, tlog_password, tlog_username, tlog_id,
                loopback_days, dynamic_output, registration_output, True
            )
        elif source_info["platform_name"] == constants.Alanbase:
            endpoint = source_info["endpoint"]
            platform = constants.Alanbase
            adve_name = source_info["name"]
            tlog_id = str(source_info["operator_id"])
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            api_key = source_info["api_key"]
            loopback_days = source_info['loopback_days']
            dates = source_info["recovery_dates"]
            parts = dates.split(', "')

            # Initialize variables to store the streams
            common_statistic = []
            conversions = []

            # Loop through the parts and extract dates for each stream
            for part in parts:
                if "common_statistic" in part:
                    common_statistic.append(part.split(":")[1])
                elif "conversions" in part:
                    conversions.append(part.split(":")[1])
                

            non_empty_streams_exist = (
                common_statistic
                or conversions 
            )

            current_date = datetime.now().strftime('%Y-%m-%d')

            # Replace empty lists with the current date only if there is at least one non-empty stream
            if non_empty_streams_exist:
                if not common_statistic:
                    common_statistic.append(current_date)
                if not conversions:
                    conversions.append(current_date)

            common_statistic_output = "{}".format(','.join(common_statistic))
            conversions_output = "{}".format(','.join(conversions))

            task_logger.info(common_statistic_output)
            task_logger.info(conversions_output)
            # Create the source instance
            source_instance = Alanbase(
                endpoint, start_date, api_key, tlog_id,
                loopback_days, common_statistic_output, conversions_output, True
            )
        elif source_info["platform_name"] == constants.Sweep:
            endpoint = source_info["endpoint"]
            platform = constants.Sweep
            adve_name = source_info["name"]
            tlog_id = str(source_info["operator_id"])
            username = source_info["username"]
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            tlog_password = source_info["password"]
            loopback_days = source_info['loopback_days']
            tlog_username = source_info["username"]
            dates = source_info["recovery_dates"]
            parts = dates.split(', "')

            # Initialize variables to store the streams
            dynamic_variables_report_stream = []
            registration_report_stream = []
            ftd_registration_report_stream = []

            # Loop through the parts and extract dates for each stream
            for part in parts:
                if "dynamic_variables_report_stream" in part:
                    dynamic_variables_report_stream.append(part.split(":")[1])
                elif "registration_report_stream" in part:
                    registration_report_stream.append(part.split(":")[1])
                elif "ftd_registration_report_stream" in part:
                    ftd_registration_report_stream.append(part.split(":")[1])

            non_empty_streams_exist = (
                dynamic_variables_report_stream
                or registration_report_stream
                or ftd_registration_report_stream
            )

            current_date = datetime.now().strftime('%Y-%m-%d')

            # Replace empty lists with the current date only if there is at least one non-empty stream
            if non_empty_streams_exist:
                if not dynamic_variables_report_stream:
                    dynamic_variables_report_stream.append(current_date)
                if not registration_report_stream:
                    registration_report_stream.append(current_date)
                if not ftd_registration_report_stream:
                    ftd_registration_report_stream.append(current_date)

            dynamic_output = "{}".format(','.join(dynamic_variables_report_stream))
            registration_output = "{}".format(','.join(registration_report_stream))
            ftd_registration_output = "{}".format(','.join(ftd_registration_report_stream))

            task_logger.info(dynamic_output)
            task_logger.info(registration_output)
            task_logger.info(ftd_registration_output)

            # Create the source instance
            source_instance = Sweep(
                endpoint, start_date, tlog_password, tlog_username, tlog_id,
                loopback_days, dynamic_output, registration_output, ftd_registration_output, True
            )

        elif source_info["platform_name"] == constants.Voluum:
            loopback_days = source_info['loopback_days']
            platform = constants.Voluum
            adve_name = source_info["name"]
            tlog_id = str(source_info["operator_id"])
            username = source_info["username"]
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            dates = source_info["recovery_dates"]
            parts = dates.split(', "')
            affiliate_network_report_stream = []
            campaign_report_stream = []
            conversions_stream = []
            flow_report_stream = []
            lander_report_stream = []
            offer_report_stream = []
            traffic_source_report_stream = []

            # Loop through the parts and extract dates for each stream
            for part in parts:
                if "affiliate_network_report" in part:
                    affiliate_network_report_stream.append(part.split(":")[1])
                elif "campaign_report" in part:
                    campaign_report_stream.append(part.split(":")[1])
                elif "conversions" in part:
                    conversions_stream.append(part.split(":")[1])
                elif "flow_report" in part:
                    flow_report_stream.append(part.split(":")[1])
                elif "lander_report" in part:
                    lander_report_stream.append(part.split(":")[1])
                elif "offer_report" in part:
                    offer_report_stream.append(part.split(":")[1])
                elif "traffic_source_report" in part:
                    traffic_source_report_stream.append(part.split(":")[1])

            # Check if there is at least one non-empty stream
            non_empty_streams_exist = (
                affiliate_network_report_stream
                or campaign_report_stream
                or conversions_stream
                or flow_report_stream
                or lander_report_stream
                or offer_report_stream
                or traffic_source_report_stream
            )

            current_date = datetime.now().strftime('%Y-%m-%d')

            # Replace empty lists with the current date only if there is at least one non-empty stream
            if non_empty_streams_exist:
                if not affiliate_network_report_stream:
                    affiliate_network_report_stream.append(current_date)
                if not campaign_report_stream:
                    campaign_report_stream.append(current_date)
                if not conversions_stream:
                    conversions_stream.append(current_date)
                if not flow_report_stream:
                    flow_report_stream.append(current_date)
                if not lander_report_stream:
                    lander_report_stream.append(current_date)
                if not offer_report_stream:
                    offer_report_stream.append(current_date)
                if not traffic_source_report_stream:
                    traffic_source_report_stream.append(current_date)

            affiliate_network_report_output = "{}".format(','.join(affiliate_network_report_stream))
            task_logger.info(f'affiliate_network_report_output={affiliate_network_report_output}')
            campaign_report_output = "{}".format(','.join(campaign_report_stream))
            task_logger.info(f'campaign_report_output={campaign_report_output}')
            conversions_output = "{}".format(','.join(conversions_stream))
            task_logger.info(f'conversions_output={conversions_output}')
            flow_report_output = "{}".format(','.join(flow_report_stream))
            task_logger.info(f'flow_report_output={flow_report_output}')
            lander_report_output = "{}".format(','.join(lander_report_stream))
            task_logger.info(f'lander_report_output={lander_report_output}')
            offer_report_output = "{}".format(','.join(offer_report_stream))
            task_logger.info(f'offer_report_output={offer_report_output}')
            traffic_source_report_output = "{}".format(','.join(traffic_source_report_stream))
            task_logger.info(f'traffic_source_report_output={traffic_source_report_output}')

            source_instance = Voluum(
                start_date, loopback_days, affiliate_network_report_output,
                campaign_report_output, conversions_output, flow_report_output,
                lander_report_output, offer_report_output, traffic_source_report_output, True
            )

        elif source_info["platform_name"] == constants.Google_Analytics:
            platform = constants.Google_Analytics
            adve_name = source_info["name"]
            property_id = str(source_info["operator_id"])
            username = source_info["username"]
            reprocess_name = f"Reprocess-{adve_name}-{property_id}"
            dates = source_info["recovery_dates"]
            parts = dates.split(', "')

            # Initialize variables to store the streams
            pages_recovery_dates = []
            devices_recovery_dates = []
            locations_recovery_dates = []
            traffic_sources_recovery_dates = []
            website_overview_recovery_dates = []
            daily_active_users_recovery_dates = []
            weekly_active_users_recovery_dates = []
            four_weekly_active_users_recovery_dates = []

            # Loop through the parts and extract dates for each stream
            for part in parts:
                if "pages" in part:
                    pages_recovery_dates.append(part.split(":")[1])
                elif "devices" in part:
                    devices_recovery_dates.append(part.split(":")[1])
                elif "locations" in part:
                    locations_recovery_dates.append(part.split(":")[1])
                elif "traffic_sources" in part:
                    traffic_sources_recovery_dates.append(part.split(":")[1])
                elif "website_overview" in part:
                    website_overview_recovery_dates.append(part.split(":")[1])
                elif "daily_active_users" in part:
                    daily_active_users_recovery_dates.append(part.split(":")[1])
                elif "weekly_active_users" in part:
                    weekly_active_users_recovery_dates.append(part.split(":")[1])
                elif "four_weekly_active_users" in part:
                    four_weekly_active_users_recovery_dates.append(part.split(":")[1])


            # Check if there is at least one non-empty stream
            non_empty_streams_exist = (
                pages_recovery_dates
                or devices_recovery_dates
                or locations_recovery_dates
                or traffic_sources_recovery_dates
                or website_overview_recovery_dates
                or daily_active_users_recovery_dates
                or weekly_active_users_recovery_dates
                or four_weekly_active_users_recovery_dates
            )

            current_date = datetime.now().strftime('%Y-%m-%d')

            # Replace empty lists with the current date only if there is at least one non-empty stream
            if non_empty_streams_exist:
                if not pages_recovery_dates:
                    pages_recovery_dates.append(current_date)
                if not devices_recovery_dates:
                    devices_recovery_dates.append(current_date)
                if not locations_recovery_dates:
                    locations_recovery_dates.append(current_date)
                if not traffic_sources_recovery_dates:
                    traffic_sources_recovery_dates.append(current_date)
                if not website_overview_recovery_dates:
                    website_overview_recovery_dates.append(current_date)
                if not daily_active_users_recovery_dates:
                    daily_active_users_recovery_dates.append(current_date)
                if not weekly_active_users_recovery_dates:
                    weekly_active_users_recovery_dates.append(current_date)
                if not four_weekly_active_users_recovery_dates:
                    four_weekly_active_users_recovery_dates.append(current_date)

            pages_recovery_dates_output = "{}".format(','.join(pages_recovery_dates))
            devices_recovery_dates_output = "{}".format(','.join(devices_recovery_dates))
            locations_recovery_dates_output = "{}".format(','.join(locations_recovery_dates))
            traffic_sources_recovery_dates_output = "{}".format(','.join(traffic_sources_recovery_dates))
            website_overview_recovery_dates_output = "{}".format(','.join(website_overview_recovery_dates))
            daily_active_users_recovery_dates_output = "{}".format(','.join(daily_active_users_recovery_dates))
            weekly_active_users_recovery_dates_output = "{}".format(','.join(weekly_active_users_recovery_dates))
            four_weekly_active_users_recovery_dates_output = "{}".format(','.join(four_weekly_active_users_recovery_dates))

            source_instance = GoogleAnalytics4(
                property_id, start_date, True, pages_recovery_dates_output,
                devices_recovery_dates_output, locations_recovery_dates_output,
                traffic_sources_recovery_dates_output, website_overview_recovery_dates_output,
                daily_active_users_recovery_dates_output, weekly_active_users_recovery_dates_output,
                four_weekly_active_users_recovery_dates_output
            )

        elif source_info["platform_name"] == constants.NetRefer:
            platform = constants.NetRefer
            adve_name = source_info["name"]
            tlog_id = str(source_info["operator_id"])
            username = source_info["username"]
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            tlog_username = source_info["username"]
            tlog_password = source_info["password"]
            loopback_days = source_info['loopback_days']
            endpoint = source_info["endpoint"]
            dates = source_info["recovery_dates"].split(":")[1]

            api_key_str = source_info['api_key']
            context = get_current_context()

            if api_key_str:
                try:
                    api_key_str = api_key_str.strip()
                    api_key_json = json.loads(api_key_str)
                    api_key = api_key_json.get('api_key',None)
                    if not api_key:
                        raise ValueError("XML key is missing in the API key JSON")

                    source_instance = NetRefer(
                        api_key, endpoint, start_date, tlog_id, loopback_days, dates, True
                    )
                    source_class = NetRefer

                    if source_instance is not None:
                        payload = source_instance.create_source_payload(
                            name=name, airbyte_workspace_id=airbyte_workspace_id
                        )
                    else:
                        logging.info("Failed to create source_instance.")
                except json.JSONDecodeError:
                    logging.error("Error: Failed to decode API key JSON.")
                except ValueError as e:
                    logging.error(str(e))
            else:
                logging.error(f"Api Key Missing for operator_id {tlog_id}")

        elif source_info["platform_name"] == constants.Referon:
            platform = constants.Referon
            adve_name = source_info["name"]
            tlog_id = str(source_info["operator_id"])
            username = source_info["username"]
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            tlog_username = source_info["username"]
            tlog_password = source_info["password"]
            loopback_days = source_info['loopback_days']
            endpoint = source_info["endpoint"]
            dates = source_info["recovery_dates"].split(":")[1]

            api_key_str = source_info['api_key']
            context = get_current_context()

            if api_key_str:
                try:
                    api_key_str = api_key_str.strip()
                    api_key_json = json.loads(api_key_str)
                    token = api_key_json.get('token',None)
                    export_id = api_key_json.get('id',None)
                    if not token:
                        raise ValueError("token is missing in the API key JSON")

                    if not export_id:
                        raise ValueError("token is missing in the API key JSON")
                    
                    source_instance = Referon(
                        api_key_str, endpoint, start_date, tlog_id, loopback_days, dates, True
                    )
                    source_class = Referon

                    if source_instance is not None:
                        payload = source_instance.create_source_payload(
                            name=name, airbyte_workspace_id=airbyte_workspace_id
                        )
                    else:
                        logging.info("Failed to create source_instance.")
                except json.JSONDecodeError:
                    logging.error("Error: Failed to decode API key JSON.")
                except ValueError as e:
                    logging.error(str(e))
            else:
                logging.error(f"Api Key Missing for operator_id {tlog_id}")


        elif source_info['platform_name'] == constants.MyAffiliates:
            logging.info(f"operator_id----->{operator_id}")
            platform = constants.MyAffiliates
            adve_name = source_info['name']
            tlog_id = str(source_info["operator_id"])
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            endpoint = source_info['endpoint']
            # start_date = source_info["start_date"]
            loopback_days = source_info['loopback_days']
            dates = source_info["recovery_dates"].split(":")[1]

            context = get_current_context()
            api_key = source_info['api_key']
            source_class = MyAffiliates

            if api_key is not None and api_key != '':
                try:
                    api_key_json = json.loads(api_key)
                    client_identifier = api_key_json.get("client_id")
                    client_secret = api_key_json.get("client_secret")

                    if client_identifier and client_secret:
                        source_instance = MyAffiliates(
                            endpoint, client_identifier, start_date, client_secret,
                            tlog_id, loopback_days, dates, True
                        )
                        if source_instance is not None:
                            pass
                        else:
                            task_logger.info("Error: Failed to create source_instance.")
                    else:
                        task_logger.info(
                            "Error: Client identifier or client secret not found in the API key for "
                            "MyAffiliates platform.")
                except json.decoder.JSONDecodeError:
                    task_logger.info("Error: Failed to decode API key JSON.")

        elif source_info['platform_name'] == constants.Inhouse and source_info['name'] == "Buffalo Partners ":
            platform = constants.Buffalo_Partners
            adve_name = source_info['name']
            tlog_id = str(source_info["operator_id"])
            username = source_info['username']
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            tlog_username = source_info['username']
            api_key = source_info['api_key']
            base_url = source_info['endpoint']
            loopback_days = source_info['loopback_days']
            dates = source_info["recovery_dates"].split(":")[1]
            # start_date = source_info["start_date"]
            source_instance = BuffaloPartner(
                tlog_username, api_key, start_date, end_date, base_url, loopback_days, tlog_id, dates, True
            )

        elif source_info['platform_name'] == constants.Smartico:
            platform = constants.Smartico
            adve_name = source_info['name']
            tlog_id = str(source_info["operator_id"])
            username = source_info['username']
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            token = source_info['api_key']
            base_url = source_info['endpoint']
            loopback_days = source_info['loopback_days']
            dates = source_info["recovery_dates"].split(":")[1]
            source_instance = Smartico(
                token, base_url, start_date, tlog_id, loopback_days, dates, True
            )

        elif source_info['platform_name'] == constants.Q:
            platform = constants.Q
            adve_name = source_info['name']
            tlog_id = str(source_info["operator_id"])
            username = source_info['username']
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            endpoint = source_info['endpoint']
            # start_date = source_info["start_date"]
            loopback_days = source_info['loopback_days']
            dates = source_info["recovery_dates"].split(":")[1]
            api_key_str = source_info['api_key']
            context = get_current_context()

            if api_key_str:
                try:
                    api_key_str = api_key_str.strip()
                    api_key_json = json.loads(api_key_str)
                    api_key = api_key_json.get('apikey')
                    if not api_key:
                        raise ValueError("apikey is missing in the API key JSON")

                    source_instance = SourceQ(
                        api_key, endpoint, "1", start_date, tlog_id, loopback_days, dates, True
                    )

                    if source_instance is not None:
                        payload = source_instance.create_source_payload(
                            name=name, airbyte_workspace_id=airbyte_workspace_id
                        )
                    else:
                        logging.info("Failed to create source_instance.")
                except json.JSONDecodeError:
                    logging.error("Error: Failed to decode API key JSON.")
                except ValueError as e:
                    logging.error(str(e))
            else:
                logging.error(f"Api Key Missing for operator_id {tlog_id}")

        elif source_info['platform_name'] == constants.Income_Access:
            platform = constants.Income_Access
            adve_name = source_info['name']
            tlog_id = str(source_info['operator_id'])
            username = source_info['username']
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            endpoint = source_info['endpoint']
            # start_date = source_info["start_date"]
            dates = source_info["recovery_dates"].split(":")[1]
            loopback_days = source_info['loopback_days']
            api_key_str = source_info['api_key']
            context = get_current_context()

            if api_key_str:
                try:
                    api_key_str = api_key_str.strip()
                    api_key_json = json.loads(api_key_str)
                    api_key = api_key_json.get('apikey')
                    if not api_key:
                        raise ValueError("apikey is missing in the API key JSON")
                    source_instance = IncomeAccess(
                        api_key, endpoint, start_date, tlog_id, loopback_days, dates, True
                    )
                    if source_instance is not None:
                        payload = source_instance.create_source_payload(
                            name=name, airbyte_workspace_id=airbyte_workspace_id
                        )
                    else:
                        logging.info("Failed to create source_instance.")
                except json.JSONDecodeError:
                    logging.error("Error: Failed to decode API key JSON.")
                except ValueError as e:
                    logging.error(str(e))
            else:
                logging.error(f"Api Key Missing for operator_id {tlog_id}")

        elif source_info['platform_name'] == constants.SoftSwiss:
            platform = constants.SoftSwiss
            adve_name = source_info['name']
            tlog_id = str(source_info["operator_id"])
            username = source_info['username']
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            api_key = source_info['api_key']
            endpoint = source_info['endpoint']
            exchange_rates_date = Variable.get('exchange_rates_date')
            dates = source_info["recovery_dates"].split(":")[1]
            step = Variable.get('step')
            dynamic_tag = Variable.get('dynamic_tag')
            loopback_days = source_info['loopback_days']
            api_key_str = source_info['api_key']
            context = get_current_context()

            if api_key_str:
                try:
                    api_key_str = api_key_str.strip()
                    api_key_json = json.loads(api_key_str)
                    api_key = api_key_json.get('statistic_token')
                    if not api_key:
                        raise ValueError("apikey is missing in the API key JSON")

                    endpoint = source_info['endpoint']
                    loopback_days = source_info['loopback_days']
                    exchange_rates_date = start_date
                    step = '10'

                    source_instance = Softswiss(
                        endpoint, api_key, start_date, exchange_rates_date, tlog_id,
                        step, loopback_days, dynamic_tag, dates, True
                    )

                    if source_instance is not None:
                        payload = source_instance.create_source_payload(
                            name=name, airbyte_workspace_id=airbyte_workspace_id
                        )
                    else:
                        logging.info("Failed to create source_instance.")
                except json.JSONDecodeError:
                    logging.error("Error: Failed to decode API key JSON.")
                except ValueError as e:
                    logging.error(str(e))
            else:
                logging.error(f"Api Key Missing for operator_id {tlog_id}")

        elif source_info["platform_name"] == constants.EGO:
            platform = constants.EGO
            adve_name = source_info["name"]
            tlog_id = str(source_info["operator_id"])
            username = source_info["username"]
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            tlog_username = source_info["username"]
            tlog_password = source_info["password"]
            endpoint = source_info["endpoint"]
            loopback_days = source_info['loopback_days']
            dates = source_info["recovery_dates"].split(":")[1]
            source_instance = Ego(
                tlog_password, endpoint, tlog_username, start_date, tlog_id, loopback_days, dates, True
            )
            source_class = Ego

        elif source_info["platform_name"] == constants.Mexos:
            platform = constants.Mexos
            adve_name = source_info["name"]
            tlog_id = str(source_info["operator_id"])
            username = source_info["username"]
            name = f"{adve_name}-{tlog_id}"
            reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
            tlog_username = source_info["username"]
            tlog_password = source_info["password"]
            endpoint = source_info["endpoint"]
            loopback_days = str(source_info['loopback_days'])
            api_key = source_info["api_key"]
            dates = source_info["recovery_dates"].split(":")[1]
            task_logger.info(dates)

            # start_date = source_info["start_date"]

            if api_key is not None or api_key != '':
                try:
                    api_key = json.loads(api_key)
                    campaign_id = api_key.get("campaignId")
                    mexos_id = str(api_key.get("id"))
                    mexos_vars = api_key.get("var")
                    source_class = Mexos

                    if campaign_id and mexos_id and mexos_vars:
                        source_instance = Mexos(
                            tlog_username, tlog_password, mexos_id, mexos_vars[0], start_date,
                            campaign_id, tlog_id, loopback_days, endpoint, dates, True
                        )
                        if source_instance is not None:
                            payload = source_instance.create_source_payload(
                                name=name, airbyte_workspace_id=airbyte_workspace_id
                            )
                        else:
                            task_logger.info("Error : Failed to create source_instance")
                except json.decoder.JSONDecodeError:
                    task_logger.info("Error: Failed to decode API key JSON")

        else:
            task_logger.info("Source connector not found")

        if source_instance is None:
            task_logger.info("error: Failed to create source_instance.")

        recovery_source_id = source_info["recovery_airbyte_source_id"]

        if recovery_source_id is not None and recovery_source_id != '':
            # Create payload specific to a source_instance
            payload = source_instance.update_source_payload(
                name=reprocess_name, source_id=recovery_source_id, airbyte_workspace_id=airbyte_workspace_id
            )
            # Update the source with new information
            response = AirbyteSources.update_source(payload)
            task_logger.info(response)

        context = get_current_context()

        result = Utils.fetch_path_from_data_source(source_info["recovery_airbyte_connection_id"])
        bucket_name = os.getenv("Input_bucket")
        for path in result:
            print(path)
            path = path[0]
            folder_path = os.path.dirname(path)
            # Delete any raw_data.jsonl files (of prior syncs) present in s3
            delete_all_files_in_folder(bucket_name, folder_path)
        _AIRBYTE_CONN = os.getenv('airbyte_conn')
        airbyte_connections = source_info["recovery_airbyte_connection_id"]
        task_logger.info(airbyte_connections)

        job_id = 0
        is_job_complete = False
        task_logger.info(f"Start syncing data for airbyte connection {airbyte_connections} .")
        retries=0
        max_retries=5
        retry_delay=10
        while retries < max_retries:

            # Operator for starting an Airbyte connection sync
            trigger_sync = HttpOperator(
                method="POST",
                task_id='start_airbyte_sync',
                http_conn_id=_AIRBYTE_CONN,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",  # Airbyte cloud requires that a user agent is defined
                    "Accept": "application/json"
                },
                endpoint=f'/api/v1/connections/sync',
                data=json.dumps({"connectionId": airbyte_connections}),
                response_filter=lambda response: {
                    'job_id': response.json()['job']['id'],
                    'config_id': response.json()['job']['configId']
                }
            )
            try:
                # triggering airbyte sync
                response_values = trigger_sync.execute(context=context)
                job_id = response_values['job_id']
                config_id = response_values['config_id']
                break
            except Exception as e:
                error_msg = str(e)
                if "502" in error_msg or "Bad Gateway" in error_msg:
                    retries += 1
                    time.sleep(retry_delay)
                    task_logger.info(f"Received 502 error on connection_sync for connection_id: {airbyte_connections}. Retrying {retries+1}/{max_retries}...")
                elif "409" in error_msg or "Conflict" in error_msg:
                    #Condition to check conflicts in airbyte sync
                    task_logger.info(f"Conflict for connection_id: {airbyte_connections}")
                    # Function to get job_id and config_id for running sync
                    response = Utils.check_conflict(airbyte_connections)
                    job_id = response['job']['id']
                    config_id = response['job']['configId']  
                    break # Exit the loop once job_id and config_id are obtained                       
                else:
                    raise AirflowFailException(f"Error as {e}")
        if retries == max_retries:
             raise AirflowFailException(f"Max retries exceded")

        dates = source_info["recovery_dates"]
        ti.xcom_push(key='job_startdate', value=f"{job_id}/{config_id}/{dates}")
        stream_state = "Incremental"
        ti.xcom_push(key=f'stream_state', value=f'{stream_state}')

        task_logger.info(f"job ids --->{job_id}")
        task_logger.info(f"Config ids --->{config_id}")

        task_logger.info(f"Waiting job {job_id} of airbyte connection {airbyte_connections} complete.")
        # Sensor to check airbyte sync status
        retries=0
        while retries < max_retries:
            wait_for_sync_to_complete = HttpSensor(
                method="POST",
                task_id="wait_for_airbyte_sync",
                http_conn_id=_AIRBYTE_CONN,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "fake-useragent",
                    "Accept": "application/json",
                },
                request_params=json.dumps({"id": job_id}),
                endpoint="/api/v1/jobs/get",
                poke_interval=90,
                soft_fail=True,
                response_check=lambda response: airbyte_is_job_complete(response, operator_id),
            )
            try:
                is_job_complete = wait_for_sync_to_complete.execute(context=context)
                break
            except Exception as e:
                error_msg = str(e)
                if "502" in error_msg or "Bad Gateway" in error_msg or "500" in error_msg or "Internal Server Error" in error_msg:
                    #Retry when we get status code as 502
                    retries += 1
                    time.sleep(retry_delay)  
                    task_logger.info(f"Received {error_msg} error on connection_sync for connection_id: {airbyte_connections}. Retrying {retries}/{max_retries}...")
                elif "SIGTERM" in error_msg:
                    retries += 1
                    time.sleep(retry_delay)  
                    task_logger.info(f"Received {error_msg} error on connection_sync for connection_id: {airbyte_connections}. Retrying {retries}/{max_retries}...")
                else:
                    raise AirflowFailException(f"Error as {e}")                    
        if retries == max_retries:
             raise AirflowFailException(f"Max retries exceded") 
        task_logger.info(f"Job {job_id} of airbyte connection {airbyte_connections} complete.")

    except Exception as e:
        task_logger.error(f"Error {e} .")
        airflow_trigger_airbyte_sync_task_error_slack_alert({"config_id": airbyte_connections, "error_msg": str(e)})

        if job_id == 0:
            raise AirflowFailException(f"Error when creating job for airbyte connection {airbyte_connections} .")
        elif is_job_complete == False:
            raise AirflowFailException(
                f"Error when waiting {job_id} of airbyte connection {airbyte_connections} complete.")
        else:
            raise AirflowFailException(
                f"Error when check job {job_id} status of airbyte connection {airbyte_connections} complete.")

    else:
        if status_ in ("failed", "cancelled"):
            raise AirflowFailException(f"Job has {status_}")


def airbyte_is_job_complete(response, operator_id):
    """
        Returns True if an airbyte sync is succeeded, failed or cancelled, else False.
        If job has succeeded, then rename files loaded in s3 i.e. from raw_data.jsonl to reprocess_raw_data.jsonl
    """
    global status_

    job_status_dict = json.loads(response.text)
    status_ = job_status_dict['job']['status']

    if status_ == 'succeeded':
        result = Utils.get_s3_path(operator_id)
        for res in result:
            path = res['path']
            task_logger.info(f"path---->{path}")
            dir_name, file_name = os.path.split(path)
            bucket_name=os.getenv("Input_bucket")
            bucket_path=os.getenv('Input_bucket_path')
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_path+dir_name)
            if "Contents" in response:
                for obj in response['Contents']:
                    dir_name, file_name = os.path.split(obj['Key'])
                    path=os.path.join(dir_name,file_name)
                    reprocess_path = os.path.join(dir_name, f"reprocess_{file_name}")
                    task_logger.info(f"reprocess_path---->{reprocess_path}")
                    source_bucket = os.getenv('Input_bucket')
                    source_key = path
                    destination_bucket = os.getenv('Input_bucket')
                    destination_key = reprocess_path
                    rename_object(source_bucket=source_bucket, source_key=source_key,
                                destination_bucket=destination_bucket, destination_key=destination_key)
        return True
    elif status_ == 'failed':
        airflow_airbyte_sync_task_slack_alert(job_status_dict)
        return True
    elif status_ == 'cancelled':
        airflow_airbyte_sync_task_slack_alert(job_status_dict)
        return True
    else:
        return False