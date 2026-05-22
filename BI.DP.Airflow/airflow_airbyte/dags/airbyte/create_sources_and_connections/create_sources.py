import os
import sys
import json
import logging
from mysql.connector import Error
from airflow.models import Variable
from airflow.operators.python import get_current_context

sys.path.insert(1, "dags/airbyte/create_sources_and_connections")
sys.path.append("dags/airbyte")
from referon import Referon
from mexos import Mexos
from cellxpert import Cellxpert
from alanbase import Alanbase
from voluum import Voluum
from buffalopartner import BuffaloPartner
from ego import Ego
from income_access import IncomeAccess
from myaffiliates import MyAffiliates
from netrefer import NetRefer
from q import SourceQ
from smartico import Smartico
from softswiss import Softswiss
from google_analytics4 import GoogleAnalytics4
from airbytesources import AirbyteSources
from airbyteconnections import AirbyteConnections
from airbyte import constants
from utils import Utils
from slack_alerts import operator_validation_fail_slack_alert


logger = logging.getLogger(__name__)


def create_check_sources():
    try:

        airbyte_workspace_id = os.getenv('workspace_id')
        end_date = Variable.get('end_date')
        destination_id = Variable.get('destination_id')
        merchants = Variable.get('merchants')
        step = Variable.get('step')
        exchange_rates_date = Variable.get('exchange_rates_date')
        dynamic_tag = Variable.get('dynamic_tag')
        Utils.update_account_status_for_valid_source()

        # Fetch source information from the table
        source_info_list = Utils.fetch_source_info()

        # Dictionary to store validation results
        validation_status = None
        context = get_current_context()

        for source_info in source_info_list:
            source_instance = None
            start_date = source_info["start_date"] #Variable.get('start_date')
            operator_id = source_info['operator_id']
            Id = source_info['id']

            if source_info["platform_name"] == constants.Cellxpert:
                logging.info(f"operator_id----->{operator_id}")
                endpoint = source_info["endpoint"]
                platform = constants.Cellxpert
                adve_name = source_info["name"]
                tlog_id = str(source_info["operator_id"])
                username = source_info["username"]
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                tlog_password = source_info["password"]
                tlog_username = source_info["username"]
                loopback_days = source_info['loopback_days']
                
                source_instance = Cellxpert(
                    endpoint, start_date, tlog_password, tlog_username, tlog_id, loopback_days, "", "", False
                )
                source_class = Cellxpert
            elif source_info["platform_name"] == constants.Alanbase:
                logging.info(f"operator_id----->{operator_id}")
                endpoint = source_info["endpoint"]
                platform = constants.Alanbase
                adve_name = source_info["name"]
                tlog_id = str(source_info["operator_id"])
                username = source_info["username"]
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                api_key_str = source_info['api_key']
                context = get_current_context()

                if api_key_str:
                    try:
                        api_key_str = api_key_str.strip()
                        api_key_json = json.loads(api_key_str)
                        api_key = api_key_json.get('api_key',None)
                        if not api_key:
                            raise ValueError("api_key is missing in the API key JSON")
                        loopback_days = source_info['loopback_days']
                        source_instance = Alanbase(
                            endpoint, start_date, api_key, tlog_id, loopback_days, "", "", False
                        )
                        source_class = Alanbase

                        if source_instance is not None:
                            payload = source_instance.create_source_payload(
                                name=name, airbyte_workspace_id=airbyte_workspace_id
                            )
                        else:
                            logging.info("Failed to create source_instance.")
                            operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except json.JSONDecodeError:
                        logging.error("Error: Failed to decode API key JSON.")
                        operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except ValueError as e:
                        logging.error(str(e))
                        operator_validation_fail_slack_alert(context, platform, tlog_id, str(e))
                else:
                    logging.error(f"Api Key Missing for operator_id {tlog_id}")
                    validation_status = constants.Invalid
                    message = "No API key found"
                    operator_id = source_info["operator_id"]
                    
                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    operator_validation_fail_slack_alert(context, platform, tlog_id, message)
                    Utils.update_validation_status_for_missing_api_key(tlog_id, validation_status)
                    Utils.update_validation_data(tlog_id, validation_status, message)

            elif source_info["platform_name"] == constants.Voluum:
                logging.info(f"operator_id----->{operator_id}")
                loopback_days = source_info['loopback_days']
                platform = constants.Voluum
                adve_name = source_info["name"]
                tlog_id = str(source_info["operator_id"])
                username = source_info["username"]
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                
                source_instance = Voluum(start_date, loopback_days, "", "", "", "", "", "", "", False)
                source_class = Voluum
            elif source_info["platform_name"] == constants.Google_Analytics:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.Google_Analytics
                adve_name = source_info["name"]
                property_id = str(source_info["operator_id"])
                username = source_info["username"]
                name = f"{adve_name}-{property_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{property_id}")
                reprocess_name = f"Reprocess-{adve_name}-{property_id}"
                reprocess_connection_name = Utils.connection_name_format(
                    f"Reprocess_{platform}_{adve_name}_{property_id}"
                )
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{property_id}")
                
                source_instance = GoogleAnalytics4(
                    property_id, start_date, False, "", "", "", "", "", "", "", ""
                )
                source_class = GoogleAnalytics4
            elif source_info["platform_name"] == constants.NetRefer:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.NetRefer
                adve_name = source_info['name']
                tlog_id = str(source_info["operator_id"])
                username = source_info['username']
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")

                api_key_str = source_info['api_key']
                context = get_current_context()

                if api_key_str:
                    try:
                        api_key_str = api_key_str.strip()
                        api_key_json = json.loads(api_key_str)
                        api_key = api_key_json.get('api_key',None)
                        if not api_key:
                            raise ValueError("XML key is missing in the API key JSON")

                        endpoint = source_info['endpoint']
                        loopback_days = source_info['loopback_days']

                        source_instance = NetRefer(
                            api_key, endpoint, start_date, tlog_id, loopback_days, "", False
                        )
                        source_class = NetRefer

                        if source_instance is not None:
                            payload = source_instance.create_source_payload(
                                name=name, airbyte_workspace_id=airbyte_workspace_id
                            )
                        else:
                            logging.info("Failed to create source_instance.")
                            operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except json.JSONDecodeError:
                        logging.error("Error: Failed to decode API key JSON.")
                        operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except ValueError as e:
                        logging.error(str(e))
                        operator_validation_fail_slack_alert(context, platform, tlog_id, str(e))
                else:
                    logging.error(f"Api Key Missing for operator_id {tlog_id}")
                    validation_status = constants.Invalid
                    message = "No API key found"
                    operator_id = source_info["operator_id"]
                    
                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    operator_validation_fail_slack_alert(context, platform, tlog_id, message)
                    Utils.update_validation_status_for_missing_api_key(tlog_id, validation_status)
                    Utils.update_validation_data(tlog_id, validation_status, message)

            elif source_info["platform_name"] == constants.Referon:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.Referon
                adve_name = source_info['name']
                tlog_id = str(source_info["operator_id"])
                username = source_info['username']
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")

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
                            raise ValueError("export id is missing in the API key JSON")
                        

                        endpoint = source_info['endpoint']
                        loopback_days = source_info['loopback_days']

                        source_instance = Referon(
                            api_key_str, endpoint, start_date, tlog_id, loopback_days, "", False
                        )
                        source_class = Referon

                        if source_instance is not None:
                            payload = source_instance.create_source_payload(
                                name=name, airbyte_workspace_id=airbyte_workspace_id
                            )
                        else:
                            logging.info("Failed to create source_instance.")
                            operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except json.JSONDecodeError:
                        logging.error("Error: Failed to decode API key JSON.")
                        operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except ValueError as e:
                        logging.error(str(e))
                        operator_validation_fail_slack_alert(context, platform, tlog_id, str(e))
                else:
                    logging.error(f"Api Key Missing for operator_id {tlog_id}")
                    validation_status = constants.Invalid
                    message = "No API key found"
                    operator_id = source_info["operator_id"]
                    
                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    operator_validation_fail_slack_alert(context, platform, tlog_id, message)
                    Utils.update_validation_status_for_missing_api_key(tlog_id, validation_status)
                    Utils.update_validation_data(tlog_id, validation_status, message)


            elif source_info['platform_name'] == constants.MyAffiliates:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.MyAffiliates
                adve_name = source_info['name']
                tlog_id = str(source_info["operator_id"])
                username = source_info['username']
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                endpoint = source_info['endpoint']
                loopback_days = source_info['loopback_days']
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
                                tlog_id, loopback_days, "", False
                            )
                
                            if source_instance is not None:
                                pass
                            else:
                                logging.info("Error: Failed to create source_instance.")
                        else:
                            logging.info("Error: Client identifier or client secret not found in the API key for "
                                         "MyAffiliates platform.")
                            operator_validation_fail_slack_alert(context, platform, operator_id, "Incorrect API key")
                    except json.decoder.JSONDecodeError:
                        logger.error("Error: Failed to decode API key JSON.")
                        operator_validation_fail_slack_alert(context, platform, operator_id, "Incorrect API key")
                    source_class = MyAffiliates

                else:
                    operator_validation_fail_slack_alert(context, platform, operator_id, "No api key found")
                    logger.error(f"Api Key Missing for operator_id {operator_id} ")
                    validation_status = constants.Invalid
                    message = "No api key found"
                    operator_id = source_info["operator_id"]
                    
                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    Utils.update_validation_status_for_missing_api_key(operator_id, validation_status)
                    Utils.update_validation_data(Id, validation_status, message)

            elif source_info['platform_name'] == constants.Inhouse and source_info['name'] == "Buffalo Partners ":
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.Buffalo_Partners
                adve_name = source_info['name']
                tlog_id = str(source_info["operator_id"])
                username = source_info['username']
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                tlog_username = source_info['username']
                api_key = source_info['api_key']
                base_url = source_info['endpoint']
                loopback_days = source_info['loopback_days']

                if api_key:
                    source_instance = BuffaloPartner(
                        tlog_username, api_key, start_date, end_date, base_url, loopback_days, tlog_id, "", False
                    )
                    source_class = BuffaloPartner
                else:
                    logging.error(f"Api Key Missing for operator_id {tlog_id}")
                    validation_status = constants.Invalid
                    message = "No API key found"
                    operator_validation_fail_slack_alert(context, platform, tlog_id, message)
                    operator_id = source_info["operator_id"]

                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    Utils.update_validation_status_for_missing_api_key(tlog_id, validation_status)
                    Utils.update_validation_data(tlog_id, validation_status, message)

            elif source_info['platform_name'] == constants.Smartico:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.Smartico
                adve_name = source_info['name']
                tlog_id = str(source_info["operator_id"])
                username = source_info['username']
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                api_key_json = source_info['api_key']
                loopback_days = source_info['loopback_days']
                if api_key_json:
                    api_key = json.loads(api_key_json)
                    token = api_key['token']
                    base_url = api_key['base_url']
                    source_instance = Smartico(
                        token, base_url, start_date, tlog_id, loopback_days, "", False
                    )
                    source_class = Smartico
                else:
                    logging.error(f"Api Key Missing for operator_id {tlog_id}")
                    validation_status = constants.Invalid
                    message = "No API key found"
                    operator_validation_fail_slack_alert(context, platform, tlog_id, message)
                    operator_id = source_info["operator_id"]

                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    Utils.update_validation_status_for_missing_api_key(tlog_id, validation_status)
                    Utils.update_validation_data(tlog_id, validation_status, message)

            elif source_info['platform_name'] == constants.Q:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.Q
                adve_name = source_info['name']
                tlog_id = str(source_info["operator_id"])
                username = source_info['username']
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                api_key_str = source_info['api_key']
                context = get_current_context()

                if api_key_str:
                    try:
                        api_key_str = api_key_str.strip()
                        api_key_json = json.loads(api_key_str)
                        api_key = api_key_json.get('apikey')
                        if not api_key:
                            raise ValueError("apikey is missing in the API key JSON")

                        endpoint = source_info['endpoint']
                        loopback_days = source_info['loopback_days']

                        source_instance = SourceQ(
                            api_key, endpoint, merchants, start_date, tlog_id, loopback_days, "", False
                        )
                        source_class = SourceQ

                        if source_instance is not None:
                            payload = source_instance.create_source_payload(
                                name=name, airbyte_workspace_id=airbyte_workspace_id
                            )
                        else:
                            logging.info("Failed to create source_instance.")
                            operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except json.JSONDecodeError:
                        logging.error("Error: Failed to decode API key JSON.")
                        operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except ValueError as e:
                        logging.error(str(e))
                        operator_validation_fail_slack_alert(context, platform, tlog_id, str(e))
                else:
                    logging.error(f"Api Key Missing for operator_id {tlog_id}")
                    validation_status = constants.Invalid
                    message = "No API key found"
                    operator_validation_fail_slack_alert(context, platform, tlog_id, message)
                    operator_id = source_info["operator_id"]

                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    Utils.update_validation_status_for_missing_api_key(tlog_id, validation_status)
                    Utils.update_validation_data(tlog_id, validation_status, message)

            elif source_info['platform_name'] == constants.Income_Access:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.Income_Access
                adve_name = source_info['name']
                tlog_id = str(source_info['operator_id'])
                username = source_info['username']
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                api_key_str = source_info['api_key']

                context = get_current_context()

                if api_key_str:
                    try:
                        api_key_str = api_key_str.strip()
                        api_key_json = json.loads(api_key_str)
                        api_key = api_key_json.get('apikey')
                        if not api_key:
                            raise ValueError("apikey is missing in the API key JSON")

                        endpoint = source_info['endpoint']
                        loopback_days = source_info['loopback_days']

                        source_instance = IncomeAccess(
                            api_key, endpoint, start_date, tlog_id, loopback_days, "", False
                        )
                        source_class = IncomeAccess

                        if source_instance is not None:
                            payload = source_instance.create_source_payload(
                                name=name, airbyte_workspace_id=airbyte_workspace_id
                            )
                        else:
                            logging.info("Failed to create source_instance.")
                            operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except json.JSONDecodeError:
                        logging.error("Error: Failed to decode API key JSON.")
                        operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except ValueError as e:
                        logging.error(str(e))
                        operator_validation_fail_slack_alert(context, platform, tlog_id, str(e))
                else:
                    logging.error(f"Api Key Missing for operator_id {tlog_id}")
                    validation_status = constants.Invalid
                    message = "No API key found"
                    operator_validation_fail_slack_alert(context, platform, tlog_id, message)
                    operator_id = source_info["operator_id"]

                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    Utils.update_validation_status_for_missing_api_key(tlog_id, validation_status)
                    Utils.update_validation_data(tlog_id, validation_status, message)

            elif source_info["platform_name"] == constants.EGO:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.EGO
                adve_name = source_info["name"]
                tlog_id = str(source_info["operator_id"])
                username = source_info["username"]
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                tlog_username = source_info["username"]
                tlog_password = source_info["password"]
                endpoint = source_info["endpoint"]
                loopback_days = source_info['loopback_days']
                source_instance = Ego(
                    tlog_password, endpoint, tlog_username, start_date, tlog_id, loopback_days, "", False
                )
                source_class = Ego

            elif source_info['platform_name'] == constants.SoftSwiss:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.SoftSwiss
                adve_name = source_info['name']
                tlog_id = str(source_info["operator_id"])
                username = source_info['username']
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
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
                            endpoint, api_key, start_date, exchange_rates_date,
                            tlog_id, step, loopback_days, dynamic_tag, "", False
                        )
                        source_class = Softswiss

                        if source_instance is not None:
                            payload = source_instance.create_source_payload(
                                name=name, airbyte_workspace_id=airbyte_workspace_id
                            )
                        else:
                            logging.info("Failed to create source_instance.")
                            operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except json.JSONDecodeError:
                        logging.error("Error: Failed to decode API key JSON.")
                        operator_validation_fail_slack_alert(context, platform, tlog_id, "Incorrect API key")
                    except ValueError as e:
                        logging.error(str(e))
                        operator_validation_fail_slack_alert(context, platform, tlog_id, str(e))
                else:
                    logging.error(f"Api Key Missing for operator_id {tlog_id}")
                    validation_status = constants.Invalid
                    message = "No API key found"
                    operator_validation_fail_slack_alert(context, platform, tlog_id, message)
                    operator_id = source_info["operator_id"]

                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    Utils.update_validation_status_for_missing_api_key(tlog_id, validation_status)
                    Utils.update_validation_data(tlog_id, validation_status, message)

            elif source_info["platform_name"] == constants.Mexos:
                logging.info(f"operator_id----->{operator_id}")
                platform = constants.Mexos
                adve_name = source_info["name"]
                tlog_id = str(source_info["operator_id"])
                username = source_info["username"]
                name = f"{adve_name}-{tlog_id}"
                connection_name = Utils.connection_name_format(f"{platform}_{adve_name}_{tlog_id}")
                reprocess_name = f"Reprocess-{adve_name}-{tlog_id}"
                reprocess_connection_name = Utils.connection_name_format(f"Reprocess_{platform}_{adve_name}_{tlog_id}")
                namespace_format = Utils.to_camel_case(f"{platform}/{adve_name}/{username}_{tlog_id}")
                tlog_username = source_info["username"]
                tlog_password = source_info["password"]
                endpoint = source_info["endpoint"]
                api_key = source_info["api_key"]
                loopback_days = source_info["loopback_days"]
                context = get_current_context()

                if api_key is not None and api_key != '':
                    try:
                        api_key = json.loads(api_key)
                        campaign_id = api_key.get("campaignId")
                        mexos_id = str(api_key.get("id"))
                        mexos_vars = api_key.get("var")
                        source_class = Mexos

                        if campaign_id and mexos_id and mexos_vars:
                            source_instance = Mexos(tlog_username, tlog_password, mexos_id, mexos_vars[0], start_date,
                                                    campaign_id, tlog_id, loopback_days, endpoint, "", False)
                            if source_instance is not None:
                                payload = source_instance.create_source_payload(
                                    name=name, airbyte_workspace_id=airbyte_workspace_id
                                )
                            else:
                                logger.info("Failed to create source_instance.")
                                operator_validation_fail_slack_alert(context, platform, operator_id, "Incorrect API key")
                    
                    except json.decoder.JSONDecodeError:
                        logger.error("Error: Failed to decode API key JSON.")
                        operator_validation_fail_slack_alert(context, platform, operator_id, "Incorrect API key")
                else:
                    operator_validation_fail_slack_alert(context, platform, operator_id, "No api key found")
                    logger.error(f"Api Key Missing for operator_id {operator_id} ")
                    validation_status = constants.Invalid
                    message = "No api key found"
                    operator_id = source_info["operator_id"]
                    
                    Utils.update_validation_message(operator_id=operator_id, message=message)
                    Utils.update_validation_status_for_missing_api_key(operator_id, validation_status)
                    Utils.update_validation_data(Id, validation_status, message)
            else:
                logger.info("Source connector not found")
                continue

            if source_instance is None:
                logger.info(f"error: Failed to create source_instance for operator id: {operator_id}")
                continue

            source_id = source_info["airbyte_source_id"]
            recovery_source_id = source_info["recovery_airbyte_source_id"]
            endpoint = source_info["endpoint"]
            
            # Check account status and proceed accordingly
            if source_info["account_status"] == "Updated" and \
                    (source_info["airbyte_source_id"] != '' or source_info["airbyte_source_id"] is not None):
                # Update existing source if account status is 'Updated'
                logger.info("Updating the sourceID")
                if source_id is not None and source_id != '':
                    payload = source_instance.update_source_payload(
                        name=name, source_id=source_id, airbyte_workspace_id=airbyte_workspace_id
                    )
                    #Function to update source
                    response = AirbyteSources.update_source(payload)
                    source_id = response.json().get('sourceId')
                    recovery_payload = source_instance.update_source_payload(
                        name=reprocess_name, source_id=recovery_source_id, airbyte_workspace_id=airbyte_workspace_id
                    )
                    #Function to update recovery source
                    recovery_response = AirbyteSources.update_source(recovery_payload)
                    recovery_source_id = recovery_response.json().get('sourceId')
                    
                    if response.status_code == 200:
                        account_status = 'Existing'
                        Utils.update_account_status(source_id, account_status)
                        Utils.update_last_updated(source_id)
                        #Function to check source is valid or not
                        check_connection_response = AirbyteSources.check_connection(source_id)
                    
                        if check_connection_response is not None:
                            if check_connection_response.status_code == 200:
                                response_json = check_connection_response.json()
                                if 'status' in response_json:
                                    status_code = response_json['status']
                                    message = response_json.get('message', '')
                                    validation_status = 'Valid' if status_code == "succeeded" else constants.Invalid
                                    Utils.update_validation_status(source_id, validation_status)

                                    if validation_status == "Failed":
                                        context = get_current_context()
                                        operator_validation_fail_slack_alert(context, platform, operator_id, message)

                                    if validation_status == 'Valid' and (
                                            source_info["airbyte_connection_id"] is None 
                                            or source_info["airbyte_connection_id"] == ''
                                    ):
                                        objsource = source_class.create_connection_payload(
                                            name=connection_name, namespace_format=namespace_format,
                                            source_id=source_id, destination_id=destination_id,
                                            user=username, workspace_id=airbyte_workspace_id
                                        )
                                        response = AirbyteConnections.create_connection(objsource)
                                        connection_id = response.json().get('connectionId')
                                        operator_id = source_info["operator_id"]
                                        Utils.add_connection_id(connection_id, operator_id)

                                    if validation_status == 'Valid' and (
                                            source_info["recovery_airbyte_connection_id"] is None 
                                            or source_info["recovery_airbyte_connection_id"] == ''
                                    ):
                                        objsource = source_class.create_connection_payload(
                                            name=reprocess_connection_name, namespace_format=namespace_format,
                                            source_id=recovery_source_id, destination_id=destination_id, user=username,
                                            workspace_id=airbyte_workspace_id)
                                        response = AirbyteConnections.create_connection(objsource)
                                        connection_id = response.json().get('connectionId')
                                        operator_id = source_info["operator_id"]
                                        Utils.add_recovery_connection_id(connection_id, operator_id)

                                    if message is not None and message != "None" and validation_status == constants.Invalid:
                                        operator_id = source_info["operator_id"]
                                        Utils.update_validation_message(operator_id=operator_id, message=message)
                                    else:
                                        pass
                                    Utils.update_validation_data(Id, validation_status, message)

                                    if validation_status == 'Valid':
                                        valid_message = 'success'
                                        operator_id = source_info["operator_id"]
                                        Utils.update_validation_message(operator_id=operator_id, message=valid_message)
                                    else:
                                        pass
                                else:
                                    try:
                                        message = check_connection_response["failureReason"]["externalMessage"]
                                    except:
                                        message = "The check connection failed because of an internal error"
                                    
                                    logger.info("Error: 'status' key not found in response.")
                                    validation_status = constants.Invalid
                                    Utils.update_validation_status(source_id, validation_status)
                                    
                                    operator_id = source_info["operator_id"]
                                    Utils.update_validation_message(operator_id=operator_id, message=message)
                                    Utils.update_validation_data(
                                        id=source_id, validation_status=validation_status, message=message
                                    )

                                    context = get_current_context()
                                    operator_validation_fail_slack_alert(
                                        context, platform, operator_id,
                                        "The check connection failed because of an internal error"
                                    )

                            else:
                                validation_status = constants.Invalid
                                Utils.update_validation_status(source_id, validation_status)
                                message = f'Invalid response, Status_code = {check_connection_response.status_code}.'
                                operator_id = source_info["operator_id"]
                                Utils.update_validation_message(operator_id=operator_id, message=message)
                                Utils.update_validation_data(
                                    id=source_id, validation_status=validation_status, message=message
                                )

                                context = get_current_context()
                                operator_validation_fail_slack_alert(context, platform, operator_id, "Bad Credentials")

                        else:
                            validation_status = constants.Invalid
                            Utils.update_validation_status(source_id, validation_status)
                            message = f"Error checking connection API for sourceId {source_id}. Failed to reach the {endpoint}, request timed out."
                            operator_id = source_info["operator_id"]
                            Utils.update_validation_message(operator_id=operator_id, message=message)
                            logger.info(f"Error check connection api for sourceId->{source_id}: Request timed out")
                            Utils.update_validation_data(
                                id=source_id, validation_status=validation_status, message=message
                            )

                            context = get_current_context()
                            operator_validation_fail_slack_alert(context, platform, operator_id, message)
                    else:
                        logger.info(f"Error for sourceId->{source_id}:")
                else:
                    logger.info(f"sourceId not present for this account")

            elif source_info["account_status"] == "New" or (source_info["account_status"] == "Updated" and (
                    source_info["airbyte_source_id"] == '' or source_info["airbyte_source_id"] is None)):
                logger.info("Creating the sourceID")
                payload = source_instance.create_source_payload(
                    name=name, airbyte_workspace_id=airbyte_workspace_id
                )
                #Function to create new source
                response = AirbyteSources.create_source(payload)
                source_id = response.json().get('sourceId')
                operator_id = source_info["operator_id"]
                Utils.add_source_id(source_id, operator_id)

                recovery_payload = source_instance.create_source_payload(
                    name=reprocess_name, airbyte_workspace_id=airbyte_workspace_id
                )
                recovery_response = AirbyteSources.create_source(recovery_payload)
                recovery_source_id = recovery_response.json().get('sourceId')
                operator_id = source_info["operator_id"]
                Utils.add_recovery_source_id(recovery_source_id, operator_id)

                if response.status_code == 200:
                    account_status = 'Existing'
                    Utils.update_account_status(source_id, account_status)
                    Utils.update_created_at(source_id)

                    #Function to check source is valid or not
                    check_connection_response = AirbyteSources.check_connection(source_id)
                    if check_connection_response is not None:
                        if check_connection_response.status_code == 200:
                            response_json = check_connection_response.json()
                            if 'status' in response_json:
                                status_code = response_json['status']
                                message = response_json.get('message', '')
                                validation_status = 'Valid' if status_code == "succeeded" else constants.Invalid

                                if validation_status == "Failed":
                                    context = get_current_context()
                                    operator_validation_fail_slack_alert(context, platform, operator_id, message)

                                if validation_status == 'Valid':
                                    objsource = source_class.create_connection_payload(
                                        name=connection_name, namespace_format=namespace_format, source_id=source_id,
                                        destination_id=destination_id, user=username, workspace_id=airbyte_workspace_id
                                    )
                                    #Function to create new connection
                                    response = AirbyteConnections.create_connection(objsource)
                                    connection_id = response.json().get('connectionId')
                                    operator_id = source_info["operator_id"]
                                    Utils.add_connection_id(connection_id, operator_id)

                                    objsource = source_class.create_connection_payload(
                                        name=reprocess_connection_name, namespace_format=namespace_format,
                                        source_id=recovery_source_id, destination_id=destination_id,
                                        user=username, workspace_id=airbyte_workspace_id
                                    )
                                    recovery_response = AirbyteConnections.create_connection(objsource)
                                    connection_id = recovery_response.json().get('connectionId')
                                    operator_id = source_info["operator_id"]
                                    Utils.add_recovery_connection_id(connection_id, operator_id)

                                    if source_info["connection_status"] is None or source_info["connection_status"] == "":
                                        connection_status = 'Enabled'
                                        Utils.update_connection_status(connection_id, connection_status)
                                    else:
                                        pass
                                else:
                                    pass

                                # Update validation status for the new source
                                Utils.update_validation_status(source_id, validation_status)

                                if message is not None and message != "None" and validation_status == constants.Invalid:
                                    operator_id = source_info["operator_id"]
                                    Utils.update_validation_message(operator_id=operator_id, message=message)
                                else:
                                    pass

                                Utils.update_validation_data(Id, validation_status, message)

                                if validation_status == 'Valid':
                                    valid_message = 'success'
                                    operator_id = source_info["operator_id"]
                                    Utils.update_validation_message(operator_id=operator_id, message=valid_message)
                                else:
                                    pass
                            else:
                                try:
                                    message = check_connection_response["failureReason"]["externalMessage"]
                                except:
                                    message = "The check connection failed because of an internal error"
                                logger.info("Error: 'status' key not found in response.")
                                validation_status = constants.Invalid
                                Utils.update_validation_status(source_id, validation_status)
                                
                                operator_id = source_info["operator_id"]
                                Utils.update_validation_message(operator_id=operator_id, message=message)
                                Utils.update_validation_data(
                                    id=source_id, validation_status=validation_status, message=message
                                )

                                context = get_current_context()
                                operator_validation_fail_slack_alert(
                                    context, platform, operator_id,
                                    "The check connection failed because of an internal error"
                                )

                        else:
                            validation_status = constants.Invalid
                            Utils.update_validation_status(source_id, validation_status)
                            message = f'Invalid response, Status_code = {check_connection_response.status_code}.'
                            operator_id = source_info["operator_id"]
                            Utils.update_validation_message(operator_id=operator_id, message=message)
                            Utils.update_validation_data(
                                id=source_id, validation_status=validation_status, message=message
                            )

                            context = get_current_context()
                            operator_validation_fail_slack_alert(context, platform, operator_id, "Bad Credentials")

                    else:
                        validation_status = constants.Invalid
                        Utils.update_validation_status(source_id, validation_status)
                        message = f"Error checking connection API for sourceId {source_id}. Failed to reach the {endpoint}, request timed out."
                        operator_id = source_info["operator_id"]
                        Utils.update_validation_message(operator_id=operator_id, message=message)
                        logger.info(f"Error in check connection api for sourceId->{source_id}: Request timed out")
                        Utils.update_validation_data(id=source_id, validation_status=validation_status, message=message)

                        context = get_current_context()
                        operator_validation_fail_slack_alert(context, platform, operator_id, message)
                else:
                    logger.info(f"Error for sourceId->{source_id}:")

        incorrect_sources = Utils.get_incorrect_sources()
        if incorrect_sources:
            for source in incorrect_sources:
                operator_id = source['operator_id']
                account_status = "Existing"
                Utils.update_account_status_for_invalid_sources(operator_id, account_status)
        else:
            pass
        return None
    except Error as e:
        logger.error(f"Error: {e}")