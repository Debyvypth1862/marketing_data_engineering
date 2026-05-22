import snowflake.connector
import mysql.connector
import sys
sys.path.insert(1,"dags/airbyte")
from db_connection import mysql_conn
connection = mysql_conn()
# from airbyte.Utils import Utils
import os
from urllib.parse import urlparse
import json
import requests
import logging
from airflow.models import Variable
logger = logging.getLogger(__name__)
 
def apisix_sync():
    api_url = Variable.get("API_SIX_URL")
    #'https://localhost:55801/'
    api_six_api_key = Variable.get("API_SIX_API_KEY")
    #'ghmd9SRhwq78558eVJfB3LmuAToWbX0k'
    devdbCursor = connection.cursor()
    devdbCursor.execute("SELECT * from PLATFORM")
    platformResult = devdbCursor.fetchall()
    for platform in platformResult:
        name = platform[1]
        platform_id = platform[0]
        sql_query = f"SELECT * from ACCOUNT WHERE platform_id={platform_id}"
        devdbCursor.execute(sql_query)
        accountResult = devdbCursor.fetchall()
        hosts = set()
        for account in accountResult:
            parsed_url = urlparse(account[20])
            base_domain = parsed_url.netloc
            hosts.add(base_domain)
        template_body = {
            "uri": "/*",
            "name": "CellXpert",
            "host": "",
            "plugins": {
                "limit-req": {
                    "_meta": {
                        "disable": False
                    },
                    "burst": 10,
                    "key": "remote_addr",
                    "key_type": "var",
                    "rate": 1,
                    "rejected_code": 429,
                    "rejected_msg": "APISIX burst limit exceeded"
                },
                "prometheus": {
                    "_meta": {
                        "disable": False
                    }
                },
                "response-rewrite": {
                    "_meta": {
                        "disable": False
                    },
                    "headers": {
                        "set": {
                            "X-ApiSix-Timestamp": "$time_iso8601"
                        }
                    }
                },
                "file-logger": {
                    "_meta": {
                        "disable": False
                    },
                    "include_req_body": True,
                    "include_resp_body": True,
                    "path": "/usr/local/apisix/logs/file.log"
                },
            },
            "upstream": {
                "nodes": [
                    {
                        "host": "",  # This will be updated in the loop
                        "port": 443,
                        "weight": 1
                    }
                ],
                "timeout": {
                    "connect": 600,
                    "send": 600,
                    "read": 600
                },
                "type": "roundrobin",
                "scheme": "https",
                "pass_host": "pass",
                "keepalive_pool": {
                    "idle_timeout": 60,
                    "requests": 1000,
                    "size": 320
                }
            },
            "status": 1
        }
        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": f"{api_six_api_key}"  # Uncomment and modify if needed
        }
        i = 0
        hosts_existed = set()
        response_check = requests.get(f"{api_url}/apisix/admin/routes", headers=headers,verify=False)
        content = json.loads(response_check.content)
        # hosts = set()
        for cont in content["list"]:
            for node in cont["value"]["upstream"]["nodes"]:
                try:
                    hosts_existed.add(node["host"])
                    # hosts.add(node["host"])\
                except:
                    next
 
        for host in hosts:
            if host not in hosts_existed:
                try:
                    template_body["plugins"]["limit-req"]["rate"] = 30 #Variable.get(f"{name}_Rate_limit")
                    template_body["upstream"]["nodes"][0]["host"] = host
                    template_body["host"] = host
                    template_body["name"] = f"{name}-{host}"
                    template_body["uri"] = "/*"
                    response = requests.post(f"{api_url}/apisix/admin/routes", headers=headers, data=json.dumps(template_body),verify=False)
                    if response.status_code == 201:
                        logger.info(f"Route created successfully for host {host}!")
                    else:
                        logger.info(f"Failed to create route for host {host}. Status code: {response.status_code}, Response: {response.text}")
                    i += 1
                except Exception as e:
                    logger.info(f"Skip {e}")
    connection.commit()
    connection.close()
 
 
def update_rate_limit():
    api_six_url = Variable.get("API_SIX_URL") 
    api_url = f"{api_six_url}/apisix/admin/routes"
    api_six_api_key = Variable.get("API_SIX_API_KEY")
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": f"{api_six_api_key}"  # Uncomment and modify if needed
    }
    response = requests.get(api_url, headers=headers, verify=False)
    logger.info(f"Get routes successfully: {response.status_code}!")
    content = json.loads(response.content)
 
    for cont in content["list"]:
        try:
            route_name = cont['value']['name']
            platform_name = route_name.split('-')[0]
            cont['value']['plugins']['limit-req']['rate'] = Variable.get(f"{platform_name}_Rate_limit")
            logger.info(f"Route ID: {cont['value']['id']}")
            logger.info(f"Content: {cont['value']}")
            response = requests.patch(f"{api_url}/{cont['value']['id']}", headers=headers, data=json.dumps(cont['value']), verify=False)
            logger.info(f"Route created successfully for host {cont['value']['upstream']['nodes'][0]['host']}!")
            logger.info(f"Route created successfully: {response}!")
    # hosts.add(node["host"])\
        except:
            logger.info(f"Skip!!!")


