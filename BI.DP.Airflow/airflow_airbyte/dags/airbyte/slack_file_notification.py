import os
import logging
import sys
import base64
from datetime import datetime
from io import BytesIO
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

sys.path.insert(1, "dags/airbyte")

logger = logging.getLogger(__name__)


def send_inactive_accounts_slack_alert(excel_data, **context):
    """
    Send inactive accounts alert with Excel file attachment to Slack
    Args:
        excel_data (dict): Dictionary with 'filename', 'data' (base64), and 'size'
        **context: Airflow context
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get context information
        if context and 'task_instance' in context:
            task_id = context['task_instance'].task_id
            dag_id = context['task_instance'].dag_id
            logger.info(f"Sending inactive accounts alert - Task: {task_id}, DAG: {dag_id}")
        else:
            logger.info("Sending inactive accounts alert")
        
        # Validate input data
        if not excel_data:
            logger.error("❌ No Excel data provided from previous task")
            logger.info("This task will be skipped. Please check the generate_inactive_accounts_report task.")
            return False
        
        if not isinstance(excel_data, dict):
            logger.error(f"❌ Invalid data format - expected dict, got {type(excel_data)}")
            logger.info("The previous task should return a dictionary with 'filename' and 'data' keys")
            return False
        
        # Extract data from dictionary
        filename = excel_data.get('filename')
        data_base64 = excel_data.get('data')
        file_size = excel_data.get('size', 0)
        
        if not filename or not data_base64:
            logger.error("❌ Missing 'filename' or 'data' in excel_data dictionary")
            logger.info(f"Received keys: {list(excel_data.keys())}")
            return False
        
        logger.info(f"📁 Received Excel file data from XCom")
        logger.info(f"📁 Filename: {filename}")
        logger.info(f"📁 Original file size: {file_size} bytes ({file_size / 1024:.2f} KB)")
        logger.info(f"📁 Base64 encoded size: {len(data_base64)} bytes ({len(data_base64) / 1024:.2f} KB)")
        
        # Decode base64 to bytes
        try:
            excel_bytes = base64.b64decode(data_base64)
            decoded_size = len(excel_bytes)
            logger.info(f"✅ Successfully decoded Excel file: {decoded_size} bytes ({decoded_size / 1024:.2f} KB)")
            
            if decoded_size != file_size:
                logger.warning(f"⚠️ Size mismatch: expected {file_size}, got {decoded_size}")
            
        except Exception as e:
            logger.error(f"❌ Failed to decode base64 data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        
        # Get Slack token from Airflow connection
        try:
            # Try to get the slack connection
            slack_conn = BaseHook.get_connection('slack_api')
            slack_token = slack_conn.password  # Token should be stored in password field
            
            if not slack_token:
                logger.error("Slack token is empty in connection 'slack_api'")
                logger.info("Please add your Slack Bot Token to the 'slack_api' connection password field")
                return False
                
            logger.info("✅ Slack connection retrieved successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to get Slack connection 'slack_api': {e}")
            logger.info("")
            logger.info("=" * 60)
            logger.info("SLACK API CONNECTION NOT FOUND")
            logger.info("=" * 60)
            logger.info("To fix this:")
            logger.info("1. Go to https://api.slack.com/apps")
            logger.info("2. Create a new app: 'Airflow Alert Bot'")
            logger.info("3. Add scopes: chat:write, files:write, channels:read, groups:read")
            logger.info("4. Install to workspace and copy Bot User OAuth Token")
            logger.info("5. In Airflow UI → Admin → Connections → Add:")
            logger.info("   - Connection Id: slack_api")
            logger.info("   - Connection Type: HTTP")
            logger.info("   - Password: <your-bot-token>")
            logger.info("6. Add bot to #accounts-no-traffic-alert channel")
            logger.info("=" * 60)
            logger.info("")
            logger.info("See SLACK_API_SETUP_QUICK_GUIDE.md for detailed instructions")
            return False
        
        # Initialize Slack client
        logger.info("Initializing Slack client...")
        client = WebClient(token=slack_token)
        
        # Prepare the message
        # Prepare the message
        message = (
            "*Weekly Inactive Accounts Alert*\n\n"
            "We are attaching the list of Operator Accounts that have do not have traffic "
            "in the last 90+ days consecutively and are marked as \"active\". "
            "Please review the accounts and let us know if any of these accounts should still be active. "
            "If we do not receive a feedback from you by Friday, we will disable all of these accounts.\n\n"
        )
        
        # Use Channel ID for private channels (more reliable than channel name)
        # Channel: #accounts-no-traffic-alert
        channel = 'C09HABK993J'
        
        # Upload file to Slack from bytes (not from file path)
        logger.info(f"📤 Uploading Excel file to Slack channel: #accounts-no-traffic-alert (ID: {channel})")
        logger.info(f"📤 Filename: {filename}")
        logger.info(f"📤 File size: {len(excel_bytes)} bytes ({len(excel_bytes) / 1024:.2f} KB)")
        
        response = client.files_upload_v2(
            channel=channel,
            content=excel_bytes,  # Upload bytes directly, not a file path
            title="Inactive Accounts Report",
            filename=filename,  # This is the filename users will see in Slack
            initial_comment=message
        )
        
        if response["ok"]:
            logger.info("=" * 60)
            logger.info("✅ SUCCESS! Excel file uploaded to Slack")
            logger.info("=" * 60)
            logger.info(f"Channel: {channel}")
            logger.info(f"File: {filename}")
            logger.info(f"Size: {len(excel_bytes)} bytes")
            logger.info("Users can download this as a normal Excel file (.xlsx)")
            logger.info("=" * 60)
            return True
        else:
            logger.error(f"❌ Failed to send file to Slack: {response}")
            return False
        
    except SlackApiError as e:
        error_msg = e.response.get('error', 'unknown_error')
        logger.error(f"❌ Slack API error: {error_msg}")
        
        if error_msg == 'not_in_channel' or error_msg == 'channel_not_found':
            logger.info("")
            logger.info("=" * 60)
            logger.info("BOT NOT IN CHANNEL")
            logger.info("=" * 60)
            logger.info("The bot needs to be added to #accounts-no-traffic-alert")
            logger.info("")
            logger.info("For PRIVATE channels:")
            logger.info("1. Go to #accounts-no-traffic-alert in Slack")
            logger.info("2. Click the channel name at the top")
            logger.info("3. Click 'Integrations' tab")
            logger.info("4. Click 'Add an App'")
            logger.info("5. Search for and add 'Airflow Alert Bot'")
            logger.info("")
            logger.info("Alternative: Type @Airflow Alert Bot and click 'Invite to Channel'")
            logger.info("=" * 60)
        elif error_msg == 'invalid_auth':
            logger.info("")
            logger.info("=" * 60)
            logger.info("INVALID AUTHENTICATION")
            logger.info("=" * 60)
            logger.info("The Slack token is invalid or expired")
            logger.info("1. Check the token starts with 'xoxb-'")
            logger.info("2. Generate a new token if needed")
            logger.info("3. Update the Airflow connection 'slack_api'")
            logger.info("=" * 60)
        elif error_msg == 'missing_scope':
            logger.info("")
            logger.info("=" * 60)
            logger.info("MISSING PERMISSIONS")
            logger.info("=" * 60)
            logger.info("The bot is missing required permissions")
            logger.info("Required scopes:")
            logger.info("  - chat:write")
            logger.info("  - files:write")
            logger.info("  - channels:read")
            logger.info("  - groups:read (for private channels)")
            logger.info("")
            logger.info("Fix:")
            logger.info("1. Go to https://api.slack.com/apps")
            logger.info("2. Select your app → OAuth & Permissions")
            logger.info("3. Add missing scopes")
            logger.info("4. Reinstall app to workspace")
            logger.info("5. Copy NEW token and update Airflow connection")
            logger.info("=" * 60)
        
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error in send_inactive_accounts_slack_alert: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def send_inactive_accounts_alert(**context):
    """
    Main function to send inactive accounts alert to Slack
    This function pulls excel_data from XCom and passes it to the alert function
    Args:
        **context: Airflow context (required to pull XCom data)
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Pull excel_data from the previous task using XCom
        task_instance = context.get('task_instance')
        if not task_instance:
            logger.error("❌ No task_instance found in context")
            return False
        
        excel_data = task_instance.xcom_pull(task_ids='generate_inactive_accounts_report')
        
        if not excel_data:
            logger.error("❌ No excel_data found in XCom from task 'generate_inactive_accounts_report'")
            logger.info("Please check if the generate_inactive_accounts_report task completed successfully")
            return False
        
        logger.info(f"✅ Successfully pulled excel_data from XCom - type: {type(excel_data)}")
        
        # Now call the actual sending function
        return send_inactive_accounts_slack_alert(excel_data, **context)
        
    except Exception as e:
        logger.error(f"❌ Error pulling XCom data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
