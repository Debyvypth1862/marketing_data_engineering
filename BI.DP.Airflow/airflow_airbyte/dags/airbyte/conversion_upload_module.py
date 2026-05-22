"""
Redtrack Conversion Upload Module

This module handles uploading conversion data (purchase_value, purchase_value_reporting)
to Redtrack API from Snowflake for various platforms (Bing, Google, Refills).

Key Features:
- Base class for common conversion upload functionality
- Platform-specific upload implementations
- Control table tracking for idempotency
- Automatic retry logic for API failures
- Slack notifications with detailed statistics
- Configurable via Airflow Variables
"""

import logging
import snowflake.connector
import requests
import json
from datetime import datetime, timedelta
import pytz
import time
import random
from typing import Dict, List, Optional, Any
from airflow.models import Variable

logger = logging.getLogger("airflow.task")

# Slack webhook for "no conversion" alerts (when source data is truly empty)
NO_CONVERSION_SLACK_WEBHOOK = Variable.get("no_conversion_slack_webhook", default_var="")


class ConversionUploadBase:
    """Base class for conversion upload operations"""

    def __init__(
        self,
        api_endpoint: str,
        slack_webhook_url: str,
        snowflake_config: Dict[str, str],
        control_table_name: str,
        platform_name: str,
        conversion_type: str
    ):
        self.api_endpoint = api_endpoint
        self.slack_webhook_url = slack_webhook_url
        self.snowflake_config = snowflake_config
        self.control_table_name = control_table_name
        self.platform_name = platform_name
        self.conversion_type = conversion_type
        self.conn = None

    def connect_to_snowflake(self) -> snowflake.connector.SnowflakeConnection:
        """Establish connection to Snowflake"""
        try:
            logger.info("Attempting to connect to Snowflake")
            self.conn = snowflake.connector.connect(**self.snowflake_config)
            logger.info("Successfully connected to Snowflake")
            return self.conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def close_connection(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")

    def get_missing_dates_from_last_n_days(self, n_days: int = 7) -> List[str]:
        """Get dates from last N days (including yesterday, excluding today) that have no records in control table
        AND have actual data to upload

        Checks last N days for missing data. Yesterday is included in the check and also processed
        separately in Step 2. This is safe because the queries are idempotent - if yesterday was
        already processed in backfill, Step 2 will find 0 new records to upload.

        Args:
            n_days: Number of days to look back (default 7, includes yesterday)

        Returns:
            List of date strings that have zero records in control table AND have data to upload, ordered oldest to newest
        """
        # Get candidate dates (last N days including yesterday, excluding today)
        candidate_dates = get_last_n_days_excluding_yesterday_pst(n_days)

        logger.info(f"Checking {len(candidate_dates)} candidate dates for missing records: {candidate_dates}")
        logger.info(f"Control table: {self.control_table_name}")

        missing_dates = []

        for date in candidate_dates:
            # Check if this date has any records in control table
            has_records = self._check_date_has_records(date)

            if not has_records:
                # Control table has no records, but check if there's actually data to upload
                has_data = self._check_date_has_data(date)
                if has_data:
                    logger.info(f"✓ Date {date} has NO records in control table and HAS data to upload - will process")
                    missing_dates.append(date)
                else:
                    logger.info(f"○ Date {date} has NO records in control table but NO data to upload - skipping")
            else:
                logger.info(f"✗ Date {date} has records in control table - skipping")

        logger.info(f"Found {len(missing_dates)} missing dates to process: {missing_dates}")
        return missing_dates

    def _check_date_has_records(self, date: str) -> bool:
        """Check if control table has any records for this date

        Different control tables use different date column names:
        - DATE: Used by Bing, Google, Taboola
        - CONVERSION_DATE: Used by Refills, Bing High Value
        - Special case for Google High Value: Uses CONVERSION_TIMESTAMP

        Returns True if ANY records exist for this date, False otherwise.
        """
        cursor = None
        try:
            cursor = self.conn.cursor()

            # Try different date column patterns based on control table name
            queries_to_try = []

            if "HIGH_VALUE" in self.control_table_name:
                # High value tables may use CONVERSION_TIMESTAMP or CONVERSION_DATE
                queries_to_try = [
                    f"SELECT COUNT(*) FROM {self.control_table_name} WHERE DATE(CONVERSION_TIMESTAMP) = %s",
                    f"SELECT COUNT(*) FROM {self.control_table_name} WHERE CONVERSION_DATE = %s"
                ]
            elif "REFILLS" in self.control_table_name:
                # Refills tables use CONVERSION_DATE
                queries_to_try = [
                    f"SELECT COUNT(*) FROM {self.control_table_name} WHERE CONVERSION_DATE = %s"
                ]
            else:
                # Standard tables (Bing, Google, Taboola) use DATE
                queries_to_try = [
                    f"SELECT COUNT(*) FROM {self.control_table_name} WHERE DATE = %s"
                ]

            # Try each query until one works
            for query in queries_to_try:
                try:
                    logger.info(f"Checking date {date} in {self.control_table_name} with query: {query}")
                    cursor.execute(query, (date,))
                    result = cursor.fetchone()
                    count = result[0] if result else 0
                    logger.info(f"✓ Query succeeded: Control table {self.control_table_name} has {count} records for date {date}")
                    return count > 0
                except Exception as e:
                    # This query didn't work, try the next one
                    logger.warning(f"✗ Query failed: {query}, error: {e}")
                    continue

            # If all queries failed, log warning and return True (safe default)
            logger.error(f"All queries failed for date {date} in {self.control_table_name}, assuming it has records to avoid reprocessing")
            return True

        except Exception as e:
            logger.error(f"Failed to check records for date {date}: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return True  # Safe default: assume it has records to avoid reprocessing
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def _check_date_has_data(self, date: str) -> bool:
        """Check if there's actual data to upload for this date

        This queries the source data (not control table) to see if there are any
        unsent conversions for this date.

        Returns True if there's data to upload, False otherwise.
        """
        try:
            # Use the subclass's get_unsent_data method to check for data
            data = self.get_unsent_data(date)
            count = len(data) if data else 0
            logger.info(f"Found {count} rows of data to upload for date {date}")
            return count > 0
        except Exception as e:
            logger.error(f"Failed to check data for date {date}: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            # Safe default: assume there's data to avoid missing uploads
            return True

    def create_control_table_if_not_exists(self, table_schema: str):
        """Create control table if it doesn't exist"""
        try:
            logger.info("Checking if control table exists...")
            cursor = self.conn.cursor()
            cursor.execute(table_schema)
            cursor.close()
            logger.info("Control table checked/created")
            return True
        except Exception as e:
            logger.error(f"Failed to create control table: {e}")
            return False

    def record_send_result(
        self,
        primary_keys: Dict[str, Any],
        status: str,
        retry_count: int,
        response_code: str,
        response_body: str,
        additional_fields: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Record the result of sending to the API in the control table"""
        try:
            logger.info(f"Recording result for: {primary_keys}")

            # Clean response body for storage (truncate if needed)
            if response_body:
                response_body = str(response_body)[:1000]

            # Build merge query dynamically based on primary keys and additional fields
            merge_query = self._build_merge_query(primary_keys, status, retry_count, response_code, response_body, additional_fields)

            cursor = self.conn.cursor()
            cursor.execute(merge_query)
            cursor.close()

            logger.info("Recorded result")
            return True
        except Exception as e:
            logger.error(f"Failed to record result: {e}")
            return False

    def _build_merge_query(
        self,
        primary_keys: Dict[str, Any],
        status: str,
        retry_count: int,
        response_code: str,
        response_body: str,
        additional_fields: Optional[Dict[str, Any]] = None
    ) -> str:
        """Build MERGE query for control table - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement _build_merge_query")

    def format_api_payload(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format a row of data into the API payload format - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement format_api_payload")

    def get_unsent_data(self, date: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement get_unsent_data")

    def send_to_api(self, payload: List[Dict[str, Any]], retry_count: int = 0) -> Dict[str, Any]:
        """Send data to API endpoint with unlimited retry logic on errors"""
        try:
            clickid = payload[0]['clickid']
            logger.info(f"Sending data to API for clickid: {clickid}")
            if retry_count > 0:
                logger.info(f"Retry attempt: {retry_count}")

            payload_json = json.dumps(payload)

            if retry_count == 0:
                logger.info(f"Raw request payload: {payload_json}")

            headers = {'Content-Type': 'application/json'}

            logger.info(f"Sending HTTP POST request to {self.api_endpoint}")
            response = requests.post(
                self.api_endpoint,
                data=payload_json,
                headers=headers,
                timeout=30
            )

            logger.info(f"Response status code: {response.status_code}")
            logger.info(f"Raw response content: '{response.text}'")

            # Check for errors (4xx or 5xx) - retry until success
            if response.status_code >= 400:
                error_type = "rate limit" if response.status_code == 429 else f"error {response.status_code}"

                retry_after = response.headers.get('Retry-After')
                if retry_after and response.status_code == 429:
                    try:
                        wait_time = float(retry_after)
                        logger.info(f"Server requested wait time of {wait_time} seconds before retry")
                        logger.warning(f"Hit {error_type}. Waiting {wait_time} seconds before retrying. Attempt {retry_count+1}")
                        time.sleep(wait_time)
                    except ValueError:
                        logger.warning(f"Hit {error_type}. Retrying immediately. Attempt {retry_count+1}")
                else:
                    logger.warning(f"Hit {error_type}. Retrying immediately. Attempt {retry_count+1}")

                return self.send_to_api(payload, retry_count + 1)

            result = {
                'clickid': clickid,
                'status_code': response.status_code,
                'success': 200 <= response.status_code < 300,
                'response_body': response.text,
                'retry_count': retry_count,
                'request_body': payload_json
            }

            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            logger.warning(f"Network error. Retrying immediately. Attempt {retry_count+1}")
            return self.send_to_api(payload, retry_count + 1)

    def send_slack_alert(
        self,
        stats: Dict[str, Any],
        failed_requests: List[Dict[str, Any]],
        date: str,
        start_time: datetime,
        end_time: datetime,
        is_backfill: bool = False
    ):
        """Send alert to Slack with summary information

        Args:
            stats: Statistics dictionary
            failed_requests: List of failed requests
            date: Date being processed
            start_time: Start time of processing
            end_time: End time of processing
            is_backfill: True if this is a backfill date, False for regular daily run
        """
        logger.info("Preparing Slack alert")

        pst_timezone = pytz.timezone('America/Los_Angeles')

        if start_time.tzinfo is None:
            start_time = pytz.utc.localize(start_time)
        if end_time.tzinfo is None:
            end_time = pytz.utc.localize(end_time)

        start_time_pst = start_time.astimezone(pst_timezone)
        end_time_pst = end_time.astimezone(pst_timezone)

        duration_minutes = (end_time - start_time).total_seconds() / 60
        total_processed = stats['total']
        success_rate = (stats['success'] / total_processed * 100) if total_processed > 0 else 0

        # Determine status emoji and color
        if stats['failed'] == 0:
            status_emoji = "✅"
            color = "good"
        elif stats['failed'] > stats['success']:
            status_emoji = "❌"
            color = "danger"
        else:
            status_emoji = "⚠️"
            color = "warning"

        # Add backfill indicator to title
        backfill_label = " [BACKFILL]" if is_backfill else ""

        message = {
            "text": f"{status_emoji} Redtrack Upload Summary - {self.platform_name} {self.conversion_type} - {date}{backfill_label}",
            "attachments": [
                {
                    "color": color,
                    "fields": [
                        {"title": "Processing Date", "value": date, "short": True},
                        {"title": "Run Type", "value": "🔄 Backfill" if is_backfill else "📅 Daily Run", "short": True},
                        {"title": "Platform", "value": self.platform_name, "short": True},
                        {"title": "Conversion Type", "value": self.conversion_type, "short": True},
                        {"title": "Total Records", "value": str(stats['total']), "short": True},
                        {"title": "✅ Successful", "value": f"{stats['success']} ({success_rate:.1f}%)", "short": True},
                        {"title": "❌ Failed", "value": str(stats['failed']), "short": True},
                        {"title": "⏱️ Processing Time", "value": f"{duration_minutes:.1f} minutes", "short": True},
                        {"title": "💰 Total Amount", "value": f"${stats.get('total_amount', 0):,.2f}", "short": True},
                        {"title": "🕐 Start Time (PST)", "value": start_time_pst.strftime("%Y-%m-%d %H:%M:%S"), "short": True},
                        {"title": "🕐 End Time (PST)", "value": end_time_pst.strftime("%Y-%m-%d %H:%M:%S"), "short": True}
                    ],
                    "footer": f"Redtrack {self.conversion_type} ({self.platform_name}) - AWS Snowflake",
                    "ts": int(time.time())
                }
            ]
        }

        # Add failed requests details if any
        if failed_requests:
            failed_details = ""
            for failed in failed_requests[:5]:
                response_text = ""
                if failed.get('response_body'):
                    if isinstance(failed['response_body'], dict):
                        response_text = json.dumps(failed['response_body'])
                    else:
                        response_text = str(failed['response_body'])[:100]

                retry_info = f" (after {failed.get('retry_count', 0)} retries)" if failed.get('retry_count', 0) > 0 else ""
                failed_details += f"\n• {failed['clickid']} - Status: {failed['status_code']}{retry_info}"
                if response_text:
                    failed_details += f" - Response: {response_text}"

            if len(failed_requests) > 5:
                failed_details += f"\n... and {len(failed_requests) - 5} more failures"

            message["attachments"][0]["fields"].append({
                "title": "❌ Failed Request Details",
                "value": failed_details,
                "short": False
            })

        try:
            logger.info("Sending alert to Slack")
            response = requests.post(self.slack_webhook_url, json=message, timeout=10)

            if response.status_code == 200:
                logger.info("Slack alert sent successfully")
            else:
                logger.error(f"Failed to send Slack alert. Status code: {response.status_code}")
        except Exception as e:
            logger.error(f"Error sending Slack alert: {e}")

    def send_no_conversion_alert(self, date: str, is_backfill: bool = False):
        """Send alert to separate Slack webhook when no conversions found in source data

        This is triggered when the source table/view has NO data for the given date,
        not when data exists but was already uploaded (control table filtered).

        Args:
            date: Date being processed
            is_backfill: True if this is a backfill date, False for regular daily run
        """
        logger.info(f"Sending 'no conversion' alert for {self.platform_name} on {date}")

        pst_timezone = pytz.timezone('America/Los_Angeles')
        current_time = datetime.now(pytz.utc).astimezone(pst_timezone)

        run_type = "🔄 Backfill" if is_backfill else "📅 Daily Run"
        backfill_label = " [BACKFILL]" if is_backfill else ""

        message = {
            "text": f"⚠️ No Conversions Found - {self.platform_name} {self.conversion_type} - {date}{backfill_label}",
            "attachments": [
                {
                    "color": "warning",
                    "fields": [
                        {"title": "Platform", "value": self.platform_name, "short": True},
                        {"title": "Conversion Type", "value": self.conversion_type, "short": True},
                        {"title": "Processing Date", "value": date, "short": True},
                        {"title": "Run Type", "value": run_type, "short": True},
                        {"title": "Status", "value": "No data found in source table/view", "short": False},
                        {"title": "🕐 Alert Time (PST)", "value": current_time.strftime("%Y-%m-%d %H:%M:%S"), "short": True}
                    ],
                    "footer": f"Redtrack {self.conversion_type} ({self.platform_name}) - No Conversion Alert",
                    "ts": int(time.time())
                }
            ]
        }

        try:
            logger.info(f"Sending 'no conversion' alert to Slack webhook")
            response = requests.post(NO_CONVERSION_SLACK_WEBHOOK, json=message, timeout=10)

            if response.status_code == 200:
                logger.info("'No conversion' Slack alert sent successfully")
            else:
                logger.error(f"Failed to send 'no conversion' Slack alert. Status code: {response.status_code}")
        except Exception as e:
            logger.error(f"Error sending 'no conversion' Slack alert: {e}")

    def check_source_has_data(self, date: str) -> bool:
        """Check if source table/view has ANY data for this date (ignoring control table)

        This method checks the raw source data to determine if there are any conversions
        for the given date, regardless of whether they've been uploaded or not.

        Args:
            date: Date to check in YYYY-MM-DD format

        Returns:
            True if source has data for this date, False otherwise
        """
        raise NotImplementedError("Subclasses must implement check_source_has_data")

    def process_conversions(self, process_date: str, is_backfill: bool = False):
        """Main processing function

        Args:
            process_date: Date to process in YYYY-MM-DD format
            is_backfill: True if this is a backfill date, False for regular daily run
        """
        start_time = datetime.now()
        backfill_label = " [BACKFILL]" if is_backfill else ""
        logger.info(f"===== Processing {self.platform_name} {self.conversion_type} for date: {process_date}{backfill_label} =====")
        logger.info(f"Started at: {start_time.isoformat()}")

        successful_count = 0
        failed_count = 0
        total_count = 0
        total_amount = 0.0
        failed_requests = []

        try:
            self.connect_to_snowflake()

            # Create control table
            self.create_control_table()

            # Get unsent data
            data = self.get_unsent_data(process_date)
            total_count = len(data)

            # Check if source has no data and send alert if truly empty
            if total_count == 0:
                logger.info(f"No unsent data found for {process_date}, checking if source has any data...")
                source_has_data = self.check_source_has_data(process_date)
                if not source_has_data:
                    logger.warning(f"Source table/view has NO data for {process_date} - sending no conversion alert")
                    self.send_no_conversion_alert(process_date, is_backfill)
                else:
                    logger.info(f"Source has data but all already uploaded for {process_date}")

            # Calculate total amount
            total_amount = self.calculate_total_amount(data)
            logger.info(f"Total amount: ${total_amount:.2f}")

            # Process each row
            logger.info(f"Processing {total_count} rows and sending to API")

            for i, row in enumerate(data):
                try:
                    logger.info(f"Processing row {i+1}/{total_count}")

                    # Format payload
                    payload = self.format_api_payload(row)

                    # Send to API
                    result = self.send_to_api(payload)

                    if result.get('success', False):
                        retry_info = f" (after {result.get('retry_count', 0)} retries)" if result.get('retry_count', 0) > 0 else ""
                        logger.info(f"Processed row {i+1}/{total_count}{retry_info}")
                        successful_count += 1
                        self.record_result(row, 'SUCCESS', result)
                    else:
                        retry_info = f" (after {result.get('retry_count', 0)} retries)" if result.get('retry_count', 0) > 0 else ""
                        logger.error(f"Failed to process row {i+1}/{total_count}{retry_info}")
                        failed_count += 1
                        failed_requests.append(result)
                        self.record_result(row, 'FAILED', result)

                except Exception as e:
                    logger.error(f"Failed to process row {i+1}/{total_count}: {e}")
                    failed_count += 1
                    failed_requests.append({
                        'clickid': 'unknown',
                        'status_code': 'Exception',
                        'error_message': str(e),
                        'response_body': str(e),
                        'retry_count': 0
                    })
                    self.record_result(row, 'ERROR', {'response_body': str(e), 'status_code': 'Exception', 'retry_count': 0})

            end_time = datetime.now()

            stats = {
                'total': total_count,
                'success': successful_count,
                'failed': failed_count,
                'total_amount': total_amount
            }

            logger.info(f"Process finished at {end_time.isoformat()}")
            logger.info(f"SUMMARY - Total: {total_count}, Success: {successful_count}, Failed: {failed_count}")

            self.send_slack_alert(stats, failed_requests, process_date, start_time, end_time, is_backfill)

        except Exception as e:
            logger.critical(f"An error occurred in the main process: {e}")
            import traceback
            logger.debug(f"Stack trace: {traceback.format_exc()}")
            raise
        finally:
            self.close_connection()

    def create_control_table(self):
        """Create control table - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement create_control_table")

    def calculate_total_amount(self, data: List[Dict[str, Any]]) -> float:
        """Calculate total amount - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement calculate_total_amount")

    def record_result(self, row: Dict[str, Any], status: str, result: Dict[str, Any]):
        """Record result - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement record_result")


class BingPurchaseValueUpload(ConversionUploadBase):
    """Bing purchase_value conversion upload to Redtrack"""

    def __init__(self, api_endpoint: str, slack_webhook_url: str, snowflake_config: Dict[str, str], campaign_id: str):
        super().__init__(
            api_endpoint=api_endpoint,
            slack_webhook_url=slack_webhook_url,
            snowflake_config=snowflake_config,
            control_table_name="INTM.CONVERSION_UPLOAD.BING_REDTRACK_CONTROL",
            platform_name="Bing (BING)",
            conversion_type="purchase_value"
        )
        self.campaign_id = campaign_id

    def create_control_table(self):
        """Create Bing control table"""
        table_schema = """
        CREATE TABLE IF NOT EXISTS INTM.CONVERSION_UPLOAD.BING_REDTRACK_CONTROL (
            DATE STRING,
            POST_3RD_PARTY_CLICKID STRING,
            SENT_TIMESTAMP TIMESTAMP_NTZ,
            STATUS STRING,
            RETRY_COUNT NUMBER,
            RESPONSE_CODE STRING,
            RESPONSE_BODY STRING,
            REQUEST_BODY STRING,
            PRIMARY KEY (DATE, POST_3RD_PARTY_CLICKID)
        )
        """
        return self.create_control_table_if_not_exists(table_schema)

    def check_source_has_data(self, date: str) -> bool:
        """Check if source has ANY Bing data for this date (ignoring control table)"""
        query = """
        WITH CellXpert_ClickID as (
            SELECT
                AFP,
                USERID,
                SPLIT_PART(userid, '/', 2) AS Account_ID,
                Tracking_Code
            FROM RAW.SWEEP.ICT_FTD_REGISTRATION_REPORT
            WHERE tracker_login_id = 4467
            AND tracking_code in ('GOOG', 'BING', 'TWIT')
            GROUP BY ALL
        ),
        MainData as (
            SELECT
                A.Date as UTC_DATE,
                B."3RD_PARTY_CLICKID" as POST_3RD_PARTY_CLICKID,
                SUM(A.deposits) AS PURCHASE_VALUE
            FROM RAW.SWEEP.DYNAMIC_VARIABLES_REPORT A
            JOIN CellXpert_ClickID cx
                ON SPLIT_PART(a.userid, '/', 2) = cx.Account_ID
            JOIN (
                SELECT
                    POST_CLICK_DATE AS DATE,
                    POST_SIGNUP_TIMESTAMP,
                    POST_3RD_PARTY_CLICKID as "3RD_PARTY_CLICKID",
                    POST_CLICKID AS ClickID
                FROM RAW.BRC.POSTBACK_TRACKING pstbk
                LEFT OUTER JOIN RAW.BRC.CAMPAIGN_TRACKERS cmtkr
                    ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
                GROUP BY ALL
            ) AS B
            ON upper(cx.afp) = upper(B.ClickID)
            WHERE
                a.date <= cast(timeadd(hour,2160,b.POST_SIGNUP_TIMESTAMP) as date)
                AND A.tracking_code = 'BING'
                AND A.TRACKER_LOGIN_ID = 4467
                AND a.date = %s
            GROUP BY ALL
            HAVING SUM(A.deposits) > 0
        )
        SELECT COUNT(*) as cnt FROM MainData
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (date,))
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            logger.info(f"Bing source data check for {date}: {count} records found")
            return count > 0
        except Exception as e:
            logger.error(f"Error checking Bing source data for {date}: {e}")
            return True  # Safe default: assume data exists

    def get_unsent_data(self, date: str) -> List[Dict[str, Any]]:
        """Get unsent Bing FTD data"""
        query = """
        WITH CellXpert_ClickID as (
            SELECT
                AFP,
                USERID,
                SPLIT_PART(userid, '/', 2) AS Account_ID,
                Tracking_Code
            FROM RAW.SWEEP.ICT_FTD_REGISTRATION_REPORT
            WHERE tracker_login_id = 4467
            AND tracking_code in ('GOOG', 'BING', 'TWIT')
            GROUP BY ALL
        ),
        MainData as (
            SELECT
                A.Date as UTC_DATE,
                B."3RD_PARTY_CLICKID" as POST_3RD_PARTY_CLICKID,
                B.Bing_ClickID,
                SUM(A.deposits) AS PURCHASE_VALUE,
                cx.AFP
            FROM RAW.SWEEP.DYNAMIC_VARIABLES_REPORT A
            JOIN CellXpert_ClickID cx
                ON SPLIT_PART(a.userid, '/', 2) = cx.Account_ID
            JOIN (
                SELECT
                    POST_CLICK_DATE AS DATE,
                    POST_SIGNUP_DATE,
                    POST_SIGNUP_TIMESTAMP,
                    substr(POST_CLICK_DATE,1,4)||substr(POST_CLICK_DATE,6,2) AS MonthYear,
                    ADVE_NAME AS Advertiser_Name,
                    POST_3RD_PARTY_CLICKID as "3RD_PARTY_CLICKID",
                    POST_CLICKID AS ClickID,
                    POST_GCLID,
                    POST_IP AS IP_ADDRESS,
                    POEV_TWCLID AS Twitter_ClickID,
                    POEV_MSCLKID AS Bing_ClickID,
                    SUM(CASE WHEN POST_FTD_DATE IS NOT NULL THEN 1 ELSE 0 END) AS FTD_Cnt,
                    SUM(CASE WHEN POST_SIGNUP_DATE IS NOT NULL THEN 1 ELSE 0 END) AS SignUp_Cnt
                FROM RAW.BRC.POSTBACK_TRACKING pstbk
                LEFT OUTER JOIN RAW.BRC.POSTBACK_EXTRA_VARIABLES ev
                    ON pstbk.POST_CLICKID = ev.POEV_CLICKID
                LEFT OUTER JOIN RAW.BRC.CAMPAIGN_TRACKERS cmtkr
                    ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
                LEFT OUTER JOIN RAW.BRC.CAMPAIGNS a
                    ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
                LEFT OUTER JOIN RAW.BRC.BRANDS b
                    ON a.CAMP_FK_BRAND = b.BRAN_ID
                LEFT OUTER JOIN RAW.BRC.TRACKER_LOGINS trk
                    ON cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
                LEFT OUTER JOIN RAW.BRC.PUBLISHERS pub
                    ON trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
                LEFT OUTER JOIN RAW.BRC.ADVERTISERS adv
                    ON trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
                GROUP BY ALL
            ) AS B
            ON upper(cx.afp) = upper(B.ClickID)
            WHERE
                a.date <= cast(timeadd(hour,2160,b.POST_SIGNUP_TIMESTAMP) as date)
                AND A.tracking_code = 'BING'
                AND A.TRACKER_LOGIN_ID = 4467
                AND a.date = %s
            GROUP BY ALL
            HAVING SUM(A.deposits) > 0
        )
        SELECT
            UTC_DATE,
            POST_3RD_PARTY_CLICKID,
            Bing_ClickID,
            PURCHASE_VALUE,
            AFP
        FROM MainData
        WHERE NOT EXISTS (
            SELECT 1
            FROM INTM.CONVERSION_UPLOAD.BING_REDTRACK_CONTROL C
            WHERE C.DATE = MainData.UTC_DATE
            AND C.POST_3RD_PARTY_CLICKID = MainData.POST_3RD_PARTY_CLICKID
            AND C.STATUS = 'SUCCESS'
        )
        ORDER BY UTC_DATE DESC
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (date,))
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()

        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Retrieved {len(data)} rows from Snowflake")
        return data

    def format_api_payload(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format Bing API payload"""
        utc_date = row['UTC_DATE']

        if isinstance(utc_date, str):
            dt = datetime.strptime(utc_date, "%Y-%m-%d")
        else:
            dt = datetime.combine(utc_date, datetime.min.time())

        dt = dt.replace(
            hour=random.randint(10, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )

        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        payload = [{
            "campaign_id": self.campaign_id,
            "clickid": row['POST_3RD_PARTY_CLICKID'],
            "created_at": iso_timestamp,
            "payout": float(row['PURCHASE_VALUE']),
            "type": "purchase_value"
        }]

        return payload

    def calculate_total_amount(self, data: List[Dict[str, Any]]) -> float:
        """Calculate total purchase value"""
        return sum(float(row['PURCHASE_VALUE']) for row in data)

    def record_result(self, row: Dict[str, Any], status: str, result: Dict[str, Any]):
        """Record Bing result"""
        query = """
        MERGE INTO INTM.CONVERSION_UPLOAD.BING_REDTRACK_CONTROL AS target
        USING (SELECT
            %s AS DATE,
            %s AS POST_3RD_PARTY_CLICKID,
            CURRENT_TIMESTAMP() AS SENT_TIMESTAMP,
            %s AS STATUS,
            %s AS RETRY_COUNT,
            %s AS RESPONSE_CODE,
            %s AS RESPONSE_BODY,
            %s AS REQUEST_BODY
        ) AS source
        ON target.DATE = source.DATE AND target.POST_3RD_PARTY_CLICKID = source.POST_3RD_PARTY_CLICKID
        WHEN MATCHED THEN
            UPDATE SET
                target.SENT_TIMESTAMP = source.SENT_TIMESTAMP,
                target.STATUS = source.STATUS,
                target.RETRY_COUNT = source.RETRY_COUNT,
                target.RESPONSE_CODE = source.RESPONSE_CODE,
                target.RESPONSE_BODY = source.RESPONSE_BODY,
                target.REQUEST_BODY = source.REQUEST_BODY
        WHEN NOT MATCHED THEN
            INSERT (DATE, POST_3RD_PARTY_CLICKID, SENT_TIMESTAMP, STATUS, RETRY_COUNT, RESPONSE_CODE, RESPONSE_BODY, REQUEST_BODY)
            VALUES (source.DATE, source.POST_3RD_PARTY_CLICKID, source.SENT_TIMESTAMP, source.STATUS, source.RETRY_COUNT, source.RESPONSE_CODE, source.RESPONSE_BODY, source.REQUEST_BODY)
        """

        response_body = str(result.get('response_body', ''))[:1000]
        request_body = result.get('request_body', '')
        cursor = self.conn.cursor()
        cursor.execute(query, (
            row['UTC_DATE'],
            row['POST_3RD_PARTY_CLICKID'],
            status,
            result.get('retry_count', 0),
            result.get('status_code', ''),
            response_body,
            request_body
        ))
        cursor.close()


class GooglePurchaseValueUpload(ConversionUploadBase):
    """Google purchase_value conversion upload to Redtrack"""

    def __init__(self, api_endpoint: str, slack_webhook_url: str, snowflake_config: Dict[str, str], campaign_id: str):
        super().__init__(
            api_endpoint=api_endpoint,
            slack_webhook_url=slack_webhook_url,
            snowflake_config=snowflake_config,
            control_table_name="INTM.CONVERSION_UPLOAD.GOOGLE_PURCHASE_VALUE_REDTRACK_CONTROL",
            platform_name="Google (GOOG)",
            conversion_type="purchase_value"
        )
        self.campaign_id = campaign_id

    def create_control_table(self):
        """Create Google purchase_value control table"""
        table_schema = """
        CREATE TABLE IF NOT EXISTS INTM.CONVERSION_UPLOAD.GOOGLE_PURCHASE_VALUE_REDTRACK_CONTROL (
            DATE STRING,
            POST_3RD_PARTY_CLICKID STRING,
            SENT_TIMESTAMP TIMESTAMP_NTZ,
            STATUS STRING,
            RETRY_COUNT NUMBER,
            RESPONSE_CODE STRING,
            RESPONSE_BODY STRING,
            DEPOSIT_AMOUNT NUMBER(18,2),
            REQUEST_BODY STRING,
            PRIMARY KEY (DATE, POST_3RD_PARTY_CLICKID)
        )
        """
        return self.create_control_table_if_not_exists(table_schema)

    def check_source_has_data(self, date: str) -> bool:
        """Check if source has ANY Google purchase_value data for this date (ignoring control table)"""
        query = """
        WITH CellXpert_ClickID as (
            SELECT AFP, USERID, SPLIT_PART(userid, '/', 2) AS Account_ID, Tracking_Code
            FROM RAW.SWEEP.ICT_FTD_REGISTRATION_REPORT
            WHERE tracker_login_id = 4467 AND tracking_code in ('GOOG', 'BING', 'TWIT')
            GROUP BY ALL
        ),
        MainData as (
            SELECT A.Date as DATE, B."3RD_PARTY_CLICKID" as POST_3RD_PARTY_CLICKID, SUM(A.deposits) AS DEPOSITS_SUM
            FROM RAW.SWEEP.DYNAMIC_VARIABLES_REPORT A
            JOIN CellXpert_ClickID cx ON SPLIT_PART(a.userid, '/', 2) = cx.Account_ID
            JOIN (
                SELECT POST_CLICK_DATE AS DATE, POST_SIGNUP_TIMESTAMP, POST_3RD_PARTY_CLICKID as "3RD_PARTY_CLICKID", POST_CLICKID AS ClickID
                FROM RAW.BRC.POSTBACK_TRACKING pstbk
                LEFT OUTER JOIN RAW.BRC.CAMPAIGN_TRACKERS cmtkr ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
                GROUP BY ALL
            ) AS B ON upper(cx.afp) = upper(B.ClickID)
            WHERE a.date <= cast(timeadd(hour,2160,b.POST_SIGNUP_TIMESTAMP) as date)
                AND A.tracking_code = 'GOOG' AND A.TRACKER_LOGIN_ID = 4467
                AND B."3RD_PARTY_CLICKID" like '6%%' AND a.date = %s
            GROUP BY ALL HAVING SUM(A.deposits) > 0
        )
        SELECT COUNT(*) as cnt FROM MainData
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (date,))
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            logger.info(f"Google purchase_value source data check for {date}: {count} records found")
            return count > 0
        except Exception as e:
            logger.error(f"Error checking Google purchase_value source data for {date}: {e}")
            return True  # Safe default: assume data exists

    def get_unsent_data(self, date: str) -> List[Dict[str, Any]]:
        """Get unsent Google purchase_value data"""
        query = """
        WITH CellXpert_ClickID as (
            SELECT
                AFP,
                USERID,
                SPLIT_PART(userid, '/', 2) AS Account_ID,
                Tracking_Code
            FROM RAW.SWEEP.ICT_FTD_REGISTRATION_REPORT
            WHERE tracker_login_id = 4467
            AND tracking_code in ('GOOG', 'BING', 'TWIT')
            GROUP BY ALL
        ),
        MainData as (
            SELECT
                A.Date as DATE,
                B."3RD_PARTY_CLICKID" as POST_3RD_PARTY_CLICKID,
                SUM(A.deposits) AS DEPOSITS_SUM,
                cx.AFP
            FROM RAW.SWEEP.DYNAMIC_VARIABLES_REPORT A
            JOIN CellXpert_ClickID cx
                ON SPLIT_PART(a.userid, '/', 2) = cx.Account_ID
            JOIN (
                SELECT
                    POST_CLICK_DATE AS DATE,
                    POST_SIGNUP_DATE,
                    POST_SIGNUP_TIMESTAMP,
                    substr(POST_CLICK_DATE,1,4)||substr(POST_CLICK_DATE,6,2) AS MonthYear,
                    ADVE_NAME AS Advertiser_Name,
                    POST_3RD_PARTY_CLICKID as "3RD_PARTY_CLICKID",
                    POST_CLICKID AS ClickID,
                    POST_GCLID,
                    POST_IP AS IP_ADDRESS,
                    POEV_TWCLID AS Twitter_ClickID,
                    POEV_MSCLKID AS Bing_ClickID,
                    SUM(CASE WHEN POST_FTD_DATE IS NOT NULL THEN 1 ELSE 0 END) AS FTD_Cnt,
                    SUM(CASE WHEN POST_SIGNUP_DATE IS NOT NULL THEN 1 ELSE 0 END) AS SignUp_Cnt
                FROM RAW.BRC.POSTBACK_TRACKING pstbk
                LEFT OUTER JOIN RAW.BRC.POSTBACK_EXTRA_VARIABLES ev
                    ON pstbk.POST_CLICKID = ev.POEV_CLICKID
                LEFT OUTER JOIN RAW.BRC.CAMPAIGN_TRACKERS cmtkr
                    ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
                LEFT OUTER JOIN RAW.BRC.CAMPAIGNS a
                    ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
                LEFT OUTER JOIN RAW.BRC.BRANDS b
                    ON a.CAMP_FK_BRAND = b.BRAN_ID
                LEFT OUTER JOIN RAW.BRC.TRACKER_LOGINS trk
                    ON cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
                LEFT OUTER JOIN RAW.BRC.PUBLISHERS pub
                    ON trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
                LEFT OUTER JOIN RAW.BRC.ADVERTISERS adv
                    ON trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
                GROUP BY ALL
            ) AS B
            ON upper(cx.afp) = upper(B.ClickID)
            WHERE
                a.date <= cast(timeadd(hour,2160,b.POST_SIGNUP_TIMESTAMP) as date)
                AND A.tracking_code = 'GOOG'
                AND A.TRACKER_LOGIN_ID = 4467
                AND B."3RD_PARTY_CLICKID" like '6%%'
                AND a.date = %s
            GROUP BY ALL
            HAVING SUM(A.deposits) > 0
        )
        SELECT
            DATE,
            DEPOSITS_SUM,
            AFP,
            POST_3RD_PARTY_CLICKID
        FROM MainData
        WHERE NOT EXISTS (
            SELECT 1
            FROM INTM.CONVERSION_UPLOAD.GOOGLE_PURCHASE_VALUE_REDTRACK_CONTROL C
            WHERE C.DATE = MainData.DATE
            AND C.POST_3RD_PARTY_CLICKID = MainData.POST_3RD_PARTY_CLICKID
            AND C.STATUS = 'SUCCESS'
        )
        ORDER BY DATE DESC
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (date,))
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()

        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Retrieved {len(data)} rows from Snowflake")
        return data

    def format_api_payload(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format Google API payload"""
        date_str = row['DATE']

        if isinstance(date_str, str):
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        else:
            dt = datetime.combine(date_str, datetime.min.time())

        dt = dt.replace(
            hour=random.randint(10, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )

        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        payload = [{
            "campaign_id": self.campaign_id,
            "clickid": row['POST_3RD_PARTY_CLICKID'],
            "created_at": iso_timestamp,
            "payout": float(row['DEPOSITS_SUM']),
            "type": "purchase_value"
        }]

        return payload

    def calculate_total_amount(self, data: List[Dict[str, Any]]) -> float:
        """Calculate total deposits"""
        return sum(float(row['DEPOSITS_SUM']) for row in data)

    def record_result(self, row: Dict[str, Any], status: str, result: Dict[str, Any]):
        """Record Google result"""
        query = """
        MERGE INTO INTM.CONVERSION_UPLOAD.GOOGLE_PURCHASE_VALUE_REDTRACK_CONTROL AS target
        USING (SELECT
            %s AS DATE,
            %s AS POST_3RD_PARTY_CLICKID,
            CURRENT_TIMESTAMP() AS SENT_TIMESTAMP,
            %s AS STATUS,
            %s AS RETRY_COUNT,
            %s AS RESPONSE_CODE,
            %s AS RESPONSE_BODY,
            %s AS DEPOSIT_AMOUNT,
            %s AS REQUEST_BODY
        ) AS source
        ON target.DATE = source.DATE AND target.POST_3RD_PARTY_CLICKID = source.POST_3RD_PARTY_CLICKID
        WHEN MATCHED THEN
            UPDATE SET
                target.SENT_TIMESTAMP = source.SENT_TIMESTAMP,
                target.STATUS = source.STATUS,
                target.RETRY_COUNT = source.RETRY_COUNT,
                target.RESPONSE_CODE = source.RESPONSE_CODE,
                target.RESPONSE_BODY = source.RESPONSE_BODY,
                target.DEPOSIT_AMOUNT = COALESCE(source.DEPOSIT_AMOUNT, target.DEPOSIT_AMOUNT),
                target.REQUEST_BODY = source.REQUEST_BODY
        WHEN NOT MATCHED THEN
            INSERT (DATE, POST_3RD_PARTY_CLICKID, SENT_TIMESTAMP, STATUS, RETRY_COUNT, RESPONSE_CODE, RESPONSE_BODY, DEPOSIT_AMOUNT, REQUEST_BODY)
            VALUES (source.DATE, source.POST_3RD_PARTY_CLICKID, source.SENT_TIMESTAMP, source.STATUS, source.RETRY_COUNT, source.RESPONSE_CODE, source.RESPONSE_BODY, source.DEPOSIT_AMOUNT, source.REQUEST_BODY)
        """

        response_body = str(result.get('response_body', ''))[:1000]
        request_body = result.get('request_body', '')
        cursor = self.conn.cursor()
        cursor.execute(query, (
            row['DATE'],
            row['POST_3RD_PARTY_CLICKID'],
            status,
            result.get('retry_count', 0),
            result.get('status_code', ''),
            response_body,
            float(row['DEPOSITS_SUM']),
            request_body
        ))
        cursor.close()


class GooglePurchaseValueReportingUpload(ConversionUploadBase):
    """Google purchase_value_reporting conversion upload to Redtrack"""

    def __init__(self, api_endpoint: str, slack_webhook_url: str, snowflake_config: Dict[str, str], campaign_id: str):
        super().__init__(
            api_endpoint=api_endpoint,
            slack_webhook_url=slack_webhook_url,
            snowflake_config=snowflake_config,
            control_table_name="INTM.CONVERSION_UPLOAD.GOOGLE_PURCHASE_VALUE_REPORTING_REDTRACK_CONTROL",
            platform_name="Google (GOOG)",
            conversion_type="purchase_value_reporting"
        )
        self.campaign_id = campaign_id

    def create_control_table(self):
        """Create Google purchase_value_reporting control table"""
        table_schema = """
        CREATE TABLE IF NOT EXISTS INTM.CONVERSION_UPLOAD.GOOGLE_PURCHASE_VALUE_REPORTING_REDTRACK_CONTROL (
            DATE STRING,
            POST_3RD_PARTY_CLICKID STRING,
            SENT_TIMESTAMP TIMESTAMP_NTZ,
            STATUS STRING,
            RETRY_COUNT NUMBER,
            RESPONSE_CODE STRING,
            RESPONSE_BODY STRING,
            DEPOSIT_AMOUNT NUMBER(18,2),
            REQUEST_BODY STRING,
            PRIMARY KEY (DATE, POST_3RD_PARTY_CLICKID)
        )
        """
        return self.create_control_table_if_not_exists(table_schema)

    def check_source_has_data(self, date: str) -> bool:
        """Check if source has ANY Google purchase_value_reporting data for this date (ignoring control table)"""
        query = """
        WITH CellXpert_ClickID as (
            SELECT AFP, USERID, SPLIT_PART(userid, '/', 2) AS Account_ID, Tracking_Code
            FROM RAW.SWEEP.ICT_FTD_REGISTRATION_REPORT
            WHERE tracker_login_id = 4467 AND tracking_code in ('GOOG', 'BING', 'TWIT')
            GROUP BY ALL
        ),
        MainData as (
            SELECT A.Date as DATE, B."3RD_PARTY_CLICKID" as POST_3RD_PARTY_CLICKID, SUM(A.deposits) AS DEPOSITS_SUM
            FROM RAW.SWEEP.DYNAMIC_VARIABLES_REPORT A
            JOIN CellXpert_ClickID cx ON SPLIT_PART(a.userid, '/', 2) = cx.Account_ID
            JOIN (
                SELECT POST_CLICK_DATE AS DATE, POST_SIGNUP_TIMESTAMP, POST_3RD_PARTY_CLICKID as "3RD_PARTY_CLICKID", POST_CLICKID AS ClickID
                FROM RAW.BRC.POSTBACK_TRACKING pstbk
                LEFT OUTER JOIN RAW.BRC.CAMPAIGN_TRACKERS cmtkr ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
                GROUP BY ALL
            ) AS B ON upper(cx.afp) = upper(B.ClickID)
            WHERE a.date <= cast(timeadd(hour,2160,b.POST_SIGNUP_TIMESTAMP) as date)
                AND A.tracking_code = 'GOOG' AND A.TRACKER_LOGIN_ID = 4467
                AND B."3RD_PARTY_CLICKID" like '6%%' AND a.date = %s
            GROUP BY ALL HAVING SUM(A.deposits) > 0
        )
        SELECT COUNT(*) as cnt FROM MainData
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (date,))
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            logger.info(f"Google purchase_value_reporting source data check for {date}: {count} records found")
            return count > 0
        except Exception as e:
            logger.error(f"Error checking Google purchase_value_reporting source data for {date}: {e}")
            return True  # Safe default: assume data exists

    def get_unsent_data(self, date: str) -> List[Dict[str, Any]]:
        """Get unsent Google purchase_value_reporting data"""
        # Same query as Google purchase_value
        query = """
        WITH CellXpert_ClickID as (
            SELECT
                AFP,
                USERID,
                SPLIT_PART(userid, '/', 2) AS Account_ID,
                Tracking_Code
            FROM RAW.SWEEP.ICT_FTD_REGISTRATION_REPORT
            WHERE tracker_login_id = 4467
            AND tracking_code in ('GOOG', 'BING', 'TWIT')
            GROUP BY ALL
        ),
        MainData as (
            SELECT
                A.Date as DATE,
                B."3RD_PARTY_CLICKID" as POST_3RD_PARTY_CLICKID,
                SUM(A.deposits) AS DEPOSITS_SUM,
                cx.AFP
            FROM RAW.SWEEP.DYNAMIC_VARIABLES_REPORT A
            JOIN CellXpert_ClickID cx
                ON SPLIT_PART(a.userid, '/', 2) = cx.Account_ID
            JOIN (
                SELECT
                    POST_CLICK_DATE AS DATE,
                    POST_SIGNUP_DATE,
                    POST_SIGNUP_TIMESTAMP,
                    substr(POST_CLICK_DATE,1,4)||substr(POST_CLICK_DATE,6,2) AS MonthYear,
                    ADVE_NAME AS Advertiser_Name,
                    POST_3RD_PARTY_CLICKID as "3RD_PARTY_CLICKID",
                    POST_CLICKID AS ClickID,
                    POST_GCLID,
                    POST_IP AS IP_ADDRESS,
                    POEV_TWCLID AS Twitter_ClickID,
                    POEV_MSCLKID AS Bing_ClickID,
                    SUM(CASE WHEN POST_FTD_DATE IS NOT NULL THEN 1 ELSE 0 END) AS FTD_Cnt,
                    SUM(CASE WHEN POST_SIGNUP_DATE IS NOT NULL THEN 1 ELSE 0 END) AS SignUp_Cnt
                FROM RAW.BRC.POSTBACK_TRACKING pstbk
                LEFT OUTER JOIN RAW.BRC.POSTBACK_EXTRA_VARIABLES ev
                    ON pstbk.POST_CLICKID = ev.POEV_CLICKID
                LEFT OUTER JOIN RAW.BRC.CAMPAIGN_TRACKERS cmtkr
                    ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
                LEFT OUTER JOIN RAW.BRC.CAMPAIGNS a
                    ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
                LEFT OUTER JOIN RAW.BRC.BRANDS b
                    ON a.CAMP_FK_BRAND = b.BRAN_ID
                LEFT OUTER JOIN RAW.BRC.TRACKER_LOGINS trk
                    ON cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
                LEFT OUTER JOIN RAW.BRC.PUBLISHERS pub
                    ON trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
                LEFT OUTER JOIN RAW.BRC.ADVERTISERS adv
                    ON trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
                GROUP BY ALL
            ) AS B
            ON upper(cx.afp) = upper(B.ClickID)
            WHERE
                a.date <= cast(timeadd(hour,2160,b.POST_SIGNUP_TIMESTAMP) as date)
                AND A.tracking_code = 'GOOG'
                AND A.TRACKER_LOGIN_ID = 4467
                AND B."3RD_PARTY_CLICKID" like '6%%'
                AND a.date = %s
            GROUP BY ALL
            HAVING SUM(A.deposits) > 0
        )
        SELECT
            DATE,
            DEPOSITS_SUM,
            AFP,
            POST_3RD_PARTY_CLICKID
        FROM MainData
        WHERE NOT EXISTS (
            SELECT 1
            FROM INTM.CONVERSION_UPLOAD.GOOGLE_PURCHASE_VALUE_REPORTING_REDTRACK_CONTROL C
            WHERE C.DATE = MainData.DATE
            AND C.POST_3RD_PARTY_CLICKID = MainData.POST_3RD_PARTY_CLICKID
            AND C.STATUS = 'SUCCESS'
        )
        ORDER BY DATE DESC
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (date,))
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()

        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Retrieved {len(data)} rows from Snowflake")
        return data

    def format_api_payload(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format Google purchase_value_reporting API payload"""
        date_str = row['DATE']

        if isinstance(date_str, str):
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        else:
            dt = datetime.combine(date_str, datetime.min.time())

        dt = dt.replace(
            hour=random.randint(10, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )

        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        payload = [{
            "campaign_id": self.campaign_id,
            "clickid": row['POST_3RD_PARTY_CLICKID'],
            "created_at": iso_timestamp,
            "payout": float(row['DEPOSITS_SUM']),
            "type": "purchase_value_reporting"
        }]

        return payload

    def calculate_total_amount(self, data: List[Dict[str, Any]]) -> float:
        """Calculate total deposits"""
        return sum(float(row['DEPOSITS_SUM']) for row in data)

    def record_result(self, row: Dict[str, Any], status: str, result: Dict[str, Any]):
        """Record Google purchase_value_reporting result"""
        query = """
        MERGE INTO INTM.CONVERSION_UPLOAD.GOOGLE_PURCHASE_VALUE_REPORTING_REDTRACK_CONTROL AS target
        USING (SELECT
            %s AS DATE,
            %s AS POST_3RD_PARTY_CLICKID,
            CURRENT_TIMESTAMP() AS SENT_TIMESTAMP,
            %s AS STATUS,
            %s AS RETRY_COUNT,
            %s AS RESPONSE_CODE,
            %s AS RESPONSE_BODY,
            %s AS DEPOSIT_AMOUNT,
            %s AS REQUEST_BODY
        ) AS source
        ON target.DATE = source.DATE AND target.POST_3RD_PARTY_CLICKID = source.POST_3RD_PARTY_CLICKID
        WHEN MATCHED THEN
            UPDATE SET
                target.SENT_TIMESTAMP = source.SENT_TIMESTAMP,
                target.STATUS = source.STATUS,
                target.RETRY_COUNT = source.RETRY_COUNT,
                target.RESPONSE_CODE = source.RESPONSE_CODE,
                target.RESPONSE_BODY = source.RESPONSE_BODY,
                target.DEPOSIT_AMOUNT = COALESCE(source.DEPOSIT_AMOUNT, target.DEPOSIT_AMOUNT),
                target.REQUEST_BODY = source.REQUEST_BODY
        WHEN NOT MATCHED THEN
            INSERT (DATE, POST_3RD_PARTY_CLICKID, SENT_TIMESTAMP, STATUS, RETRY_COUNT, RESPONSE_CODE, RESPONSE_BODY, DEPOSIT_AMOUNT, REQUEST_BODY)
            VALUES (source.DATE, source.POST_3RD_PARTY_CLICKID, source.SENT_TIMESTAMP, source.STATUS, source.RETRY_COUNT, source.RESPONSE_CODE, source.RESPONSE_BODY, source.DEPOSIT_AMOUNT, source.REQUEST_BODY)
        """

        response_body = str(result.get('response_body', ''))[:1000]
        request_body = result.get('request_body', '')
        cursor = self.conn.cursor()
        cursor.execute(query, (
            row['DATE'],
            row['POST_3RD_PARTY_CLICKID'],
            status,
            result.get('retry_count', 0),
            result.get('status_code', ''),
            response_body,
            float(row['DEPOSITS_SUM']),
            request_body
        ))
        cursor.close()


class RefillsGooglePurchaseValueUpload(ConversionUploadBase):
    """Refills Google purchase_value conversion upload to Redtrack"""

    def __init__(self, api_endpoint: str, slack_webhook_url: str, snowflake_config: Dict[str, str], default_campaign_id: str):
        super().__init__(
            api_endpoint=api_endpoint,
            slack_webhook_url=slack_webhook_url,
            snowflake_config=snowflake_config,
            control_table_name="INTM.CONVERSION_UPLOAD.REFILLS_GOOGLE_PURCHASE_VALUE_REDTRACK_CONTROL",
            platform_name="Refills Google Ads",
            conversion_type="purchase_value"
        )
        self.default_campaign_id = default_campaign_id

    def create_control_table(self):
        """Create Refills control table"""
        table_schema = """
        CREATE TABLE IF NOT EXISTS INTM.CONVERSION_UPLOAD.REFILLS_GOOGLE_PURCHASE_VALUE_REDTRACK_CONTROL (
            CONVERSION_DATE STRING,
            REDTRACK_ID STRING,
            VIEW_CLICKID STRING,
            SENT_TIMESTAMP TIMESTAMP_NTZ,
            STATUS STRING,
            RETRY_COUNT NUMBER,
            RESPONSE_CODE STRING,
            RESPONSE_BODY STRING,
            PURCHASE_VALUE NUMBER(18,2),
            CAMPAIGN STRING,
            CAMPAIGN_ID STRING,
            REQUEST_BODY STRING,
            PRIMARY KEY (CONVERSION_DATE, REDTRACK_ID)
        )
        """
        return self.create_control_table_if_not_exists(table_schema)

    def check_source_has_data(self, date: str) -> bool:
        """Check if source has ANY Refills Google data for this date (ignoring control table)"""
        query = """
        SELECT COUNT(*) as cnt
        FROM EXP.PUBLIC.V_REFILLS_CALLCTR_GOOGLE_CONVERSION_UPLOAD v
        LEFT JOIN (
            SELECT Ref_ID, MAX(ID) as ID
            FROM RAW.REDTRACK.CLICKS
            GROUP BY Ref_ID
        ) c ON v.CLICKID = c.Ref_ID
        WHERE DATE(v.CONVERSION_TIMESTAMP) = %s
        AND c.ID IS NOT NULL
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (date,))
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            logger.info(f"Refills Google source data check for {date}: {count} records found")
            return count > 0
        except Exception as e:
            logger.error(f"Error checking Refills Google source data for {date}: {e}")
            return True  # Safe default: assume data exists

    def get_all_dates_with_unsent_data(self) -> List[str]:
        """Get all dates from the earliest source date until yesterday that have unsent data

        This method finds the earliest conversion date in the source view,
        then returns all dates from that date until yesterday that have unsent data.

        Returns:
            List of date strings (YYYY-MM-DD) ordered from oldest to newest
        """
        # Get the earliest date from source view
        query_earliest_date = """
        SELECT MIN(DATE(CONVERSION_TIMESTAMP)) as EARLIEST_DATE
        FROM EXP.PUBLIC.V_REFILLS_CALLCTR_GOOGLE_CONVERSION_UPLOAD
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(query_earliest_date)
            result = cursor.fetchone()
            cursor.close()

            earliest_date = result[0] if result and result[0] else None

            # Get yesterday's date in PST
            yesterday = get_yesterday_pst()

            if earliest_date:
                # Parse the earliest date
                if isinstance(earliest_date, str):
                    start_date = datetime.strptime(earliest_date, '%Y-%m-%d').date()
                else:
                    start_date = earliest_date

                logger.info(f"Refills Google: Earliest conversion date in source is {start_date}")
            else:
                # No data in source, default to 30 days ago
                pst = pytz.timezone('America/Los_Angeles')
                start_date = (datetime.now(pst) - timedelta(days=30)).date()
                logger.info(f"Refills Google: No data in source view, starting from {start_date}")

            # Parse yesterday
            end_date = datetime.strptime(yesterday, '%Y-%m-%d').date()

            # Generate all dates from start_date to end_date (inclusive)
            dates_to_check = []
            current_date = start_date
            while current_date <= end_date:
                dates_to_check.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)

            logger.info(f"Refills Google: Checking {len(dates_to_check)} dates from {start_date} to {end_date}")

            # Filter to only dates that have unsent data
            dates_with_data = []
            for date in dates_to_check:
                data = self.get_unsent_data(date)
                if data:
                    logger.info(f"Refills Google: Date {date} has {len(data)} unsent records")
                    dates_with_data.append(date)

            logger.info(f"Refills Google: Found {len(dates_with_data)} dates with unsent data")
            return dates_with_data

        except Exception as e:
            logger.error(f"Error getting dates with unsent data for Refills Google: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            # Return empty list on error to avoid processing issues
            return []

    def get_unsent_data(self, date: str) -> List[Dict[str, Any]]:
        """Get unsent Refills data"""
        query = """
        SELECT
            v.CONVERSION_TIMESTAMP,
            v.CLICKID,
            v.PURCHASE_VALUE,
            c.CAMPAIGN,
            c.CAMPAIGN_ID,
            c.ID
        FROM EXP.PUBLIC.V_REFILLS_CALLCTR_GOOGLE_CONVERSION_UPLOAD v
        LEFT JOIN (
            SELECT
                CAMPAIGN,
                CAMPAIGN_ID,
                Ref_ID,
                MAX(ID) as ID
            FROM RAW.REDTRACK.CLICKS
            GROUP BY CAMPAIGN, CAMPAIGN_ID, Ref_ID
        ) c ON v.CLICKID = c.Ref_ID
        WHERE DATE(v.CONVERSION_TIMESTAMP) = %s
        AND c.ID IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM INTM.CONVERSION_UPLOAD.REFILLS_GOOGLE_PURCHASE_VALUE_REDTRACK_CONTROL ctrl
            WHERE ctrl.CONVERSION_DATE = %s
            AND ctrl.REDTRACK_ID = c.ID
            AND ctrl.STATUS = 'SUCCESS'
        )
        ORDER BY v.CONVERSION_TIMESTAMP DESC
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (date, date))
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()

        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Retrieved {len(data)} rows from Snowflake")
        return data

    def format_api_payload(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format Refills API payload"""
        redtrack_id = row.get('ID')
        if not redtrack_id:
            raise ValueError(f"No RedTrack ID found for clickid {row['CLICKID']}")

        conversion_timestamp = row['CONVERSION_TIMESTAMP']

        if isinstance(conversion_timestamp, str):
            dt = datetime.strptime(conversion_timestamp, "%Y-%m-%d %H:%M:%S.%f")
        elif isinstance(conversion_timestamp, datetime):
            dt = conversion_timestamp
        else:
            dt = datetime.combine(conversion_timestamp, datetime.min.time())

        # Set time to 23:59:00
        dt = dt.replace(hour=23, minute=59, second=0, microsecond=0)

        # Add 3 hours
        dt = dt + timedelta(hours=3)

        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        campaign_id = row.get('CAMPAIGN_ID') or self.default_campaign_id

        payload = [{
            "campaign_id": campaign_id,
            "clickid": redtrack_id,
            "created_at": iso_timestamp,
            "payout": float(row['PURCHASE_VALUE'] or 0),
            "type": "purchase_value"
        }]

        return payload

    def calculate_total_amount(self, data: List[Dict[str, Any]]) -> float:
        """Calculate total purchase value"""
        return sum(float(row['PURCHASE_VALUE'] or 0) for row in data)

    def record_result(self, row: Dict[str, Any], status: str, result: Dict[str, Any]):
        """Record Refills result"""
        query = """
        MERGE INTO INTM.CONVERSION_UPLOAD.REFILLS_GOOGLE_PURCHASE_VALUE_REDTRACK_CONTROL AS target
        USING (SELECT
            %s AS CONVERSION_DATE,
            %s AS REDTRACK_ID,
            %s AS VIEW_CLICKID,
            CURRENT_TIMESTAMP() AS SENT_TIMESTAMP,
            %s AS STATUS,
            %s AS RETRY_COUNT,
            %s AS RESPONSE_CODE,
            %s AS RESPONSE_BODY,
            %s AS PURCHASE_VALUE,
            %s AS CAMPAIGN,
            %s AS CAMPAIGN_ID,
            %s AS REQUEST_BODY
        ) AS source
        ON target.CONVERSION_DATE = source.CONVERSION_DATE AND target.REDTRACK_ID = source.REDTRACK_ID
        WHEN MATCHED THEN
            UPDATE SET
                target.SENT_TIMESTAMP = source.SENT_TIMESTAMP,
                target.STATUS = source.STATUS,
                target.RETRY_COUNT = source.RETRY_COUNT,
                target.RESPONSE_CODE = source.RESPONSE_CODE,
                target.RESPONSE_BODY = source.RESPONSE_BODY,
                target.VIEW_CLICKID = COALESCE(source.VIEW_CLICKID, target.VIEW_CLICKID),
                target.PURCHASE_VALUE = COALESCE(source.PURCHASE_VALUE, target.PURCHASE_VALUE),
                target.CAMPAIGN = COALESCE(source.CAMPAIGN, target.CAMPAIGN),
                target.CAMPAIGN_ID = COALESCE(source.CAMPAIGN_ID, target.CAMPAIGN_ID),
                target.REQUEST_BODY = source.REQUEST_BODY
        WHEN NOT MATCHED THEN
            INSERT (CONVERSION_DATE, REDTRACK_ID, VIEW_CLICKID, SENT_TIMESTAMP, STATUS, RETRY_COUNT, RESPONSE_CODE, RESPONSE_BODY, PURCHASE_VALUE, CAMPAIGN, CAMPAIGN_ID, REQUEST_BODY)
            VALUES (source.CONVERSION_DATE, source.REDTRACK_ID, source.VIEW_CLICKID, source.SENT_TIMESTAMP, source.STATUS, source.RETRY_COUNT, source.RESPONSE_CODE, source.RESPONSE_BODY, source.PURCHASE_VALUE, source.CAMPAIGN, source.CAMPAIGN_ID, source.REQUEST_BODY)
        """

        response_body = str(result.get('response_body', ''))[:1000]
        request_body = result.get('request_body', '')

        # Get conversion date from timestamp
        conversion_timestamp = row['CONVERSION_TIMESTAMP']
        if isinstance(conversion_timestamp, str):
            conversion_date = conversion_timestamp.split()[0]
        elif isinstance(conversion_timestamp, datetime):
            conversion_date = conversion_timestamp.strftime("%Y-%m-%d")
        else:
            conversion_date = str(conversion_timestamp)

        cursor = self.conn.cursor()
        cursor.execute(query, (
            conversion_date,
            row.get('ID'),
            row.get('CLICKID'),
            status,
            result.get('retry_count', 0),
            result.get('status_code', ''),
            response_body,
            float(row['PURCHASE_VALUE'] or 0),
            row.get('CAMPAIGN', 'N/A'),
            row.get('CAMPAIGN_ID', self.default_campaign_id),
            request_body
        ))
        cursor.close()


# Helper function to get yesterday's date in PST
class RefillsFacebookPurchaseValueUpload(ConversionUploadBase):
    """Refills Facebook purchase_value conversion upload to Redtrack"""

    def __init__(self, api_endpoint: str, slack_webhook_url: str, snowflake_config: Dict[str, str], default_campaign_id: str):
        super().__init__(
            api_endpoint=api_endpoint,
            slack_webhook_url=slack_webhook_url,
            snowflake_config=snowflake_config,
            control_table_name="INTM.CONVERSION_UPLOAD.REFILLS_FACEBOOK_PURCHASE_VALUE_REDTRACK_CONTROL",
            platform_name="Refills Facebook",
            conversion_type="purchase_value"
        )
        self.default_campaign_id = default_campaign_id

    def create_control_table(self):
        """Create Refills Facebook control table"""
        table_schema = """
        CREATE TABLE IF NOT EXISTS INTM.CONVERSION_UPLOAD.REFILLS_FACEBOOK_PURCHASE_VALUE_REDTRACK_CONTROL (
            CONVERSION_DATE STRING,
            REDTRACK_ID STRING,
            VIEW_CLICKID STRING,
            SENT_TIMESTAMP TIMESTAMP_NTZ,
            STATUS STRING,
            RETRY_COUNT NUMBER,
            RESPONSE_CODE STRING,
            RESPONSE_BODY STRING,
            PURCHASE_VALUE NUMBER(18,2),
            CAMPAIGN STRING,
            CAMPAIGN_ID STRING,
            REQUEST_BODY STRING,
            PRIMARY KEY (CONVERSION_DATE, REDTRACK_ID)
        )
        """
        return self.create_control_table_if_not_exists(table_schema)

    def check_source_has_data(self, date: str) -> bool:
        """Check if source has ANY Refills Facebook data for this date (ignoring control table)"""
        query = """
        SELECT COUNT(*) as cnt
        FROM EXP.PUBLIC.V_REFILLS_CALLCTR_FACEBOOK_CONVERSION_UPLOAD v
        WHERE DATE(v.CONVERSION_TIMESTAMP) = %s
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (date,))
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            logger.info(f"Refills Facebook source data check for {date}: {count} records found")
            return count > 0
        except Exception as e:
            logger.error(f"Error checking Refills Facebook source data for {date}: {e}")
            return True  # Safe default: assume data exists

    def get_all_dates_with_unsent_data(self) -> List[str]:
        """Get all dates from the earliest source date until yesterday that have unsent data

        This method finds the earliest conversion date in the source view,
        then returns all dates from that date until yesterday that have unsent data.

        Returns:
            List of date strings (YYYY-MM-DD) ordered from oldest to newest
        """
        # Get the earliest date from source view
        query_earliest_date = """
        SELECT MIN(DATE(CONVERSION_TIMESTAMP)) as EARLIEST_DATE
        FROM EXP.PUBLIC.V_REFILLS_CALLCTR_FACEBOOK_CONVERSION_UPLOAD
        """

        try:
            cursor = self.conn.cursor()
            cursor.execute(query_earliest_date)
            result = cursor.fetchone()
            cursor.close()

            earliest_date = result[0] if result and result[0] else None

            # Get yesterday's date in PST
            yesterday = get_yesterday_pst()

            if earliest_date:
                # Parse the earliest date
                if isinstance(earliest_date, str):
                    start_date = datetime.strptime(earliest_date, '%Y-%m-%d').date()
                else:
                    start_date = earliest_date

                logger.info(f"Refills Facebook: Earliest conversion date in source is {start_date}")
            else:
                # No data in source, default to 30 days ago
                pst = pytz.timezone('America/Los_Angeles')
                start_date = (datetime.now(pst) - timedelta(days=30)).date()
                logger.info(f"Refills Facebook: No data in source view, starting from {start_date}")

            # Parse yesterday
            end_date = datetime.strptime(yesterday, '%Y-%m-%d').date()

            # Generate all dates from start_date to end_date (inclusive)
            dates_to_check = []
            current_date = start_date
            while current_date <= end_date:
                dates_to_check.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)

            logger.info(f"Refills Facebook: Checking {len(dates_to_check)} dates from {start_date} to {end_date}")

            # Filter to only dates that have unsent data
            dates_with_data = []
            for date in dates_to_check:
                data = self.get_unsent_data(date)
                if data:
                    logger.info(f"Refills Facebook: Date {date} has {len(data)} unsent records")
                    dates_with_data.append(date)

            logger.info(f"Refills Facebook: Found {len(dates_with_data)} dates with unsent data")
            return dates_with_data

        except Exception as e:
            logger.error(f"Error getting dates with unsent data for Refills Facebook: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            # Return empty list on error to avoid processing issues
            return []

    def get_unsent_data(self, date: str) -> List[Dict[str, Any]]:
        """Get unsent Refills Facebook data"""
        query = """
        SELECT
            v.CONVERSION_TIMESTAMP,
            v.CLICKID,
            v.PURCHASE_VALUE,
            c.CAMPAIGN,
            c.CAMPAIGN_ID,
            c.ID
        FROM EXP.PUBLIC.V_REFILLS_CALLCTR_FACEBOOK_CONVERSION_UPLOAD v
        LEFT JOIN (
            SELECT
                ID,
                MAX(CAMPAIGN) as CAMPAIGN,
                MAX(CAMPAIGN_ID) as CAMPAIGN_ID
            FROM RAW.REDTRACK.CLICKS
            GROUP BY ID
        ) c ON v.CLICKID = c.ID
        WHERE DATE(v.CONVERSION_TIMESTAMP) = %s
        AND NOT EXISTS (
            SELECT 1
            FROM INTM.CONVERSION_UPLOAD.REFILLS_FACEBOOK_PURCHASE_VALUE_REDTRACK_CONTROL ctrl
            WHERE ctrl.CONVERSION_DATE = %s
            AND ctrl.REDTRACK_ID = c.ID
            AND ctrl.STATUS = 'SUCCESS'
        )
        ORDER BY v.CONVERSION_TIMESTAMP ASC
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (date, date))
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()

        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Retrieved {len(data)} rows from Snowflake")
        return data

    def format_api_payload(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format Refills Facebook API payload"""
        redtrack_id = row.get('ID')
        if not redtrack_id:
            raise ValueError(f"No RedTrack ID found for clickid {row['CLICKID']}")

        conversion_timestamp = row['CONVERSION_TIMESTAMP']

        if isinstance(conversion_timestamp, str):
            dt = datetime.strptime(conversion_timestamp, "%Y-%m-%d %H:%M:%S.%f")
        elif isinstance(conversion_timestamp, datetime):
            dt = conversion_timestamp
        else:
            dt = datetime.combine(conversion_timestamp, datetime.min.time())

        # Set time to 23:59:00
        dt = dt.replace(hour=23, minute=59, second=0, microsecond=0)

        # Add 3 hours
        dt = dt + timedelta(hours=3)

        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        campaign_id = row.get('CAMPAIGN_ID') or self.default_campaign_id

        payload = [{
            "campaign_id": campaign_id,
            "clickid": redtrack_id,
            "created_at": iso_timestamp,
            "payout": float(row['PURCHASE_VALUE'] or 0),
            "type": "purchase_value"
        }]

        return payload

    def calculate_total_amount(self, data: List[Dict[str, Any]]) -> float:
        """Calculate total purchase value"""
        return sum(float(row['PURCHASE_VALUE'] or 0) for row in data)

    def record_result(self, row: Dict[str, Any], status: str, result: Dict[str, Any]):
        """Record Refills Facebook result"""
        query = """
        MERGE INTO INTM.CONVERSION_UPLOAD.REFILLS_FACEBOOK_PURCHASE_VALUE_REDTRACK_CONTROL AS target
        USING (SELECT
            %s AS CONVERSION_DATE,
            %s AS REDTRACK_ID,
            %s AS VIEW_CLICKID,
            CURRENT_TIMESTAMP() AS SENT_TIMESTAMP,
            %s AS STATUS,
            %s AS RETRY_COUNT,
            %s AS RESPONSE_CODE,
            %s AS RESPONSE_BODY,
            %s AS PURCHASE_VALUE,
            %s AS CAMPAIGN,
            %s AS CAMPAIGN_ID,
            %s AS REQUEST_BODY
        ) AS source
        ON target.CONVERSION_DATE = source.CONVERSION_DATE AND target.REDTRACK_ID = source.REDTRACK_ID
        WHEN MATCHED THEN
            UPDATE SET
                target.SENT_TIMESTAMP = source.SENT_TIMESTAMP,
                target.STATUS = source.STATUS,
                target.RETRY_COUNT = source.RETRY_COUNT,
                target.RESPONSE_CODE = source.RESPONSE_CODE,
                target.RESPONSE_BODY = source.RESPONSE_BODY,
                target.VIEW_CLICKID = COALESCE(source.VIEW_CLICKID, target.VIEW_CLICKID),
                target.PURCHASE_VALUE = COALESCE(source.PURCHASE_VALUE, target.PURCHASE_VALUE),
                target.CAMPAIGN = COALESCE(source.CAMPAIGN, target.CAMPAIGN),
                target.CAMPAIGN_ID = COALESCE(source.CAMPAIGN_ID, target.CAMPAIGN_ID),
                target.REQUEST_BODY = source.REQUEST_BODY
        WHEN NOT MATCHED THEN
            INSERT (CONVERSION_DATE, REDTRACK_ID, VIEW_CLICKID, SENT_TIMESTAMP, STATUS, RETRY_COUNT, RESPONSE_CODE, RESPONSE_BODY, PURCHASE_VALUE, CAMPAIGN, CAMPAIGN_ID, REQUEST_BODY)
            VALUES (source.CONVERSION_DATE, source.REDTRACK_ID, source.VIEW_CLICKID, source.SENT_TIMESTAMP, source.STATUS, source.RETRY_COUNT, source.RESPONSE_CODE, source.RESPONSE_BODY, source.PURCHASE_VALUE, source.CAMPAIGN, source.CAMPAIGN_ID, source.REQUEST_BODY)
        """

        response_body = str(result.get('response_body', ''))[:1000]
        request_body = result.get('request_body', '')

        # Get conversion date from timestamp
        conversion_timestamp = row['CONVERSION_TIMESTAMP']
        if isinstance(conversion_timestamp, str):
            conversion_date = conversion_timestamp.split()[0]
        elif isinstance(conversion_timestamp, datetime):
            conversion_date = conversion_timestamp.strftime("%Y-%m-%d")
        else:
            conversion_date = str(conversion_timestamp)

        cursor = self.conn.cursor()
        cursor.execute(query, (
            conversion_date,
            row.get('ID'),
            row.get('CLICKID'),
            status,
            result.get('retry_count', 0),
            result.get('status_code', ''),
            response_body,
            float(row['PURCHASE_VALUE'] or 0),
            row.get('CAMPAIGN', 'N/A'),
            row.get('CAMPAIGN_ID', self.default_campaign_id),
            request_body
        ))
        cursor.close()


class TaboolaPurchaseValueUpload(ConversionUploadBase):
    """Taboola purchase_value conversion upload to Redtrack (AWS Snowflake)"""

    def __init__(self, api_endpoint: str, slack_webhook_url: str, snowflake_config: Dict[str, str], campaign_id: str):
        super().__init__(
            api_endpoint=api_endpoint,
            slack_webhook_url=slack_webhook_url,
            snowflake_config=snowflake_config,
            control_table_name="INTM.CONVERSION_UPLOAD.TABOOLA_REDTRACK_CONTROL",
            platform_name="Taboola (TAB)",
            conversion_type="purchase_value"
        )
        self.campaign_id = campaign_id

    def create_control_table(self):
        """Create Taboola control table"""
        table_schema = """
        CREATE TABLE IF NOT EXISTS INTM.CONVERSION_UPLOAD.TABOOLA_REDTRACK_CONTROL (
            DATE STRING,
            POST_3RD_PARTY_CLICKID STRING,
            SENT_TIMESTAMP TIMESTAMP_NTZ,
            STATUS STRING,
            RETRY_COUNT NUMBER,
            RESPONSE_CODE STRING,
            RESPONSE_BODY STRING,
            REQUEST_BODY STRING,
            PRIMARY KEY (DATE, POST_3RD_PARTY_CLICKID)
        )
        """
        return self.create_control_table_if_not_exists(table_schema)

    def check_source_has_data(self, date: str) -> bool:
        """Check if source has ANY Taboola data for this date (ignoring control table)"""
        query = """
        SELECT COUNT(*) as cnt FROM (
            SELECT B.POST_FTD_DATE as Date, A.afp, B.POST_3RD_PARTY_CLICKID, SUM(A.FIRST_DEPOSIT) AS First_Deposits
            FROM RAW.SWEEP.ICT_FTD_REGISTRATION_REPORT A
            JOIN (
                SELECT POST_FTD_DATE, POST_CLICKID AS ClickID, POST_3RD_PARTY_CLICKID
                FROM RAW.BRC.POSTBACK_TRACKING pstbk
                LEFT OUTER JOIN RAW.BRC.CAMPAIGN_TRACKERS cmtkr ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
                GROUP BY 1,2,3
            ) AS B ON A.afp = B.ClickID
            WHERE A.tracking_code = 'TAB' AND A.TRACKER_LOGIN_ID = 4467 AND B.POST_3RD_PARTY_CLICKID IS NOT NULL
            AND B.POST_FTD_DATE = %s
            GROUP BY 1,2,3 HAVING SUM(A.FIRST_DEPOSIT) > 0
        )
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (date,))
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            logger.info(f"Taboola source data check for {date}: {count} records found")
            return count > 0
        except Exception as e:
            logger.error(f"Error checking Taboola source data for {date}: {e}")
            return True  # Safe default: assume data exists

    def get_unsent_data(self, date: str) -> List[Dict[str, Any]]:
        """Get unsent Taboola FTD data from AWS Snowflake"""
        query = """
        SELECT
          A.DATE,
          A.AFP,
          A.POST_3RD_PARTY_CLICKID,
          A.first_deposit_time,
          A.FIRST_DEPOSITS
        FROM (
        SELECT
            B.POST_FTD_TIMESTAMP as first_deposit_time,
            B.POST_FTD_DATE as Date,
            A.afp,
            B.POST_3RD_PARTY_CLICKID,
            SUM(A.FIRST_DEPOSIT) AS First_Deposits
          FROM RAW.SWEEP.ICT_FTD_REGISTRATION_REPORT A
          JOIN (
            SELECT
              POST_CLICK_DATE AS DATE,
              POST_FTD_DATE,
              POST_FTD_TIMESTAMP,
              POST_SIGNUP_DATE,
              POST_SIGNUP_TIMESTAMP,
              substr(POST_CLICK_DATE,1,4)||substr(POST_CLICK_DATE,6,2) AS MonthYear,
              ADVE_NAME AS Advertiser_Name,
              CAMP_NAME AS Campaign_Name,
              POST_CLICKID AS ClickID,
              POST_3RD_PARTY_CLICKID,
              POEV_MSCLKID AS BING_CLICKID,
              SUM(CASE WHEN POST_FTD_DATE IS NOT NULL THEN 1 ELSE 0 END) AS FTD_Cnt,
              SUM(CASE WHEN POST_SIGNUP_DATE IS NOT NULL THEN 1 ELSE 0 END) AS SignUp_Cnt
            FROM RAW.BRC.POSTBACK_TRACKING pstbk
            LEFT OUTER JOIN RAW.BRC.POSTBACK_EXTRA_VARIABLES ev
              ON upper(pstbk.POST_CLICKID) = upper(ev.POEV_CLICKID)
            LEFT OUTER JOIN RAW.BRC.CAMPAIGN_TRACKERS cmtkr
              ON pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
            LEFT OUTER JOIN RAW.BRC.CAMPAIGNS a
              ON cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
            LEFT OUTER JOIN RAW.BRC.BRANDS b
              ON a.CAMP_FK_BRAND = b.BRAN_ID
            LEFT OUTER JOIN RAW.BRC.TRACKER_LOGINS trk
              ON cmtkr.CAMT_FK_LOGIN = trk.TLOG_ID
            LEFT OUTER JOIN RAW.BRC.PUBLISHERS pub
              ON trk.TLOG_FK_PUBLISHER = pub.PUBL_ID
            LEFT OUTER JOIN RAW.BRC.ADVERTISERS adv
              ON trk.TLOG_FK_ADVERTISER = adv.ADVE_ID
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
          ) AS B
          ON A.afp = B.ClickID
          WHERE 1=1 AND
            A.tracking_code = 'TAB' AND
            A.TRACKER_LOGIN_ID = 4467 AND
            B.POST_3RD_PARTY_CLICKID IS NOT NULL
          GROUP BY
          1,2,3,4
          HAVING SUM(A.FIRST_DEPOSIT) > 0
          ORDER BY 1
          ) A
          WHERE DATE = %s
          AND NOT EXISTS (
            SELECT 1
            FROM INTM.CONVERSION_UPLOAD.TABOOLA_REDTRACK_CONTROL C
            WHERE C.DATE = A.DATE
            AND C.POST_3RD_PARTY_CLICKID = A.POST_3RD_PARTY_CLICKID
            AND C.STATUS = 'SUCCESS'
          )
          group by 1,2,3,4,5
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (date,))
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()

        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Retrieved {len(data)} rows from Snowflake")
        return data

    def format_api_payload(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format Taboola API payload"""
        original_timestamp = row['FIRST_DEPOSIT_TIME']

        # Parse the timestamp
        if isinstance(original_timestamp, str):
            try:
                dt = datetime.strptime(original_timestamp, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(original_timestamp, "%Y-%m-%d %H:%M:%S")
        else:
            dt = datetime.combine(original_timestamp, datetime.min.time())

        # Adjust hour if less than 10
        if dt.hour < 10:
            dt = dt.replace(hour=dt.hour + 10)

        # Format to ISO8601 with Z suffix
        if dt.microsecond > 0:
            iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"

        payload = [{
            "campaign_id": self.campaign_id,
            "clickid": row['POST_3RD_PARTY_CLICKID'],
            "created_at": iso_timestamp,
            "payout": float(row['FIRST_DEPOSITS']),
            "type": "purchase_value"
        }]

        return payload

    def calculate_total_amount(self, data: List[Dict[str, Any]]) -> float:
        """Calculate total first deposits"""
        return sum(float(row['FIRST_DEPOSITS']) for row in data)

    def record_result(self, row: Dict[str, Any], status: str, result: Dict[str, Any]):
        """Record Taboola result"""
        query = """
        MERGE INTO INTM.CONVERSION_UPLOAD.TABOOLA_REDTRACK_CONTROL AS target
        USING (SELECT
            %s AS DATE,
            %s AS POST_3RD_PARTY_CLICKID,
            CURRENT_TIMESTAMP() AS SENT_TIMESTAMP,
            %s AS STATUS,
            %s AS RETRY_COUNT,
            %s AS RESPONSE_CODE,
            %s AS RESPONSE_BODY,
            %s AS REQUEST_BODY
        ) AS source
        ON target.DATE = source.DATE AND target.POST_3RD_PARTY_CLICKID = source.POST_3RD_PARTY_CLICKID
        WHEN MATCHED THEN
            UPDATE SET
                target.SENT_TIMESTAMP = source.SENT_TIMESTAMP,
                target.STATUS = source.STATUS,
                target.RETRY_COUNT = source.RETRY_COUNT,
                target.RESPONSE_CODE = source.RESPONSE_CODE,
                target.RESPONSE_BODY = source.RESPONSE_BODY,
                target.REQUEST_BODY = source.REQUEST_BODY
        WHEN NOT MATCHED THEN
            INSERT (DATE, POST_3RD_PARTY_CLICKID, SENT_TIMESTAMP, STATUS, RETRY_COUNT, RESPONSE_CODE, RESPONSE_BODY, REQUEST_BODY)
            VALUES (source.DATE, source.POST_3RD_PARTY_CLICKID, source.SENT_TIMESTAMP, source.STATUS, source.RETRY_COUNT, source.RESPONSE_CODE, source.RESPONSE_BODY, source.REQUEST_BODY)
        """

        response_body = str(result.get('response_body', ''))[:1000]
        request_body = result.get('request_body', '')
        cursor = self.conn.cursor()
        cursor.execute(query, (
            row['DATE'],
            row['POST_3RD_PARTY_CLICKID'],
            status,
            result.get('retry_count', 0),
            result.get('status_code', ''),
            response_body,
            request_body
        ))
        cursor.close()


class GoogleHighValueUpload(ConversionUploadBase):
    """Google high_value conversion upload to Redtrack"""

    def __init__(self, api_endpoint: str, slack_webhook_url: str, snowflake_config: Dict[str, str], campaign_id: str):
        super().__init__(
            api_endpoint=api_endpoint,
            slack_webhook_url=slack_webhook_url,
            snowflake_config=snowflake_config,
            control_table_name="INTM.CONVERSION_UPLOAD.GOOGLE_HIGH_VALUE_REDTRACK_CONTROL",
            platform_name="Google Ads",
            conversion_type="high_value"
        )
        self.campaign_id = campaign_id

    def create_control_table(self):
        """Create Google high_value control table"""
        table_schema = """
        CREATE TABLE IF NOT EXISTS INTM.CONVERSION_UPLOAD.GOOGLE_HIGH_VALUE_REDTRACK_CONTROL (
            CLICKID STRING,
            CONVERSION_TIMESTAMP TIMESTAMP_NTZ,
            HIGH_VALUE NUMBER,
            SENT_CONVERSION_TIMESTAMP STRING,
            REQUEST_BODY STRING,
            SENT_TIMESTAMP TIMESTAMP_NTZ,
            STATUS STRING,
            RETRY_COUNT NUMBER,
            RESPONSE_CODE STRING,
            RESPONSE_BODY STRING,
            PRIMARY KEY (CLICKID, CONVERSION_TIMESTAMP)
        )
        """
        return self.create_control_table_if_not_exists(table_schema)

    def check_source_has_data(self, date: str) -> bool:
        """Check if source has ANY Google high_value data for this date (ignoring control table)"""
        query = """
        SELECT COUNT(*) as cnt
        FROM EXP.SPREE.SPREE_HIGH_VALUE
        WHERE DATE(CONVERSION_TIMESTAMP) = %s
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (date,))
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            logger.info(f"Google high_value source data check for {date}: {count} records found")
            return count > 0
        except Exception as e:
            logger.error(f"Error checking Google high_value source data for {date}: {e}")
            return True  # Safe default: assume data exists

    def get_unsent_data(self, date: str) -> List[Dict[str, Any]]:
        """Get unsent Google high_value data"""
        query = """
        SELECT
            CLICKID,
            CONVERSION_TIMESTAMP,
            HIGH_VALUE
        FROM EXP.SPREE.SPREE_HIGH_VALUE
        WHERE DATE(CONVERSION_TIMESTAMP) = %s
        AND NOT EXISTS (
            SELECT 1
            FROM INTM.CONVERSION_UPLOAD.GOOGLE_HIGH_VALUE_REDTRACK_CONTROL C
            WHERE C.CLICKID = EXP.SPREE.SPREE_HIGH_VALUE.CLICKID
            AND C.CONVERSION_TIMESTAMP = EXP.SPREE.SPREE_HIGH_VALUE.CONVERSION_TIMESTAMP
            AND C.STATUS = 'SUCCESS'
        )
        ORDER BY CONVERSION_TIMESTAMP DESC
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (date,))
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()

        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Retrieved {len(data)} rows from Snowflake")
        return data

    def format_api_payload(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format Google high_value API payload"""
        conversion_timestamp = row['CONVERSION_TIMESTAMP']

        # Handle timestamp conversion
        if isinstance(conversion_timestamp, str):
            if '.' in conversion_timestamp:
                dt = datetime.strptime(conversion_timestamp, "%Y-%m-%d %H:%M:%S.%f")
            else:
                dt = datetime.strptime(conversion_timestamp, "%Y-%m-%d %H:%M:%S")
        else:
            dt = conversion_timestamp

        # Only adjust if hour < 10, keep minute and second intact
        if dt.hour < 10:
            random_hour = random.randint(10, 23)
            dt = dt.replace(hour=random_hour)

        # Format to ISO8601 format with Z suffix for UTC
        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        payload = [{
            "campaign_id": self.campaign_id,
            "clickid": row['CLICKID'],
            "created_at": iso_timestamp,
            "payout": float(row['HIGH_VALUE']),
            "type": "high_value"
        }]

        return payload

    def calculate_total_amount(self, data: List[Dict[str, Any]]) -> float:
        """Calculate total high value"""
        return sum(float(row['HIGH_VALUE']) for row in data)

    def record_result(self, row: Dict[str, Any], status: str, result: Dict[str, Any]):
        """Record Google high_value result"""
        # Get the sent conversion timestamp from the payload
        payload = self.format_api_payload(row)
        sent_conversion_timestamp = payload[0]['created_at']

        query = """
        MERGE INTO INTM.CONVERSION_UPLOAD.GOOGLE_HIGH_VALUE_REDTRACK_CONTROL AS target
        USING (SELECT
            %s AS CLICKID,
            %s AS CONVERSION_TIMESTAMP,
            %s AS HIGH_VALUE,
            %s AS SENT_CONVERSION_TIMESTAMP,
            %s AS REQUEST_BODY,
            CURRENT_TIMESTAMP() AS SENT_TIMESTAMP,
            %s AS STATUS,
            %s AS RETRY_COUNT,
            %s AS RESPONSE_CODE,
            %s AS RESPONSE_BODY
        ) AS source
        ON target.CLICKID = source.CLICKID AND target.CONVERSION_TIMESTAMP = source.CONVERSION_TIMESTAMP
        WHEN MATCHED THEN
            UPDATE SET
                target.HIGH_VALUE = source.HIGH_VALUE,
                target.SENT_CONVERSION_TIMESTAMP = source.SENT_CONVERSION_TIMESTAMP,
                target.REQUEST_BODY = source.REQUEST_BODY,
                target.SENT_TIMESTAMP = source.SENT_TIMESTAMP,
                target.STATUS = source.STATUS,
                target.RETRY_COUNT = source.RETRY_COUNT,
                target.RESPONSE_CODE = source.RESPONSE_CODE,
                target.RESPONSE_BODY = source.RESPONSE_BODY
        WHEN NOT MATCHED THEN
            INSERT (CLICKID, CONVERSION_TIMESTAMP, HIGH_VALUE, SENT_CONVERSION_TIMESTAMP, REQUEST_BODY, SENT_TIMESTAMP, STATUS, RETRY_COUNT, RESPONSE_CODE, RESPONSE_BODY)
            VALUES (source.CLICKID, source.CONVERSION_TIMESTAMP, source.HIGH_VALUE, source.SENT_CONVERSION_TIMESTAMP, source.REQUEST_BODY, source.SENT_TIMESTAMP, source.STATUS, source.RETRY_COUNT, source.RESPONSE_CODE, source.RESPONSE_BODY)
        """

        response_body = str(result.get('response_body', ''))[:1000]
        request_body = result.get('request_body', '')
        cursor = self.conn.cursor()
        cursor.execute(query, (
            row['CLICKID'],
            row['CONVERSION_TIMESTAMP'],
            float(row['HIGH_VALUE']),
            sent_conversion_timestamp,
            request_body,
            status,
            result.get('retry_count', 0),
            result.get('status_code', ''),
            response_body
        ))
        cursor.close()


class BingHighValueUpload(ConversionUploadBase):
    """Bing high_value conversion upload to Redtrack"""

    def __init__(self, api_endpoint: str, slack_webhook_url: str, snowflake_config: Dict[str, str], campaign_id: str):
        super().__init__(
            api_endpoint=api_endpoint,
            slack_webhook_url=slack_webhook_url,
            snowflake_config=snowflake_config,
            control_table_name="INTM.CONVERSION_UPLOAD.BING_HIGH_VALUE_REDTRACK_CONTROL",
            platform_name="Bing Ads",
            conversion_type="high_value"
        )
        self.campaign_id = campaign_id

    def create_control_table(self):
        """Create Bing high_value control table"""
        table_schema = """
        CREATE TABLE IF NOT EXISTS INTM.CONVERSION_UPLOAD.BING_HIGH_VALUE_REDTRACK_CONTROL (
            CONVERSION_DATE STRING,
            REDTRACK_ID STRING,
            VIEW_CLICKID STRING,
            SENT_TIMESTAMP TIMESTAMP_NTZ,
            STATUS STRING,
            RETRY_COUNT NUMBER,
            RESPONSE_CODE STRING,
            RESPONSE_BODY STRING,
            HIGH_VALUE NUMBER(18,2),
            CAMPAIGN STRING,
            CAMPAIGN_ID STRING,
            REQUEST_BODY STRING,
            SENT_CONVERSION_TIMESTAMP STRING,
            PRIMARY KEY (CONVERSION_DATE, REDTRACK_ID)
        )
        """
        return self.create_control_table_if_not_exists(table_schema)

    def check_source_has_data(self, date: str) -> bool:
        """Check if source has ANY Bing high_value data for this date (ignoring control table)"""
        query = """
        SELECT COUNT(*) as cnt
        FROM EXP.SPREE.SPREE_HIGH_VALUE_BING
        WHERE DATE(CONVERSION_TIMESTAMP) = %s
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (date,))
            result = cursor.fetchone()
            cursor.close()
            count = result[0] if result else 0
            logger.info(f"Bing high_value source data check for {date}: {count} records found")
            return count > 0
        except Exception as e:
            logger.error(f"Error checking Bing high_value source data for {date}: {e}")
            return True  # Safe default: assume data exists

    def get_unsent_data(self, date: str) -> List[Dict[str, Any]]:
        """Get unsent Bing high_value data"""
        query = """
        SELECT
            CLICKID,
            CONVERSION_TIMESTAMP,
            HIGH_VALUE
        FROM EXP.SPREE.SPREE_HIGH_VALUE_BING
        WHERE DATE(CONVERSION_TIMESTAMP) = %s
        AND NOT EXISTS (
            SELECT 1
            FROM INTM.CONVERSION_UPLOAD.BING_HIGH_VALUE_REDTRACK_CONTROL C
            WHERE C.VIEW_CLICKID = EXP.SPREE.SPREE_HIGH_VALUE_BING.CLICKID
            AND C.CONVERSION_DATE = %s
            AND C.STATUS = 'SUCCESS'
        )
        ORDER BY CONVERSION_TIMESTAMP ASC
        """

        cursor = self.conn.cursor()
        cursor.execute(query, (date, date))
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()

        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Retrieved {len(data)} rows from Snowflake")
        return data

    def format_api_payload(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format Bing high_value API payload"""
        clickid = row.get('CLICKID')
        if not clickid:
            raise ValueError(f"No CLICKID found in row")

        conversion_timestamp = row['CONVERSION_TIMESTAMP']

        # Handle timestamp conversion
        if isinstance(conversion_timestamp, str):
            if '.' in conversion_timestamp:
                dt = datetime.strptime(conversion_timestamp, "%Y-%m-%d %H:%M:%S.%f")
            else:
                dt = datetime.strptime(conversion_timestamp, "%Y-%m-%d %H:%M:%S")
        else:
            dt = conversion_timestamp

        # Only adjust if hour < 10, keep minute and second intact
        if dt.hour < 10:
            random_hour = random.randint(10, 23)
            dt = dt.replace(hour=random_hour)

        # Format to ISO8601 format with Z suffix for UTC
        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        payload = [{
            "campaign_id": self.campaign_id,
            "clickid": clickid,
            "created_at": iso_timestamp,
            "payout": float(row['HIGH_VALUE']),
            "type": "high_value"
        }]

        return payload

    def calculate_total_amount(self, data: List[Dict[str, Any]]) -> float:
        """Calculate total high value"""
        return sum(float(row['HIGH_VALUE']) for row in data)

    def record_result(self, row: Dict[str, Any], status: str, result: Dict[str, Any]):
        """Record Bing high_value result"""
        # Get the sent conversion timestamp from the payload
        payload = self.format_api_payload(row)
        sent_conversion_timestamp = payload[0]['created_at']

        # Get conversion date from timestamp
        conversion_timestamp = row['CONVERSION_TIMESTAMP']
        if isinstance(conversion_timestamp, str):
            conversion_date = conversion_timestamp.split()[0]
        elif isinstance(conversion_timestamp, datetime):
            conversion_date = conversion_timestamp.strftime("%Y-%m-%d")
        else:
            conversion_date = str(conversion_timestamp)

        query = """
        MERGE INTO INTM.CONVERSION_UPLOAD.BING_HIGH_VALUE_REDTRACK_CONTROL AS target
        USING (SELECT
            %s AS CONVERSION_DATE,
            %s AS REDTRACK_ID,
            %s AS VIEW_CLICKID,
            CURRENT_TIMESTAMP() AS SENT_TIMESTAMP,
            %s AS STATUS,
            %s AS RETRY_COUNT,
            %s AS RESPONSE_CODE,
            %s AS RESPONSE_BODY,
            %s AS HIGH_VALUE,
            %s AS CAMPAIGN,
            %s AS CAMPAIGN_ID,
            %s AS REQUEST_BODY,
            %s AS SENT_CONVERSION_TIMESTAMP
        ) AS source
        ON target.CONVERSION_DATE = source.CONVERSION_DATE AND target.REDTRACK_ID = source.REDTRACK_ID
        WHEN MATCHED THEN
            UPDATE SET
                target.SENT_TIMESTAMP = source.SENT_TIMESTAMP,
                target.STATUS = source.STATUS,
                target.RETRY_COUNT = source.RETRY_COUNT,
                target.RESPONSE_CODE = source.RESPONSE_CODE,
                target.RESPONSE_BODY = source.RESPONSE_BODY,
                target.VIEW_CLICKID = COALESCE(source.VIEW_CLICKID, target.VIEW_CLICKID),
                target.HIGH_VALUE = COALESCE(source.HIGH_VALUE, target.HIGH_VALUE),
                target.CAMPAIGN = COALESCE(source.CAMPAIGN, target.CAMPAIGN),
                target.CAMPAIGN_ID = COALESCE(source.CAMPAIGN_ID, target.CAMPAIGN_ID),
                target.REQUEST_BODY = COALESCE(source.REQUEST_BODY, target.REQUEST_BODY),
                target.SENT_CONVERSION_TIMESTAMP = COALESCE(source.SENT_CONVERSION_TIMESTAMP, target.SENT_CONVERSION_TIMESTAMP)
        WHEN NOT MATCHED THEN
            INSERT (CONVERSION_DATE, REDTRACK_ID, VIEW_CLICKID, SENT_TIMESTAMP, STATUS, RETRY_COUNT, RESPONSE_CODE, RESPONSE_BODY, HIGH_VALUE, CAMPAIGN, CAMPAIGN_ID, REQUEST_BODY, SENT_CONVERSION_TIMESTAMP)
            VALUES (source.CONVERSION_DATE, source.REDTRACK_ID, source.VIEW_CLICKID, source.SENT_TIMESTAMP, source.STATUS, source.RETRY_COUNT, source.RESPONSE_CODE, source.RESPONSE_BODY, source.HIGH_VALUE, source.CAMPAIGN, source.CAMPAIGN_ID, source.REQUEST_BODY, source.SENT_CONVERSION_TIMESTAMP)
        """

        response_body = str(result.get('response_body', ''))[:1000]
        request_body = result.get('request_body', '')
        cursor = self.conn.cursor()
        cursor.execute(query, (
            conversion_date,
            row.get('CLICKID'),
            row.get('CLICKID'),
            status,
            result.get('retry_count', 0),
            result.get('status_code', ''),
            response_body,
            float(row['HIGH_VALUE']),
            None,  # No campaign info
            self.campaign_id,
            request_body,
            sent_conversion_timestamp
        ))
        cursor.close()


# Helper function to get yesterday's date in PST
def get_yesterday_pst() -> str:
    """Get yesterday's date in PST timezone"""
    pst = pytz.timezone('America/Los_Angeles')
    pst_now = datetime.now(pytz.UTC).astimezone(pst)
    yesterday = (pst_now - timedelta(days=1)).strftime("%Y-%m-%d")
    return yesterday


def get_last_n_days_excluding_yesterday_pst(n_days: int = 7) -> List[str]:
    """Get last N days in PST timezone (excluding only today)

    This is used for backfill checks. Returns the last N days including yesterday.
    Yesterday is also processed separately in Step 2, but we include it here for backfill
    checking because if yesterday has no data, the backfill will find 0 records and skip it,
    then Step 2 will also find 0 records and process nothing (idempotent).

    Args:
        n_days: Number of days to look back (default 7, includes yesterday)

    Returns:
        List of date strings in YYYY-MM-DD format, ordered from oldest to newest

    Example:
        If today is 2025-11-07 and n_days=7:
        Returns [2025-10-31, 2025-11-01, 2025-11-02, 2025-11-03, 2025-11-04, 2025-11-05, 2025-11-06]
        (7 dates: day-7 through day-1/yesterday, excluding only today day-0)
    """
    pst = pytz.timezone('America/Los_Angeles')
    pst_now = datetime.now(pytz.UTC).astimezone(pst)

    dates = []
    # Start from n_days ago and go to yesterday (1 day ago), excluding only today
    for i in range(n_days, 0, -1):  # e.g., if n_days=7: range(7, 0, -1) = [7,6,5,4,3,2,1]
        date = (pst_now - timedelta(days=i)).strftime("%Y-%m-%d")
        dates.append(date)

    return dates


# Airflow task functions
def upload_bing_purchase_value(**context):
    """Upload Bing purchase_value conversions (with automatic backfill for last 7 days)"""
    api_key = Variable.get("redtrack_api_key")
    api_endpoint = f"https://api.redtrack.io/conversions?api_key={api_key}"
    slack_webhook = Variable.get("slack_webhook_conversion_upload")

    snowflake_config = {
        'user': Variable.get("snowflake_user"),
        'password': Variable.get("snowflake_password"),
        'account': Variable.get("snowflake_account"),
        'warehouse': 'REPORTING',
        'database': 'INTM',
        'schema': 'CONVERSION_UPLOAD',
        'role': 'ACCOUNTADMIN'
    }

    campaign_id = Variable.get("bing_campaign_id")

    uploader = BingPurchaseValueUpload(api_endpoint, slack_webhook, snowflake_config, campaign_id)

    # Connect once for checking missing dates
    uploader.connect_to_snowflake()
    uploader.create_control_table()

    try:
        # Get yesterday's date (this is what we always process as "daily run")
        yesterday = get_yesterday_pst()

        # Step 1: Find and process any missing dates from last 7 days (excluding yesterday)
        # Yesterday will be processed in Step 2 as the regular daily run
        missing_dates = uploader.get_missing_dates_from_last_n_days(n_days=7)

        # Remove yesterday from missing_dates to avoid processing it twice
        missing_dates_without_yesterday = [d for d in missing_dates if d != yesterday]

        if missing_dates_without_yesterday:
            logger.info(f"Found {len(missing_dates_without_yesterday)} missing dates to process: {missing_dates_without_yesterday}")
            for missing_date in missing_dates_without_yesterday:
                logger.info(f"Processing missing date: {missing_date}")
                uploader.process_conversions(missing_date, is_backfill=True)
        else:
            logger.info("No missing dates found in last 7 days (excluding yesterday)")

        # Step 2: Process yesterday's data (the regular daily run)
        # Always process yesterday as "Daily Run", regardless of whether it has data or not
        logger.info(f"Processing yesterday ({yesterday}) as daily run")
        uploader.process_conversions(yesterday, is_backfill=False)

    finally:
        uploader.close_connection()


def upload_google_purchase_value(**context):
    """Upload Google purchase_value conversions (with automatic backfill for last 7 days)"""
    api_key = Variable.get("redtrack_api_key")
    api_endpoint = f"https://api.redtrack.io/conversions?api_key={api_key}"
    slack_webhook = Variable.get("slack_webhook_conversion_upload")

    snowflake_config = {
        'user': Variable.get("snowflake_user"),
        'password': Variable.get("snowflake_password"),
        'account': Variable.get("snowflake_account"),
        'warehouse': 'REPORTING',
        'database': 'INTM',
        'schema': 'CONVERSION_UPLOAD',
        'role': 'ACCOUNTADMIN'
    }

    campaign_id = Variable.get("google_campaign_id")

    uploader = GooglePurchaseValueUpload(api_endpoint, slack_webhook, snowflake_config, campaign_id)

    # Connect once for checking missing dates
    uploader.connect_to_snowflake()
    uploader.create_control_table()

    try:
        # Get yesterday's date (this is what we always process as "daily run")
        yesterday = get_yesterday_pst()

        # Step 1: Find and process any missing dates from last 7 days (excluding yesterday)
        # Yesterday will be processed in Step 2 as the regular daily run
        missing_dates = uploader.get_missing_dates_from_last_n_days(n_days=7)

        # Remove yesterday from missing_dates to avoid processing it twice
        missing_dates_without_yesterday = [d for d in missing_dates if d != yesterday]

        if missing_dates_without_yesterday:
            logger.info(f"Found {len(missing_dates_without_yesterday)} missing dates to process: {missing_dates_without_yesterday}")
            for missing_date in missing_dates_without_yesterday:
                logger.info(f"Processing missing date: {missing_date}")
                uploader.process_conversions(missing_date, is_backfill=True)
        else:
            logger.info("No missing dates found in last 7 days (excluding yesterday)")

        # Step 2: Process yesterday's data (the regular daily run)
        # Always process yesterday as "Daily Run", regardless of whether it has data or not
        logger.info(f"Processing yesterday ({yesterday}) as daily run")
        uploader.process_conversions(yesterday, is_backfill=False)

    finally:
        uploader.close_connection()


def upload_google_purchase_value_reporting(**context):
    """Upload Google purchase_value_reporting conversions (with automatic backfill for last 7 days)"""
    api_key = Variable.get("redtrack_api_key")
    api_endpoint = f"https://api.redtrack.io/conversions?api_key={api_key}"
    slack_webhook = Variable.get("slack_webhook_conversion_upload")

    snowflake_config = {
        'user': Variable.get("snowflake_user"),
        'password': Variable.get("snowflake_password"),
        'account': Variable.get("snowflake_account"),
        'warehouse': 'REPORTING',
        'database': 'INTM',
        'schema': 'CONVERSION_UPLOAD',
        'role': 'ACCOUNTADMIN'
    }

    campaign_id = Variable.get("google_campaign_id")

    uploader = GooglePurchaseValueReportingUpload(api_endpoint, slack_webhook, snowflake_config, campaign_id)

    # Connect once for checking missing dates
    uploader.connect_to_snowflake()
    uploader.create_control_table()

    try:
        # Get yesterday's date (this is what we always process as "daily run")
        yesterday = get_yesterday_pst()

        # Step 1: Find and process any missing dates from last 7 days (excluding yesterday)
        # Yesterday will be processed in Step 2 as the regular daily run
        missing_dates = uploader.get_missing_dates_from_last_n_days(n_days=7)

        # Remove yesterday from missing_dates to avoid processing it twice
        missing_dates_without_yesterday = [d for d in missing_dates if d != yesterday]

        if missing_dates_without_yesterday:
            logger.info(f"Found {len(missing_dates_without_yesterday)} missing dates to process: {missing_dates_without_yesterday}")
            for missing_date in missing_dates_without_yesterday:
                logger.info(f"Processing missing date: {missing_date}")
                uploader.process_conversions(missing_date, is_backfill=True)
        else:
            logger.info("No missing dates found in last 7 days (excluding yesterday)")

        # Step 2: Process yesterday's data (the regular daily run)
        # Always process yesterday as "Daily Run", regardless of whether it has data or not
        logger.info(f"Processing yesterday ({yesterday}) as daily run")
        uploader.process_conversions(yesterday, is_backfill=False)

    finally:
        uploader.close_connection()


def upload_refills_google_purchase_value(**context):
    """Upload Refills Google purchase_value conversions (with unlimited lookback from last upload date)"""
    api_key = Variable.get("redtrack_api_key")
    api_endpoint = f"https://api.redtrack.io/conversions?api_key={api_key}"
    slack_webhook = Variable.get("slack_webhook_conversion_upload")

    snowflake_config = {
        'user': Variable.get("snowflake_user"),
        'password': Variable.get("snowflake_password"),
        'account': Variable.get("snowflake_account"),
        'warehouse': 'REPORTING',
        'database': 'INTM',
        'schema': 'CONVERSION_UPLOAD',
        'role': 'ACCOUNTADMIN'
    }

    default_campaign_id = Variable.get("refills_default_campaign_id")

    uploader = RefillsGooglePurchaseValueUpload(api_endpoint, slack_webhook, snowflake_config, default_campaign_id)

    # Connect once for checking missing dates
    uploader.connect_to_snowflake()
    uploader.create_control_table()

    try:
        # Get yesterday's date (this is what we always process as "daily run")
        yesterday = get_yesterday_pst()

        # Step 1: Find and process all dates with unsent data (from earliest source date to yesterday)
        # This checks from the earliest date in source view until yesterday
        dates_to_process = uploader.get_all_dates_with_unsent_data()

        # Remove yesterday from dates_to_process to avoid processing it twice
        dates_without_yesterday = [d for d in dates_to_process if d != yesterday]

        if dates_without_yesterday:
            logger.info(f"Refills Google: Found {len(dates_without_yesterday)} dates with unsent data to process: {dates_without_yesterday}")
            for date in dates_without_yesterday:
                logger.info(f"Refills Google: Processing date: {date}")
                uploader.process_conversions(date, is_backfill=True)
        else:
            logger.info("Refills Google: No dates with unsent data found (excluding yesterday)")

        # Step 2: Process yesterday's data (the regular daily run)
        # Always process yesterday as "Daily Run", regardless of whether it has data or not
        logger.info(f"Refills Google: Processing yesterday ({yesterday}) as daily run")
        uploader.process_conversions(yesterday, is_backfill=False)

    finally:
        uploader.close_connection()


def upload_taboola_purchase_value(**context):
    """Upload Taboola purchase_value conversions (with automatic backfill for last 7 days)"""
    api_key = Variable.get("redtrack_api_key")
    api_endpoint = f"https://api.redtrack.io/conversions?api_key={api_key}"
    slack_webhook = Variable.get("slack_webhook_conversion_upload")

    snowflake_config = {
        'user': Variable.get("snowflake_user"),
        'password': Variable.get("snowflake_password"),
        'account': Variable.get("snowflake_account"),
        'warehouse': 'REPORTING',
        'database': 'INTM',
        'schema': 'CONVERSION_UPLOAD',
        'role': 'ACCOUNTADMIN'
    }

    campaign_id = Variable.get("taboola_campaign_id")

    uploader = TaboolaPurchaseValueUpload(api_endpoint, slack_webhook, snowflake_config, campaign_id)

    # Connect once for checking missing dates
    uploader.connect_to_snowflake()
    uploader.create_control_table()

    try:
        # Get yesterday's date (this is what we always process as "daily run")
        yesterday = get_yesterday_pst()

        # Step 1: Find and process any missing dates from last 7 days (excluding yesterday)
        # Yesterday will be processed in Step 2 as the regular daily run
        missing_dates = uploader.get_missing_dates_from_last_n_days(n_days=7)

        # Remove yesterday from missing_dates to avoid processing it twice
        missing_dates_without_yesterday = [d for d in missing_dates if d != yesterday]

        if missing_dates_without_yesterday:
            logger.info(f"Found {len(missing_dates_without_yesterday)} missing dates to process: {missing_dates_without_yesterday}")
            for missing_date in missing_dates_without_yesterday:
                logger.info(f"Processing missing date: {missing_date}")
                uploader.process_conversions(missing_date, is_backfill=True)
        else:
            logger.info("No missing dates found in last 7 days (excluding yesterday)")

        # Step 2: Process yesterday's data (the regular daily run)
        # Always process yesterday as "Daily Run", regardless of whether it has data or not
        logger.info(f"Processing yesterday ({yesterday}) as daily run")
        uploader.process_conversions(yesterday, is_backfill=False)

    finally:
        uploader.close_connection()


def upload_refills_facebook_purchase_value(**context):
    """Upload Refills Facebook purchase_value conversions (with unlimited lookback from last upload date)"""
    api_key = Variable.get("redtrack_api_key")
    api_endpoint = f"https://api.redtrack.io/conversions?api_key={api_key}"
    slack_webhook = Variable.get("slack_webhook_conversion_upload")

    snowflake_config = {
        'user': Variable.get("snowflake_user"),
        'password': Variable.get("snowflake_password"),
        'account': Variable.get("snowflake_account"),
        'warehouse': 'REPORTING',
        'database': 'INTM',
        'schema': 'CONVERSION_UPLOAD',
        'role': 'ACCOUNTADMIN'
    }

    default_campaign_id = Variable.get("refills_facebook_campaign_id")

    uploader = RefillsFacebookPurchaseValueUpload(api_endpoint, slack_webhook, snowflake_config, default_campaign_id)

    # Connect once for checking missing dates
    uploader.connect_to_snowflake()
    uploader.create_control_table()

    try:
        # Get yesterday's date (this is what we always process as "daily run")
        yesterday = get_yesterday_pst()

        # Step 1: Find and process all dates with unsent data (from earliest source date to yesterday)
        # This checks from the earliest date in source view until yesterday
        dates_to_process = uploader.get_all_dates_with_unsent_data()

        # Remove yesterday from dates_to_process to avoid processing it twice
        dates_without_yesterday = [d for d in dates_to_process if d != yesterday]

        if dates_without_yesterday:
            logger.info(f"Refills Facebook: Found {len(dates_without_yesterday)} dates with unsent data to process: {dates_without_yesterday}")
            for date in dates_without_yesterday:
                logger.info(f"Refills Facebook: Processing date: {date}")
                uploader.process_conversions(date, is_backfill=True)
        else:
            logger.info("Refills Facebook: No dates with unsent data found (excluding yesterday)")

        # Step 2: Process yesterday's data (the regular daily run)
        # Always process yesterday as "Daily Run", regardless of whether it has data or not
        logger.info(f"Refills Facebook: Processing yesterday ({yesterday}) as daily run")
        uploader.process_conversions(yesterday, is_backfill=False)

    finally:
        uploader.close_connection()


def upload_bing_high_value(**context):
    """Upload Bing high_value conversions (with automatic backfill for last 7 days)"""
    api_key = Variable.get("redtrack_api_key")
    api_endpoint = f"https://api.redtrack.io/conversions?api_key={api_key}"
    slack_webhook = Variable.get("slack_webhook_conversion_upload")

    snowflake_config = {
        'user': Variable.get("snowflake_user"),
        'password': Variable.get("snowflake_password"),
        'account': Variable.get("snowflake_account"),
        'warehouse': 'REPORTING',
        'database': 'INTM',
        'schema': 'CONVERSION_UPLOAD',
        'role': 'ACCOUNTADMIN'
    }

    campaign_id = Variable.get("bing_campaign_id")

    uploader = BingHighValueUpload(api_endpoint, slack_webhook, snowflake_config, campaign_id)

    # Connect once for checking missing dates
    uploader.connect_to_snowflake()
    uploader.create_control_table()

    try:
        # Get yesterday's date (this is what we always process as "daily run")
        yesterday = get_yesterday_pst()

        # Step 1: Find and process any missing dates from last 7 days (excluding yesterday)
        # Yesterday will be processed in Step 2 as the regular daily run
        missing_dates = uploader.get_missing_dates_from_last_n_days(n_days=7)

        # Remove yesterday from missing_dates to avoid processing it twice
        missing_dates_without_yesterday = [d for d in missing_dates if d != yesterday]

        if missing_dates_without_yesterday:
            logger.info(f"Found {len(missing_dates_without_yesterday)} missing dates to process: {missing_dates_without_yesterday}")
            for missing_date in missing_dates_without_yesterday:
                logger.info(f"Processing missing date: {missing_date}")
                uploader.process_conversions(missing_date, is_backfill=True)
        else:
            logger.info("No missing dates found in last 7 days (excluding yesterday)")

        # Step 2: Process yesterday's data (the regular daily run)
        # Always process yesterday as "Daily Run", regardless of whether it has data or not
        logger.info(f"Processing yesterday ({yesterday}) as daily run")
        uploader.process_conversions(yesterday, is_backfill=False)

    finally:
        uploader.close_connection()


def upload_google_high_value(**context):
    """Upload Google high_value conversions (with automatic backfill for last 7 days)"""
    api_key = Variable.get("redtrack_api_key")
    api_endpoint = f"https://api.redtrack.io/conversions?api_key={api_key}"
    slack_webhook = Variable.get("slack_webhook_conversion_upload")

    snowflake_config = {
        'user': Variable.get("snowflake_user"),
        'password': Variable.get("snowflake_password"),
        'account': Variable.get("snowflake_account"),
        'warehouse': 'REPORTING',
        'database': 'INTM',
        'schema': 'CONVERSION_UPLOAD',
        'role': 'ACCOUNTADMIN'
    }

    campaign_id = Variable.get("google_campaign_id")

    uploader = GoogleHighValueUpload(api_endpoint, slack_webhook, snowflake_config, campaign_id)

    # Connect once for checking missing dates
    uploader.connect_to_snowflake()
    uploader.create_control_table()

    try:
        # Get yesterday's date (this is what we always process as "daily run")
        yesterday = get_yesterday_pst()

        # Step 1: Find and process any missing dates from last 7 days (excluding yesterday)
        # Yesterday will be processed in Step 2 as the regular daily run
        missing_dates = uploader.get_missing_dates_from_last_n_days(n_days=7)

        # Remove yesterday from missing_dates to avoid processing it twice
        missing_dates_without_yesterday = [d for d in missing_dates if d != yesterday]

        if missing_dates_without_yesterday:
            logger.info(f"Found {len(missing_dates_without_yesterday)} missing dates to process: {missing_dates_without_yesterday}")
            for missing_date in missing_dates_without_yesterday:
                logger.info(f"Processing missing date: {missing_date}")
                uploader.process_conversions(missing_date, is_backfill=True)
        else:
            logger.info("No missing dates found in last 7 days (excluding yesterday)")

        # Step 2: Process yesterday's data (the regular daily run)
        # Always process yesterday as "Daily Run", regardless of whether it has data or not
        logger.info(f"Processing yesterday ({yesterday}) as daily run")
        uploader.process_conversions(yesterday, is_backfill=False)

    finally:
        uploader.close_connection()
