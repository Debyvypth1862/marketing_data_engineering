import json
import os
import pickle
import re
import time
from datetime import datetime
import tempfile
import uuid

import boto3
import requests
import selenium.webdriver.support.expected_conditions as EC
from env_config import (
    aws_access_key_id,
    aws_secret_access_key,
    output_bucket,
    output_bucket_path_dq_casino,
)
from selenium import webdriver
from airflow.exceptions import AirflowException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium_recaptcha_solver import RecaptchaSolver
from webdriver_manager.chrome import ChromeDriverManager
from utilities import insert_data_source_item_table, insert_job_detail_table, insert_jobs_table
from fetch_connection_list import (
    fetch_connection_data_source_table,
    fetch_connection_data_source_table,
)
from db_connection import mysql_conn

import logging

task_logger = logging.getLogger("airflow.task")

# Function to get a webdriver instance
def get_webdriver(profile_path=None):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(f"user-agent={user_agent}")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--remote-debugging-port=9222")
    unique_dir = os.path.join(tempfile.gettempdir(), f"chrome_user_data_{uuid.uuid4().hex}")
    chrome_options.add_argument(f"--user-data-dir={unique_dir}")
    task_logger.info(f"Using Chrome user data directory: {unique_dir}")
    # Use a specific Chrome driver path if available
    return webdriver.Chrome(
        options=chrome_options,
    )

# Function to perform login using Selenium and maintain the session
def login_with_selenium(
    s3, username, password
):
    # Initialize the browser with the profile path
    driver = get_webdriver()
    solver = RecaptchaSolver(driver=driver)

    try:
        # Navigate to the login page
        task_logger.info("Loading login page...")
        driver.get("https://sapphirebet.partners/sign-in")

        # Wait for page to load
        wait = WebDriverWait(driver, 20)

        # Find and fill the username field
        task_logger.info("Filling login credentials...")

        # Wait for login form to be available
        username_field = wait.until(
            EC.presence_of_element_located((By.NAME, "login"))
        )
        username_field.clear()
        username_field.send_keys(username)

        # Find and fill the password field
        password_field = driver.find_element(By.NAME, "password")
        password_field.clear()
        password_field.send_keys(password)

        # Handle cookies banner if present
        try:
            task_logger.info("Checking for cookies accept button...")
            cookies_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="root"]/div[2]/div/div/div/div/div/button')
                )
            )
            cookies_button.click()
            task_logger.info("Cookies accept button clicked")
            time.sleep(1)  # Give time for the overlay to disappear
        except Exception as cookie_error:
            task_logger.info(
                f"No cookies banner found or couldn't interact with it: {cookie_error}"
            )

        # Find and click the login button
        task_logger.info("Clicking login button...")
        login_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    '//*[@id="root"]/div[1]/main/div/div/div/div/form/div/div[4]/button',
                )
            )
        )
        login_button.click()
        task_logger.info("Login button clicked")

        # After clicking login, check if we need to solve a reCAPTCHA challenge
        try:
            task_logger.info("Checking for reCAPTCHA challenge...")

            # First check if the reCAPTCHA iframe exists without switching to it
            recaptcha_present = False
            try:
                # Check if the iframe is present in the DOM
                recaptcha_iframe = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            '//iframe[contains(@src, "recaptcha") and contains(@src, "bframe")]',
                        )
                    )
                )
                recaptcha_present = True
                task_logger.info("reCAPTCHA iframe found in the DOM")
            except Exception as iframe_error:
                task_logger.info(f"No reCAPTCHA iframe found: {iframe_error}")
                recaptcha_present = False

            if recaptcha_present:
                # Make sure we're on the main content
                driver.switch_to.default_content()

                # Wait a moment for any animations or overlays to clear
                time.sleep(2)

                try:
                    # Try to get the iframe again and check if it's visible and clickable
                    recaptcha_iframe = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable(
                            (
                                By.XPATH,
                                '//iframe[contains(@src, "recaptcha") and contains(@src, "bframe")]',
                            )
                        )
                    )

                    task_logger.info("reCAPTCHA challenge detected, solving...")

                    # Use the solver to handle the challenge
                    task_logger.info("Using RecaptchaSolver to solve the challenge...")
                    solver.solve_recaptcha_v2_challenge(iframe=recaptcha_iframe)
                    task_logger.info("reCAPTCHA challenge solved")
                except Exception as recaptcha_error:
                    raise AirflowException(
                        f"Could not solve reCAPTCHA: {recaptcha_error}"
                    )
                    # Continue with the login process anyway
            else:
                task_logger.info("No reCAPTCHA challenge detected, continuing...")
        except Exception as captcha_challenge_error:
            task_logger.info(
                f"Error during reCAPTCHA challenge detection: {captcha_challenge_error}"
            )

        # Wait for login to complete
        task_logger.info("Waiting for login to complete...")
        time.sleep(5)  # Wait for login process

        # Check if login was successful
        if "sign-in" not in driver.current_url.lower():
            task_logger.info("Login successful!")

            # Return the driver for further use
            return driver
        else:
            driver.save_screenshot("/tmp/login_failed.png")
            s3.upload_file("/tmp/login_failed.png", "sapphirebet-browser-profiles", f"{username}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}/login_failed.png")
            raise AirflowException("Login failed. Check credentials or website structure.")

    except Exception as e:
        task_logger.info(f"Error during login process: {e}")
        driver.save_screenshot("/tmp/login_failed.png")
        s3.upload_file("/tmp/login_failed.png", "sapphirebet-browser-profiles", f"{username}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}/login_failed.png")
        driver.quit()
        raise


# Function to extract CSRF token from the page
def extract_csrf_token(driver):
    try:
        # Wait for the page to load completely
        time.sleep(3)

        # Try to find the CSRF token in the page source
        task_logger.info("Checking page source for CSRF token...")
        page_source = driver.page_source

        # Try multiple regex patterns to find the CSRF token
        csrf_patterns = [
            r"%22csrf%22%3A%22([^%]+)%22",  # URL encoded pattern, usually this one
            r'"csrfToken":"([^"]+)"',  # Original pattern
            r'"csrf\\?":"([^"]+)"',  # Pattern for "csrf":"token"
            r'csrf=([^&"]+)',  # Simple csrf=token pattern
            r"csrf%22%3A%22([\w-]+)%22",  # Another URL encoded format
            r'csrf":"([\w-]+)"',  # JSON format
        ]

        for pattern in csrf_patterns:
            csrf_match = re.search(pattern, page_source)
            if csrf_match:
                task_logger.info(f"Found CSRF token using pattern: {pattern}")
                break

        if csrf_match:
            task_logger.info("Found CSRF token in page source")
            return csrf_match.group(1)

        task_logger.info("Could not find CSRF token in cookies, page source, or via JavaScript")
        return None
    except Exception as e:
        task_logger.info((f"Error extracting CSRF token: {e}"))
        raise

# Function to send GraphQL request using the authenticated session
def send_graphql_request(driver, csrf_token):
    try:
        # Create a requests session with the same cookies as the Selenium driver
        session = requests.Session()
        for cookie in driver.get_cookies():
            session.cookies.set(cookie["name"], cookie["value"])

        graphql_url = "https://sapphirebet.partners/graphql"
        headers = {"Content-Type": "application/json", "x-csrf-token": csrf_token}

        query = """
        query GetPlayersReport($filter: PlayersReportFilter!) {
          authorized {
            partner {
              reports {
                playersReport(filter: $filter) {
                  status
                  hash
                  pagesCount
                  rows {
                    ...playerRow
                    __typename
                  }
                  total {
                    ...playerRow
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        fragment playerRow on PlayersReportRow {
          siteId
          siteName
          mediaId
          subId
          playerId
          registrationDate
          country
          depositAmount
          betsAmount
          bonusAmount
          companyProfit
          comissionRS
          CPA
          comissionAmount
          holdTime
          blocked
          clickId
          __typename
        }
        """

        endPeriod = datetime.now().strftime("%Y-%m-%dT00:00:00.000Z")
        all_data = []

        retry_count = 2
        page = 1
        while True:
            task_logger.info(f"Fetching page {page}...")
            variables = {
                "filter": {
                    "currencyId": 6,
                    "startPeriod": "2024-01-01T00:00:00.000Z",
                    "endPeriod": endPeriod,
                    "onlyNewPlayers": False,
                    "withoutDepositsOnly": False,
                    "subId": "",
                    "methood": "get",
                    "pageNumber": page,
                    "countOnPage": 100,
                }
            }

            payload = {
                "operationName": "GetPlayersReport",
                "query": query,
                "variables": variables,
            }
            response = session.post(graphql_url, json=payload, headers=headers)
            task_logger.info(payload)
            if response.status_code == 200:
                data = response.json()
                task_logger.info(data)
                rows = (
                    data.get("data", {})
                    .get("authorized", {})
                    .get("partner", {})
                    .get("reports", {})
                    .get("playersReport", {})
                    .get("rows", [])
                )
                if not rows and page == 1 and retry_count > 0:
                    retry_count -= 1
                    continue
                elif not rows:
                    task_logger.info(
                        f"Empty rows array detected on page {page}. Stopping further requests."
                    )
                    break
                task_logger.info(f"Successfully fetched page {page}")
                for row in rows:
                    all_data.append(row)
                page += 1
                time.sleep(2)
            else:
                task_logger.info(
                    f"Failed to fetch page {page}. Status code: {response.status_code}"
                )
                task_logger.info(response.text)

        return all_data

    except Exception as e:
        task_logger.info(f"Error sending GraphQL request: {e}")
        raise


# Main function
def processing_sapphirebet(
    user_name, password, operator_id, platform_name="Sapphirebet"
):
    mysql_connection = mysql_conn()
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    result = fetch_connection_data_source_table(platform_name, operator_id)
    file_path = result[0][4]
    stream_name = result[0][2]
    data_source_id = result[0][0]
    job_created_at = job_updated_at = datetime.now() 
    try:
        task_logger.info("Starting login process with Selenium...")
        driver = login_with_selenium(
            s3, username=user_name, password=password
        )
        
        if driver:
            task_logger.info("Extracting CSRF token...")
            csrf_token = extract_csrf_token(driver)

            if csrf_token:
                task_logger.info("CSRF Token is extracted successfully")

                # Send GraphQL request using requests session with cookies
                task_logger.info("Sending GraphQL request using requests session...")
                data = send_graphql_request(driver, csrf_token)

                if data:
                    task_logger.info("GraphQL request completed successfully!")
                    s3_file_path = f"{output_bucket_path_dq_casino}{file_path}"
                    local_file_path = os.path.join("/tmp", s3_file_path)
                    
                    # Ensure directory exists before writing to file
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    task_logger.info(f"Created directory structure for: {os.path.dirname(local_file_path)}")
                    
                    # Save the data to a file as JSONL (JSON Lines)
                    row_count = len(data)
                    with open(local_file_path, "w") as f:
                        # Check if data is a list or dictionary
                        if isinstance(data, list):
                            # Write each item as a separate line
                            for item in data:
                                f.write(json.dumps(item) + "\n")
                        else:
                            # If it's a single object, write it as one line
                            f.write(json.dumps(data) + "\n")
                    task_logger.info(f"Data saved to {local_file_path}")
                    file_size = os.path.getsize(local_file_path)
                else:
                    task_logger.info("No data received from GraphQL request")
                    return

                s3.upload_file(local_file_path, output_bucket, s3_file_path)
                task_logger.info(f"File uploaded to S3: {s3_file_path} in {output_bucket}")
                job_id = insert_jobs_table(
                    mysql_connection,
                    operator_id,
                    "succeeded",
                    file_size,
                    job_created_at,
                    job_updated_at,
                )
                job_detail_id = insert_job_detail_table(
                    mysql_connection,
                    job_id,
                    stream_name,
                    row_count,  # records_extracted
                    file_size,  # data_size
                    job_created_at,
                    "succeeded"  # status
                )

                insert_data_source_item_table(
                    mysql_connection,
                    job_id,
                    job_detail_id,
                    data_source_id,
                    s3_file_path,
                    "Succeeded",
                    row_count,
                    job_created_at
                )

            task_logger.info("Closing browser...")
            driver.quit()
    except Exception as e:
            job_id = insert_jobs_table(
                mysql_connection,
                operator_id,
                "failed",
                0,
                job_created_at,
                job_updated_at,
            )
            job_detail_id = insert_job_detail_table(
                mysql_connection,
                job_id,
                stream_name,
                0,  # records_extracted
                0,  # data_size
                job_created_at,
                "failed"  # status
            )

            insert_data_source_item_table(
                mysql_connection,
                job_id,
                job_detail_id,
                data_source_id,
                None,
                "pending",
                0,
                job_created_at
            )
            raise