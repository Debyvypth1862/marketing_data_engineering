import os
import logging
from datetime import datetime, timedelta, timezone
import json
import platform
import base64
import time

import requests
from requests.auth import HTTPBasicAuth
import snowflake.connector
from mysql.connector import Error
from airflow.utils.state import State
from airflow.models import DagRun

from airbyte.db_connection import mysql_conn
from replace_spl_char import to_camel_case
from airbyte import constants
from airflow.models import Variable
from airbyte.replace_spl_char import replace_special_characters

logger = logging.getLogger(__name__)


class Utils:

    @classmethod
    def fetch_snowflake_data(cls):
        '''The fetch_snowflake_data method connects to a Snowflake database, executes a  SQL query to retrieve and aggregate data 
        related to tracker logins,
        advertisers, publishers, and operator accounts, then returns the results or logs any errors encountered during execution.'''        
        # snowflake_conn = snowflake.connector.connect(
        #     user=os.getenv("snowflake_user"),
        #     password=os.getenv("snowflake_password"),
        #     account=os.getenv("snowflake_account"),
        #     warehouse=os.getenv("snowflake_warehouse"),
        #     database=os.getenv("snowflake_database"),
        #     schema=os.getenv("snowflake_schema"),
        #     role=os.getenv("snowflake_role"),
        #     insecure_mode=True,
        # )
        try:
            snowflake_conn = snowflake.connector.connect(
                user=os.getenv("DBT_USER"),
                password=os.getenv("DBT_PASSWORD"),
                account=os.getenv("DBT_ACCOUNT"),
                warehouse= os.getenv("DBT_WAREHOUSE"),
                role=os.getenv("DBT_ROLE"),
                insecure_mode=True,
            )
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
        snowflake_cursor = snowflake_conn.cursor()
        try:
            snowflake_cursor.execute(f"""
                select 
                    tl.tlog_id,
                    tl.tlog_username,
                    tl.tlog_password,
                    oa.status,
                    a.adve_name,
                    a.adve_affiliate_system,
                    a.adve_affiliate_login_url,
                    ad.admi_display_name manager,
                    tl.tlog_created,
                    oa.updated_at tlog_updated,
                    case when oa.status = 0 then 1 else 0 end  tlog_deleted,
                    oa.id BRT_ACCOUNT_ID,
                    oa.password BRT_PASSWORD,
                    oa.api_key BRT_APIKEY,
                    oa.api_status as BRT_API_STATUS,
                    op.name BRT_PLATFORM,
                    opr.api_url as BRT_API_ENDPOINT,
                    p.publ_username,
                    sum(td.tdat_views) total_views,
                    sum(td.tdat_clicks) total_clicks,
                    sum(td.tdat_signups) total_signups,
                    sum(td.tdat_deposits) total_deposits,
                    sum(td.tdat_new_deposits) total_new_deposits,
                    sum(td.tdat_postback) total_postback
                from raw.brc.tracker_logins tl
                left join raw.brc.advertisers a 
                    on a.adve_id = tl.tlog_fk_advertiser
                left join raw.brc.publishers p 
                    on p.publ_id = tl.tlog_fk_publisher
                left join raw.brc.publisher_managers pu 
                    on pu.puma_fk_publisher = p.publ_id
                left join raw.brc.admins ad 
                    on ad.admi_id = pu.puma_fk_admin
                left join raw.brc.campaign_trackers ct 
                    on ct.camt_fk_login  = tl.tlog_id
                left join raw.brc.campaigns cs 
                    on cs.camp_id = ct.camt_fk_campaign
                left join raw.brc.tracker_data td 
                    on td.tdat_fk_campaign_tracker = ct.camt_id
                join raw.brt.operator_accounts oa 
                    on oa.br_tracker_login_id = tl.tlog_id
                join raw.brt.operators opr 
                    on oa.operator_id = opr.id
                join raw.brt.operator_operator_platform oop 
                    on oop.operator_id = oa.operator_id
                join raw.brt.operator_platforms op 
                    on oop.operator_platform_id = op.id
                where 1 = 1
                    and tl.tlog_password <> ''
                    and oa.br_tracker_login_id >0
                    and oa.status = 1
                group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
                order by total_clicks desc
            """)
            result = snowflake_cursor.fetchall()
            snowflake_conn.close()
            logger.info("inside snpwflake table")
            logger.info(len(result))
            return result
        except Error as e:
            logger.error(f"Error: {e}")

    @classmethod
    def create_temp_table(cls, connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE operator_staging_temp (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        tlog_id INT ,
                        tlog_username VARCHAR(255),
                        tlog_password VARCHAR(255),
                        tlog_status VARCHAR(50),
                        adve_name VARCHAR(255),
                        adve_affiliate_system VARCHAR(255),
                        adve_affiliate_login_url VARCHAR(255),
                        manager VARCHAR(255),
                        tlog_created TIMESTAMP,
                        tlog_updated TIMESTAMP,
                        tlog_deleted BOOLEAN,
                        BRT_ACCOUNT_ID INT,
                        BRT_PASSWORD VARCHAR(255),
                        BRT_APIKEY VARCHAR(255),
                        BRT_API_STATUS VARCHAR(50),
                        BRT_PLATFORM VARCHAR(50),
                        BRT_API_ENDPOINT VARCHAR(255),
                        publ_username VARCHAR(255),
                        total_views INT,
                        total_clicks INT,
                        total_signups INT,
                        total_deposits INT,
                        total_new_deposits INT,
                        total_postback INT,
                        platform_id INT);
                """)
                return None
        except Error as e:
            logger.error(f"Error: {e}")

    @classmethod
    def insert_into_temp_table(cls, data_to_transfer, connection):
        PLATFORM = os.getenv("platform")
        try:
            with connection.cursor() as cursor:
                for row in data_to_transfer:
                    platform_name = row[15]
                    cursor.execute(f"""
                        INSERT INTO operator_staging_temp
                            (tlog_id, tlog_username, tlog_password, tlog_status, adve_name, adve_affiliate_system,
                            adve_affiliate_login_url, manager, tlog_created, tlog_updated, tlog_deleted, BRT_ACCOUNT_ID, 
                            BRT_PASSWORD, BRT_APIKEY, BRT_API_STATUS, BRT_PLATFORM, BRT_API_ENDPOINT, publ_username, total_views, 
                            total_clicks, total_signups, total_deposits, total_new_deposits, total_postback, platform_id)
                        SELECT %s, %s, %s, %s, %s, %s, %s, %s, DATE_FORMAT(%s, '%Y-%m-01'), %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, p.Id
                        FROM {PLATFORM} p
                        WHERE p.name = %s
                    """, (*row, platform_name)
                    )
                connection.commit()
                result = cursor.fetchall()
                logger.info("inside temporary table")
                return None
        except Error as e:
            logger.error(f"Error: {e}")

    @classmethod
    def delete_duplicate_records(cls, operator, connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    DELETE S1 FROM {operator} AS S1  
                    INNER JOIN {operator} AS S2   
                    WHERE S1.id < S2.id 
                        AND S1.operator_id = S2.operator_id
                        AND S1.platform_id = S2.platform_id;
                """)
                connection.commit()
                logger.info(f"deleted duplicates from {operator} table")
                return None
        except Error as e:
            logger.error(f"Error: {e}")

    @classmethod
    def insert_into_operator(cls, operator, connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {operator} 
                        (operator_id, username, password, name, affiliate_login_url, manager, brt_account_id, views, 
                        clicks, signups, deposits, new_deposits, postback, platform_id, brt_password, api_key, 
                        start_date, endpoint, account_status, last_updated, connection_status, status_updated_at, 
                        tlog_deleted, publ_username )
                    SELECT tlog_id, tlog_username, tlog_password, adve_name, adve_affiliate_login_url, manager, 
                        BRT_ACCOUNT_ID, total_views, total_clicks, total_signups, total_deposits, total_new_deposits, 
                        total_postback, platform_id, BRT_PASSWORD, BRT_APIKEY, DATE_FORMAT(tlog_created, '%Y-%m-01'), 
                        BRT_API_ENDPOINT, 'New' as account_status, IFNULL(tlog_updated, tlog_created) as tlog_updated, 
                        NULL AS connection_status, NULL AS status_updated_at, tlog_deleted, publ_username
                    FROM operator_staging_temp st
                    WHERE st.tlog_id NOT IN (SELECT operator_id FROM {operator});
                """)
                connection.commit()
                result = cursor.fetchall()
                logger.info(f"inside {operator} table")
        except Error as e:
            logger.error(f"Error: {e}")

    @classmethod
    def modify_operator(cls, operator, connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE {operator} AS m
                    JOIN operator_staging_temp AS s
                        ON m.operator_id  = s.tlog_id
                        SET m.account_status = CASE WHEN m.last_updated < s.tlog_updated AND m.airbyte_source_id <> '' THEN 'Updated' 
                        ELSE 'New'
                        END, 
                        m.last_updated = s.tlog_updated,
                        m.username = s.tlog_username,
                        m.password = s.tlog_password,
                        m.status = s.tlog_status,
                        m.name = s.adve_name,
                        m.affiliate_login_url = s.adve_affiliate_login_url,
                        m.manager = s.manager,
                        m.brt_account_id = s.brt_account_id,
                        m.views = s.total_views,
                        m.clicks = s.total_clicks,
                        m.signups = s.total_signups,
                        m.deposits = s.total_deposits,
                        m.new_deposits = s.total_new_deposits,
                        m.postback = s.total_postback,
                        m.platform_id = s.platform_id,
                        m.brt_password = s.BRT_PASSWORD,
                        m.password_check = m.password_check,
                        m.api_key = s.BRT_APIKEY,
                        m.endpoint = s.BRT_API_ENDPOINT,
                        m.created_at = s.tlog_created,
                        m.tlog_deleted = s.tlog_deleted,
                        m.publ_username = s.publ_username
                    WHERE
                        m.last_updated < s.tlog_updated;
                """)
                connection.commit()
        except Error as e:
            logger.error(f"Error: {e}")

    @classmethod
    def delete_operator_rows(cls, operator, connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE {operator} AS m
                    JOIN operator_staging_temp AS s
                        ON m.operator_id  = s.tlog_id
                    SET m.account_status  = "Deleted", 
                        m.tlog_deleted_date = NOW(),
                        m.tlog_deleted = s.tlog_deleted,
                        m.connection_status = 'Disabled'
                    WHERE s.tlog_deleted = 1;
                """)
                connection.commit()
        except Error as e:
            logger.error(f"Error: {e}")

    @classmethod
    def update_data_source_values(cls, stream_names, opp):
        connection = mysql_conn()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        DATA_SOURCE = os.getenv("data_source")
        try:
            with connection.cursor() as cursor:
                for stream in stream_names:
                    namespace = to_camel_case(f"{opp[1]}/{opp[3]}/{opp[4]}")
                    path = f"{namespace}/{stream}/raw_data.jsonl"
                    cursor.execute(f"""
                        INSERT INTO {DATA_SOURCE} 
                            (operator_id, platform_name, source_name, airbyte_connection_id, path, created_at, 
                            last_updated)
                        VALUES ({opp[0]}, '{opp[1]}', '{stream}', '{opp[2]}', '{path}', '{current_time}', 
                            '{current_time}');
                    """)
            connection.commit()
        except Error as e:
            logger.error(f"Error: {e}")
        finally:
            connection.close()


    @classmethod
    def insert_missing_data_into_daily_average_record_count(cls, operator_id):
        '''
        This class method updates the `AVERAGE_RECORDS_COUNT_HISTORY` table by identifying missing `transaction_date` entries
        for a specific operator in the last 30 days, calculating the average number of records extracted for each date, and 
        inserting the results if they don't already exist in the table. It uses multiple CTEs to fetch and process the data efficiently.
        '''        
        pyconnection = mysql_conn()
        AVERAGE_RECORDS_COUNT_HISTORY = os.getenv("average_records_count_history")
        logger.info(f"AVERAGE_RECORDS_COUNT_HISTORY----->{AVERAGE_RECORDS_COUNT_HISTORY}")
        DATA_SOURCE_ITEM = os.getenv("data_source_item")
        DATA_SOURCE = os.getenv("data_source")
        logger.info(f"DATA_SOURCE_ITEM--->{DATA_SOURCE_ITEM}")
        try:
            with pyconnection.cursor() as pycursor:
                # distinct_dates - per `data_source_id` get all `transaction_date`s in the last 30 days from `DATA_SOURCE_ITEM` table
                # missing_dts_darc - get `data_source_id`, `transaction_date` pairs present in `AVERAGE_RECORDS_COUNT_HISTORY` table
                # avg_missing_dts - gets `transaction_date` for a `data_source_id` which aren't present in `AVERAGE_RECORDS_COUNT_HISTORY` table
                pycursor.execute(f"""
                    WITH distinct_dates (ds_id, txn_dt) AS (
                        SELECT DISTINCT data_source_id,
                            transaction_date
                        FROM {DATA_SOURCE_ITEM}
                        WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                            AND transaction_date <= CURDATE()
                            AND data_source_id IN (SELECT id FROM {DATA_SOURCE} WHERE operator_id = {operator_id})
                    ),
                    missing_dts_darc (ds_id, txn_dt) AS (
                        SELECT DISTINCT data_source_id,
                            transaction_date
                        FROM {AVERAGE_RECORDS_COUNT_HISTORY}
                        WHERE data_source_id IN (SELECT id FROM {DATA_SOURCE} WHERE operator_id = {operator_id})
                    ),
                    avg_missing_dts (ds_id, missing_dt) AS (
                        SELECT dd.ds_id,
                            dd.txn_dt
                        FROM distinct_dates dd
                        LEFT JOIN missing_dts_darc mdd
                            ON dd.ds_id = mdd.ds_id
                            AND dd.txn_dt = mdd.txn_dt
                        WHERE mdd.txn_dt IS NULL
                    )
                    SELECT *
                    FROM avg_missing_dts;
                """
                )
                results = pycursor.fetchall()
                logger.info(f"results----{results}")

                for result in results:
                    ds_id = result[0]
                    logger.info(f"ds_id----->{ds_id}")
                    txn_dt = result[1]

                    # past_30_day_records - for a (`data_source_id`, `transaction_date`) get all the records from past 30 days
                    # and assign a rank to each record per ()`data_source_id`, `transaction_date`) ordered by `job_id`
                    # i.e. latest job_id will have rank = 1
                    # latest_records - get all the records returned where rank = 1 and calculate it's average and round it.

                    # Use this CTE to include rows where `records_extracted` is 0
                    # latest_records AS (
                    #       SELECT ROUND(IFNULL(AVG(records_extracted),0)) as avg_records_extracted
                    #       FROM past_30_day_records
                    #       WHERE row_num = 1
                    # )
                    pycursor.execute(f"""
                        WITH past_30_day_records AS (
                            SELECT data_source_id,
                                transaction_date,
                                job_id,
                                records_extracted,
                                ROW_NUMBER() OVER (
                                    PARTITION BY data_source_id, transaction_date
                                    ORDER BY job_id DESC
                                ) AS row_num
                            FROM {DATA_SOURCE_ITEM}
                            WHERE data_source_id = '{ds_id}'
                                AND transaction_date >= DATE_SUB('{txn_dt}', INTERVAL 30 DAY)
                                AND transaction_date <= '{txn_dt}'
                        ),
                        latest_records AS (
                            SELECT ROUND(IFNULL(AVG(records_extracted),0)) as avg_records_extracted
                            FROM past_30_day_records
                            WHERE row_num = 1
                                AND records_extracted != 0
                        )
                        SELECT *
                        FROM latest_records;                
                    """)

                    avg_records_extracted = pycursor.fetchall()
                    logger.info(f"avg_records_extracted----->{avg_records_extracted}")

                    pycursor.execute(f"""
                        INSERT INTO {AVERAGE_RECORDS_COUNT_HISTORY} 
                            (data_source_id, transaction_date, avg_records_extracted)
                        SELECT {ds_id}, '{txn_dt}', {avg_records_extracted[0][0]}
                        FROM DUAL
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {AVERAGE_RECORDS_COUNT_HISTORY}
                            WHERE data_source_id = {ds_id}
                                AND transaction_date = '{txn_dt}'
                        );
                    """)

                    pyconnection.commit()
                    logger.info("Successfully updated AVERAGE_RECORDS_COUNT_HISTORY table")
            pycursor.close()
            pyconnection.close()
        except Error as e:
            logger.info(f"Error: {e}")
        else:
            logger.info("Successfully updated AVERAGE_RECORDS_COUNT_HISTORY table")

    @classmethod
    def pause_dag(cls, dag_id, is_paused):
        airflow_server = os.getenv("airflow_local_server")
        airflow_username = os.getenv("airflow_username")
        airflow_password = os.getenv("airflow_password")
        url = f"{airflow_server}/api/v1/dags/{dag_id}?update_mask=is_paused"
        headers = {"Content-Type": "application/json"}
        basic_auth = HTTPBasicAuth(airflow_username, airflow_password)
        data = {"is_paused": is_paused}

        try:
            response = requests.patch(url, headers=headers, auth=basic_auth, json=data)
            if response.status_code == 200:
                logger.info(f"DAG '{dag_id}' pause status set to {is_paused}.")
            else:
                logger.info(f"Failed to update DAG '{dag_id}'. Status Code: {response.status_code}")
        except Exception as e:
            logger.info(f"An error occurred: {e}")

    @classmethod
    def pause_recovery_dags(cls):
        recovery_dag_ids = constants.Recovery_dag_ids   
        logger.info(recovery_dag_ids)
        for dag_id in recovery_dag_ids:
            cls.pause_dag(dag_id,True)

    def waiting_end_task_group():
        logger.info("Waiting for end task group")
        return None

    '''This method upauses the reprocess dags after the primary dags run were COMPLETED '''
    @classmethod
    def unpause_dags(cls,current_dag_id,DagRun,task_group_and_task_ids,**kwargs):
        logger.info(task_group_and_task_ids)
        fail_dag_flag = cls.update_task_id_details(current_dag_id,task_group_and_task_ids)
        primary_dag_ids  = constants.Primary_dag_ids 
        recovery_dag_ids = constants.Recovery_dag_ids  
        if current_dag_id in primary_dag_ids:
            primary_dag_ids.remove(current_dag_id)
        logger.info(primary_dag_ids)    
        for dag_id in primary_dag_ids:
            dag_runs = DagRun.find(dag_id=dag_id)
            
        # Check if any of the recent dag runs are in 'running' state
            if any(dag_run.state == State.RUNNING for dag_run in dag_runs):
                logger.info(f'{dag_id} is running') 
                logger.info(f'fail_dag_flag value is {fail_dag_flag}')
                if fail_dag_flag != 0:
                    dag_run = kwargs["dag_run"]
                    dag_run.set_state(State.FAILED)   
                    logger.info("Upstream task failed. Marking this task as failed.") 
                return None
        for dag_id in recovery_dag_ids:   
            logger.info(f'{dag_id}  is unpaused')
            cls.pause_dag(dag_id,False) 
        #check failures in dag    
        # dag_run = kwargs["dag_run"]
        # for task_instance in dag_run.get_task_instances():
        #     if task_instance.state == State.FAILED:
        #         raise Exception("One or more tasks failed.")
        if fail_dag_flag != 0:
            dag_run = kwargs["dag_run"]
            dag_run.set_state(State.FAILED)   
            logger.info("Upstream task failed. Marking this task as failed.")         
           

    @classmethod          
    def rec_window(cls,job_detail_id ,snowflake_result):
        '''
        This class method calculates the recovery window for a specific job by querying historical data and identifying dates with data quality issues.
        It uses recursive queries to find missing transaction dates or failed records, then updates the recovery dates for the job in the database.
        '''          
        recovery_interval = Variable.get("recovery_interval")
        job_detail = os.getenv("job_detail")
        data_source_item = os.getenv("data_source_item")
        data_quality_issue = os.getenv("data_quality_issue")
        data_quality_rule = os.getenv("data_quality_rule")
        connection = mysql_conn()
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    WITH RECURSIVE DateList AS (
                        SELECT CURRENT_DATE() AS day_
                        UNION ALL
                        SELECT day_ - INTERVAL 1 DAY
                        FROM DateList
                        WHERE day_ >= CURRENT_DATE() - INTERVAL 30 DAY
                    ), cte(txn_dt, ds_id, job_id, status_, rule) AS (
                        SELECT dsi.transaction_date,
                            dsi.data_source_id,
                            jd.job_id,
                            dqi.status,
                            dqi.description
                        FROM
                            {data_source_item} as dsi
                        INNER JOIN
                            {job_detail} as jd
                        ON dsi.job_detail_id = jd.id
                        INNER JOIN
                            {data_quality_issue} as dqi
                        ON dqi.data_source_item_id = dsi.id
                        INNER JOIN
                            {data_quality_rule} as dqr
                        ON dqr.id = dqi.data_quality_rule_id
                        WHERE dsi.transaction_date BETWEEN CURDATE() - INTERVAL {recovery_interval} DAY AND CURDATE()
                            AND (dqr.severity = 'High' or dqi.description = 'Zero Record Count')
                        AND dsi.data_source_id = (
                            SELECT data_source_id
                            FROM {data_source_item}
                            WHERE job_detail_id = {job_detail_id}
                            LIMIT 1
                        )
                        GROUP BY dsi.data_source_id, dsi.transaction_date, dqi.description, dqi.id
                    ), cte2 AS (
                        SELECT txn_dt,
                            job_id,
                            ds_id,
                            COUNT(1) AS n_rows,
                            COUNT(IF(status_='Pass', 1, NULL)) as n_passed
                        FROM cte
                        GROUP BY txn_dt, job_id, ds_id
                    ), cte3 AS (
                        SELECT DISTINCT ds_id
                        FROM cte2
                    ), cte4 AS (
                        SELECT txn_dt, ds_id
                        FROM cte2
                        GROUP BY txn_dt, ds_id
                        HAVING COUNT(1) = COUNT(IF(n_rows!=n_passed, 1, NULL))
                    ), cte5 AS (
                        SELECT dl.day_, (
                            SELECT data_source_id
                            FROM {data_source_item}
                            WHERE job_detail_id = {job_detail_id}
                            LIMIT 1
                        ) AS ds_id
                        FROM DateList dl
                        LEFT JOIN (
                            SELECT transaction_date, data_source_id
                            FROM {data_source_item}
                            WHERE data_source_id = (
                                SELECT data_source_id
                                FROM {data_source_item}
                                WHERE job_detail_id = {job_detail_id}
                                LIMIT 1
                            )
                                AND transaction_date IN (SELECT * FROM DateList)
                        ) dsi
                        ON dl.day_ = dsi.transaction_date
                        WHERE dsi.transaction_date IS NULL
                    )
                    SELECT *
                    FROM cte4
                    UNION
                    SELECT *
                    FROM cte5;
                """)
                mysql_result = cursor.fetchall()
                result =mysql_result + snowflake_result
                dates_dict = {}
                for date, data_source_id in result:
                    if data_source_id not in dates_dict:
                        dates_dict[data_source_id] = [str(date)]
                    else:
                        dates_dict[data_source_id].append(str(date))

                for data_source_id, dates in dates_dict.items():
                    unique_dates = sorted(set(dates))
                    date_str = ",".join(unique_dates)
                    insert_stmt = f"""
                        UPDATE {job_detail} 
                            SET recovery_dates = %s 
                        WHERE {job_detail}.id = %s;
                    """
                    cursor.execute(insert_stmt, (date_str, job_detail_id))
                    logger.info("recovery window added")
            cursor.close()
            connection.commit()
        except Error as e:
            logger.error(f"Error: {e}")
        finally:
            connection.close()     
   
    @classmethod
    def update_task_id_details(cls ,current_dag_id ,task_group_and_task_ids):
        '''
        This class method updates task details such as duration, start time, end time, and state by querying the Airflow API for each task in the provided list.
        It logs task information and inserts it into the database, while also checking if the task has failed to set a failure flag.
        The method ensures that the task's status and details are captured and stored, and returns a flag indicating if any task failed.
        '''          
        logger.info(task_group_and_task_ids)
        fail_dag_flag = 0 
        dag_id =current_dag_id
        airflow_task_details = os.getenv("airflow_task_details")
        airflow_server = os.getenv("airflow_local_server")
        airflow_username = os.getenv("airflow_username")
        airflow_password = os.getenv("airflow_password")
        headers = {"Content-Type": "application/json"}
        for task_id in task_group_and_task_ids:
            connection = mysql_conn()
            basic_auth = HTTPBasicAuth(airflow_username, airflow_password)
            logger.info(f"dag_id is {dag_id}")
            logger.info(f"task_id is {task_id}")
            full_task_name = f"{dag_id}.{task_id}"
            try:
                get_dag_run_id_url = f"{airflow_server}/api/v1/dags/{dag_id}/dagRuns?order_by=-execution_date&limit=1"
                get_dag_run_id_response = requests.get(get_dag_run_id_url, headers=headers, auth=basic_auth)
                if get_dag_run_id_response.status_code == 200:
                    response_json = json.loads (get_dag_run_id_response.text)
                    dag_run_id = (response_json.get('dag_runs')[0]).get('dag_run_id')
                    logger.info(f'dag_run_id is {dag_run_id}')        
                    url = f"{airflow_server}api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"    
                    response = requests.get(url, headers=headers, auth=basic_auth)  
                    logger.info(f'get_task_duration status code = {response.status_code}')
                    if response.status_code == 200:
                        response_json = json.loads (response.text) 
                        start_date = response_json.get('start_date')
                        end_date = response_json.get('end_date')
                        state = response_json.get('state')
                        get_duration = response_json.get('duration')
                        duration =str(timedelta(seconds=round(get_duration)))
                        logger.info(f'the response state = {state}') #added this line to check the repsonse state

                        with connection.cursor() as cursor:
                            cursor.execute(f"""
                                        INSERT INTO {airflow_task_details}(dag_id,task_id,duration,start_time,end_time,state) 
                                        values ('{dag_id}','{task_id}','{duration}','{start_date}','{end_date}','{state}')""")
                        if state == 'failed' or state == 'upstream_failed':
                            logger.info("Inside if conditon where we are updating fail_dag_flag")
                            fail_dag_flag = 1
                            logger.info(f'fail_dag_flag ={fail_dag_flag}')
                cursor.close()
                connection.commit()  
        
            except Exception as e:
                logger.info(f"An error occurred: {e}")   
            finally:
                connection.close()                            
                
        return fail_dag_flag
    

    @classmethod
    def check_conflict(cls, conn_id):
        '''
        This class method checks for conflicts by querying the Airbyte API to retrieve the last replication job for a given connection ID.
        It sends a POST request to the Airbyte server and returns the JSON response if successful.
        If the request fails or encounters an error, it logs the status code or error message.
        '''        
        airbyte_host = os.getenv("airbyte_server")
        endpoint = "api/v1/jobs/get_last_replication_job"
        base_url = airbyte_host + endpoint
        basic_auth = HTTPBasicAuth("airbyte", "password")
        payload = {"connectionId": conn_id}
        headers = {"accept": "application/json", "content-type": "application/json"}
        
        try:
            response = requests.post(url=base_url, json=payload, headers=headers, auth=basic_auth)
            if response.status_code == 200:
                response_json = response.json() 
                return response_json
            else:
                logger.info(f"Failed to get_last_replication_job DAG . Status Code: {response.status_code}")
        except Exception as e:
            logger.info(f"An error occurred: {e}")

    @classmethod
    def load_jira_config(cls):
        return {
            "email": Variable.get("jira_email"),
            "url_api": Variable.get("jira_url_api"),
            "token": Variable.get("jira_token"),
            "project_name": Variable.get("jira_project_name"),
            "url": Variable.get("jira_url"),
            "label": Variable.get("jira_label")
        }

    def create_authorization_header(email, token):
        string = f"{email}:{token}"
        encoded_string = base64.b64encode(string.encode("utf-8")).decode("utf-8")
        return {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Basic {encoded_string}"
        }

    @classmethod
    def create_account_jira_ticket(self, ticket_details):
        config = self.load_jira_config()
        payload = {
            "fields": {
                "project": {"key": config["project_name"]},
                "summary": ticket_details.get("name"),
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": ticket_details.get("desc")}],
                        }
                    ],
                },
                "issuetype": {"name": "BI Incident"},
                "priority": {"name": "High - 2-4 days due"},
                "labels": [config["label"], "Operator"],
            },
        }
        headers = self.create_authorization_header(config["email"], config["token"])
        response = requests.post(url=config["url_api"], json=payload, headers=headers)
        jira_story = response.json()

        if "id" in jira_story:
            ticket_key = jira_story.get("key")
            ticket_status = "Created"
            jira_url = config["url"]
            with mysql_conn() as newpyconnection:
                with newpyconnection.cursor() as newpycursor:
                    newpycursor.execute(
                        "UPDATE ACCOUNT SET shortcut_ticket_id=%s, shortcut_ticket_url=%s, shortcut_ticket_status=%s, shortcut_ticket_created_on=%s WHERE operator_id=%s",
                        (
                            ticket_key,
                            jira_url + ticket_key,
                            ticket_status,
                            ticket_details.get("todaysDateToSave"),
                            ticket_details.get("operatorId"),
                        ),
                    )
                    newpyconnection.commit()
            return {
                "ticket_id": ticket_key,
                "ticket_url": jira_url + ticket_key,
            }

        return jira_story
    
    def name_of_account(account):
        name = f"{account['platform']} -> {account['name']} -> {account['username']} -> {account['operator_id']}"
        return name

    def description_of_account(account):
        description = ""
        name = f"{account['platform']} -> {account['name']} -> {account['username']} -> {account['operator_id']}"
        description = f"{name} \n"
        description += f"Username: {account['username'].capitalize()}\n"
        description += f"Account Status: {account['account_status'].capitalize()}\n"
        description += f"Validation Status: {account['validation_status'].capitalize()}\n"
        description += f"Validation Enable/Disable: {'Disable' if account['is_validation_enabled'] == 0 else 'Enable'}\n"
        
        if account["validation_message"]:
            description += f"Validation Message: {account['validation_message'].capitalize()}\n"
        
        description += f"Login URL: {account['affiliate_login_url']}\n"
        
        if account["airbyte_source_id"]:
            description += f"Airbyte Source ID: {account['airbyte_source_id']}\n"
        
        if account["airbyte_connection_id"]:
            description += f"Airbyte Connection ID: {account['airbyte_connection_id']}\n"
        
        description += f"Enable/Disable: {account['connection_status']}\n"
        description += f"Views: {account['views']}\n"
        description += f"Clicks: {account['clicks']}\n"
        description += f"Signups: {account['signups']}\n"
        description += f"Deposits: {account['deposits']}\n"
        description += f"New Deposits: {account['new_deposits']}\n"
        description += f"Postback: {account['postback']}\n"
        description += f"Password Check: {account['password']}\n"
        description += f"API Key: {account['api_key']}\n"
        description += f"Start Date: {account['start_date']}\n"
        
        return description

    @classmethod
    def operator_create_jira_ticket(self):
        try:
            date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            main_sql = """
            SELECT a.*, p.name as platform
                FROM ACCOUNT a JOIN PLATFORM p on p.id = a.platform_id
                WHERE a.validation_status IN ('Failed') AND DATE(a.validation_date_time) = DATE(%s)
            """
            connection = mysql_conn()
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(main_sql, (date,))
                accounts = cursor.fetchall()
                connection.commit()
                cursor.close()
                connection.close()
                if len(accounts) > 0:
                    for account in accounts:
                        if account['shortcut_ticket_id'] is not None and account['shortcut_ticket_id'] != '':
                            config = self.load_jira_config()
                            headers = self.create_authorization_header(config["email"], config["token"])
                            url = f"{config['url_api']}/{account['shortcut_ticket_id']}/comment"
                            payload = {
                                "body": {
                                    "content": [
                                    {
                                        "content": [
                                        {
                                            "text": f"Validation Error: {account['validation_message']}",
                                            "type": "text"
                                        }
                                        ],
                                        "type": "paragraph"
                                    }
                                    ],
                                    "type": "doc",
                                    "version": 1
                                }
                            }
                            requests.post(url, headers=headers, json=payload)
                            # Update ticket jira status
                            status_url = config["url_api"] + account['shortcut_ticket_id'] + "/transitions"
                            payload = {
                                "transition": {
                                    "id": 11
                                }
                            }
                            requests.post(status_url, headers=headers, json=payload)
                        else:
                            ticket = {
                                "id": account["id"],
                                "name": self.name_of_account(account),
                                "desc": self.description_of_account(account),
                                "operatorId": account["operator_id"],
                                "todaysDateToSave": datetime.now().strftime("%Y-%m-%d"),
                            }
                            self.create_account_jira_ticket(ticket)
                    return {"status": "Ok"}, 201
                else:
                    logger.info(f"No Failed Operators")
                    return {"status": "No Failed Operators"}, 201
        except Exception as e:
            logger.info(f"An error occurred: {e}")

    def name_of_job(jobDetails):
        name = f"{jobDetails['platform']} -> {jobDetails['name']} -> {jobDetails['username']} -> {jobDetails['job_id']}"
        return name

    def convert_size(value, decimals=2):
        if value is None or not value:
            return "0 Bytes"

        k = 1024
        dm = max(0, decimals)
        sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]

        i = int(floor(log(value) / log(k)))

        return f"{round(value / (k ** i), dm)} {sizes[i]}"

    @classmethod
    def job_create_jira_ticket(self, platform_name):
        try:
            date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            main_sql = """
            SELECT j.id, j.operator_id, j.job_id, j.job_execute_step, j.config_type, j.status, j.records_extracted, j.records_loaded, 
                CAST(j.data_size as SIGNED INTEGER) as data_size, j.execution_time_taken, j.created_at, j.updated_at, 
                j.attempt_started, j.attempt_ended, j.failure_origin, j.error_message, 
                j.shortcut_ticket_id, j.shortcut_ticket_url, j.shortcut_ticket_status, j.shortcut_ticket_created_on,
                a.username, a.name, a.platform, TIME_TO_SEC(j.execution_time_taken) AS totalSeconds,
                CONCAT(a.username, '_', a.operator_id) as user_name_id, a.shortcut_ticket_id as account_shortcut_ticket_id
                FROM JOB as j 
                join (SELECT a.*, p.name as platform FROM ACCOUNT a JOIN PLATFORM p on p.id = a.platform_id) as a on j.operator_id = a.operator_id
                WHERE j.status = 'Failed' AND j.shortcut_ticket_id IS NULL AND (j.error_message IS NOT NULL AND j.error_message != "") AND DATE(j.created_at) = DATE(%s) AND a.platform = %s
                ORDER BY j.created_at ASC
            """
            connection = mysql_conn()
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(main_sql, (date, platform_name))
                jobs = cursor.fetchall()
                connection.close()
                
            for job in jobs:
                if job['shortcut_ticket_id'] is not None:
                    continue
                if job['account_shortcut_ticket_id'] is None:
                    todaysDateToSave = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    account_ticket_details = {
                        "id": job['id'],
                        "operatorId": job['operator_id'],
                        "name": self.name_of_account(job),
                        "desc": "",
                        "todaysDateToSave": todaysDateToSave
                    }
                    account_ticket_response = self.create_account_jira_ticket(account_ticket_details)
                    
                    if 'ticket_id' in account_ticket_response:
                        for job_fixed in jobs:
                            if job_fixed['operator_id'] == job['operator_id']:
                                job_fixed['account_shortcut_ticket_id'] = account_ticket_response['ticket_id']
                not_completed_jobs = None
                connection = mysql_conn()
                with connection.cursor(dictionary=True) as cursor:
                    check_not_completed_sql = """
                    SELECT id, created_at, shortcut_ticket_id, shortcut_ticket_url, shortcut_ticket_status
                    FROM JOB 
                    WHERE operator_id = %s 
                    AND job_execute_step = %s
                    AND shortcut_ticket_id IS NOT NULL 
                    AND (shortcut_ticket_status IS NULL OR shortcut_ticket_status != 'COMPLETED') 
                    AND (error_message IS NOT NULL AND error_message != '')
                    AND created_at < %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                    cursor.execute(check_not_completed_sql, (job['operator_id'], job['job_execute_step'], job['created_at']))
                    not_completed_jobs = cursor.fetchone()
                    logger.info(f"Job ID: {job['id']}")
                    logger.info(f"Job: {job['created_at']}")
                    logger.info(f"Query result (not_completed_jobs): {not_completed_jobs}")
                    if not_completed_jobs:
                        logger.info(f"Found previous incomplete ticket - Job ID: {not_completed_jobs['id']}")
                        logger.info(f"Previous Job Created: {not_completed_jobs['created_at']}")
                        logger.info(f"Previous Job Shortcut Ticket ID: {not_completed_jobs['shortcut_ticket_id']}")
                        logger.info(f"Previous Job Ticket Status: {not_completed_jobs['shortcut_ticket_status']}")
                    else:
                        logger.info(f"No previous incomplete tickets found for operator_id: {job['operator_id']}, job_execute_step: {job['job_execute_step']}")
                connection.close()
                if not_completed_jobs and not_completed_jobs['shortcut_ticket_id']:
                    config = self.load_jira_config()
                    headers = self.create_authorization_header(config["email"], config["token"])
                    url = f"{config['url_api']}/{not_completed_jobs['shortcut_ticket_id']}/comment"
                    payload = {
                        "body": {
                            "content": [
                            {
                                "content": [
                                {
                                    "text": self.description_of_job(job, job['job_execute_step'], None),
                                    "type": "text"
                                }
                                ],
                                "type": "paragraph"
                            }
                            ],
                            "type": "doc",
                            "version": 1
                        }
                    }
                    
                    response = requests.post(url, headers=headers, json=payload)
                    if response.status_code == 201:
                        logger.info(f"Commented on Jira ticket: {response.text}")
                        todaysDateToSave = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                        for job_fixed in jobs:
                            if job_fixed['job_id'] == job['job_id']:
                                job_fixed['shortcut_ticket_id'] = not_completed_jobs['shortcut_ticket_id']
                        update_job_sql = """
                        UPDATE JOB 
                        SET shortcut_ticket_id = %s, shortcut_ticket_url = %s, shortcut_ticket_status = %s, shortcut_ticket_created_on = %s
                        WHERE id = %s
                        """
                        connection = mysql_conn()
                        with connection.cursor(dictionary=True) as cursor:
                            cursor.execute(update_job_sql, (
                                not_completed_jobs['shortcut_ticket_id'],
                                not_completed_jobs['shortcut_ticket_url'],
                                not_completed_jobs['shortcut_ticket_status'],
                                todaysDateToSave,
                                job['id']
                            ))
                            connection.commit()
                        connection.close()
                    else:
                        logger.error(f"Failed to comment on Jira ticket: {response.text}")
                else:
                    if job['account_shortcut_ticket_id'] is not None:
                        todaysDateToSave = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                        job_ticket_details = {
                            "id": job['id'],
                            "step": job['job_execute_step'],
                            "name": f"{job['platform']} -> {job['name']} -> {job['username']} -> {job['operator_id']} -> {job['job_execute_step']}",
                            "desc": self.description_of_job(job, job['job_execute_step'], None),
                            "todaysDateToSave": todaysDateToSave
                        }
                        rs_job = self.create_jobs_jira_ticket(job_ticket_details, job['account_shortcut_ticket_id'])
                        if 'ticket_id' in rs_job:
                            for job_fixed in jobs:
                                if job_fixed['job_id'] == job['job_id']:
                                    job_fixed['shortcut_ticket_id'] = rs_job['ticket_id']

            return jobs
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            return []

    @classmethod
    def dqissue_create_jira_ticket(self, platform_name):
        try:
            date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            main_sql = """
            SELECT j.id, j.operator_id, j.job_id, j.job_execute_step, j.config_type, j.status, j.records_extracted, j.records_loaded, 
            CAST(j.data_size as SIGNED INTEGER) as data_size, j.execution_time_taken, j.created_at, j.updated_at, 
            j.attempt_started, j.attempt_ended, j.shortcut_ticket_id, j.shortcut_ticket_status,
            dqi.incident_ticket_id, dqi.incident_ticket_url, dqi.incident_ticket_status, dqi.incident_ticket_created_on,
            a.username, a.name, a.platform, TIME_TO_SEC(j.execution_time_taken) AS totalSeconds,
            dqi.status AS dq_status, dqi.validation_error as dqi_validation_error, dqi.id as dqi_id, 'QA' as environment,
            CONCAT(a.username, '_', a.operator_id) as user_name_id, a.shortcut_ticket_id as account_shortcut_ticket_id
            FROM JOB as j 
            join (SELECT a.*, p.name as platform FROM ACCOUNT a JOIN PLATFORM p on p.id = a.platform_id) as a on j.operator_id = a.operator_id
            join DATA_SOURCE_ITEM dsi on dsi.job_id = j.job_id
            join DATA_QUALITY_ISSUE dqi on dqi.data_source_item_id = dsi.id
            WHERE dqi.status = 'Failed' AND j.shortcut_ticket_id IS NULL AND dqi.validation_error IS NOT NULL AND DATE(j.created_at) = DATE(%s) AND a.platform = %s
            ORDER BY j.created_at ASC
            """
            connection = mysql_conn()
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(main_sql, (date, platform_name))
                jobs = cursor.fetchall()
                connection.close()

                logger.info(f"Jobs fetched: {len(jobs)}")
                
                for job in jobs:
                    if job['shortcut_ticket_id'] is not None:
                        continue
                    if job['account_shortcut_ticket_id'] is None:
                        todaysDateToSave = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                        account_ticket_details = {
                            "id": job['id'],
                            "operatorId": job['operator_id'],
                            "name": self.name_of_account(job),
                            "desc": "",
                            "todaysDateToSave": todaysDateToSave
                        }
                        account_ticket_response = self.create_account_jira_ticket(account_ticket_details)
                        
                        if 'ticket_id' in account_ticket_response:
                            for job_fixed in jobs:
                                if job_fixed['operator_id'] == job['operator_id']:
                                    job_fixed['account_shortcut_ticket_id'] = account_ticket_response['ticket_id']
                    connection = mysql_conn()
                    with connection.cursor(dictionary=True) as cursor:
                        check_completed_sql = """
                        SELECT shortcut_ticket_id, shortcut_ticket_url, shortcut_ticket_status 
                        FROM JOB 
                        WHERE operator_id = %s 
                        AND job_execute_step = %s
                        AND shortcut_ticket_id IS NOT NULL 
                        AND (shortcut_ticket_status IS NULL OR shortcut_ticket_status != 'COMPLETED') 
                        AND created_at < %s
                        ORDER BY created_at DESC
                        LIMIT 1
                        """
                        cursor.execute(check_completed_sql, (job['operator_id'], job['job_execute_step'], job['created_at']))
                        not_completed_jobs = cursor.fetchone()
                        connection.close()

                    if not_completed_jobs and not_completed_jobs['shortcut_ticket_id']:
                        logger.info(f"not_completed_jobs: {not_completed_jobs.get('shortcut_ticket_id')}")
                        connection = mysql_conn()
                        with connection.cursor(dictionary=True) as cursor:
                            validation_errors_sql = """
                            SELECT dqi.validation_error, dsi.path, dqi.description
                            FROM DATA_QUALITY_ISSUE dqi
                            JOIN DATA_SOURCE_ITEM dsi ON dqi.data_source_item_id = dsi.id
                            WHERE dsi.job_id = %s AND dqi.status = 'Failed' AND dqi.validation_error IS NOT NULL
                            """
                            cursor.execute(validation_errors_sql, (job['job_id'],))
                            validation_errors = cursor.fetchall()
                            connection.close()
                        
                        error_content = ""
                        for i, error in enumerate(validation_errors, 1):
                            error_content += f"{i}. Path: {error['path']}\n"
                            error_content += f"   DQ Issue Type: {error['description']}\n"
                            error_content += f"   Validation Error: {error['validation_error']}\n"
                        
                        if not error_content:
                            error_content = "No specific validation errors found."
                        
                        config = self.load_jira_config()
                        headers = self.create_authorization_header(config["email"], config["token"])
                        url = f"{config['url_api']}/{not_completed_jobs['shortcut_ticket_id']}/comment"
                        payload = {
                            "body": {
                                "content": [
                                {
                                    "content": [
                                    {
                                        "text": f"Data Quality Issues for job_id {job['job_id']}:\n\n{error_content}",
                                        "type": "text"
                                    }
                                    ],
                                    "type": "paragraph"
                                }
                                ],
                                "type": "doc",
                                "version": 1
                            }
                        }
                        
                        response = requests.post(url, headers=headers, json=payload)
                        if response.status_code == 201:
                            logger.info(f"Commented on Jira ticket: {response.text}")
                            for job_fixed in jobs:
                                if job_fixed['job_id'] == job['job_id']:
                                    job_fixed['shortcut_ticket_id'] = not_completed_jobs['shortcut_ticket_id']
                            connection = mysql_conn()
                            with connection.cursor(dictionary=True) as cursor:
                                update_job_sql = """
                                UPDATE JOB 
                                SET shortcut_ticket_id = %s, shortcut_ticket_url = %s, shortcut_ticket_status = %s 
                                WHERE id = %s
                                """
                                cursor.execute(update_job_sql, (
                                    not_completed_jobs['shortcut_ticket_id'],
                                    not_completed_jobs['shortcut_ticket_url'],
                                    not_completed_jobs['shortcut_ticket_status'],
                                    job['id']
                                ))
                                connection.commit()
                                connection.close()
                        else:
                            logger.error(f"Failed to comment on Jira ticket: {response.text}")
                    else:
                        if job['account_shortcut_ticket_id'] is not None:
                            logger.info(f"Job {job['id']} has no shortcut ticket or shortcut ticket is COMPLETED")
                            validation_errors_sql = """
                            SELECT dqi.validation_error, dsi.path, dqi.description 
                            FROM DATA_QUALITY_ISSUE dqi
                            JOIN DATA_SOURCE_ITEM dsi ON dqi.data_source_item_id = dsi.id
                            WHERE dsi.job_id = %s AND dqi.status = 'Failed' AND dqi.validation_error IS NOT NULL
                            """
                            connection = mysql_conn()
                            with connection.cursor(dictionary=True) as cursor:
                                cursor.execute(validation_errors_sql, (job['job_id'],))
                                validation_errors = cursor.fetchall()
                            connection.close()
                            
                            error_content = ""
                            for i, error in enumerate(validation_errors, 1):
                                error_content += f"{i}. DQ Issue Type: {error['description']}\n"
                                error_content += f"   Path: {error['path']}\n"
                                error_content += f"   Validation Error: {error['validation_error']}\n"
                            
                            if not error_content:
                                error_content = "No specific validation errors found."
                            
                            todaysDateToSave = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                            job_ticket_details = {
                                "id": job['id'],
                                "step": 'S4',
                                "name": f"{job['platform']} -> {job['name']} -> {job['username']} -> {job['operator_id']} -> S4",
                                "desc": self.description_of_job(job, 'S4', error_content),
                                "todaysDateToSave": todaysDateToSave
                            }
                            rs_dqissue = self.create_jobs_jira_ticket(job_ticket_details, job['account_shortcut_ticket_id'])
                            if 'ticket_id' in rs_dqissue:
                                for dqissue_fixed in jobs:
                                    if dqissue_fixed['job_id'] == job['job_id']:
                                        dqissue_fixed['shortcut_ticket_id'] = rs_dqissue['ticket_id']
            return jobs
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            return []
            
    @classmethod
    def description_of_job(self, jobDetails, step = 'S1', error_message = None):
        step_executed = step
        job_id = jobDetails.get('job_id', '')
        date = jobDetails['created_at']
        dag_name = replace_special_characters(f"S1_ Ego {jobDetails.get('username', '')}_{jobDetails.get('name', '')}_{jobDetails.get('operator_id', '')}")
        task_name = f'process_operator_{jobDetails.get("operator_id", "")}.{dag_name}'
        dag_id = f'{jobDetails.get("platform", "")}ExecuteAllOperatorAccounts'

        description = f"Step: {step_executed}\n"
        description += f"Job ID: {job_id}\n"
        description += f"Date: {date}\n"
        description += f"Task Name: {task_name}\n"
        description += f"DAG ID: {dag_id}\n"
        if error_message:
            description += f"Error Message: {error_message}\n"
        else:
            if 'failure_origin' in jobDetails and jobDetails['failure_origin']:
                description += f"Error Message: {jobDetails['failure_origin'].capitalize()}\n"
            
            if 'error_message' in jobDetails and jobDetails['error_message']:
                description += f"Error Message: {jobDetails['error_message'].capitalize()}\n"
        
        return description

    @classmethod
    def create_jobs_jira_ticket(self, ticket_details, ticket_parent_id=None):
        config = self.load_jira_config()
        headers = self.create_authorization_header(config["email"], config["token"])
        
        # Prepare fields dictionary
        fields = {
            "project": {"key": config["project_name"]},
            "summary": ticket_details.get("name"),
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": ticket_details.get("desc")}],
                    }
                ],
            },
            "issuetype": {"name": "BI Incident"},
            "priority": {"name": "High - 2-4 days due"},
            "labels": [config["label"], ticket_details.get("step") + "-Airflow"],
        }
        
        # Add parent field only if ticket_parent_id is provided
        if ticket_parent_id:
            fields["parent"] = {"key": ticket_parent_id}
            fields["issuetype"] = {"name": "Sub-task"}
            
        payload = {
            "fields": fields
        }
        response = requests.post(url=config["url_api"], json=payload, headers=headers)
        jira_story = response.json()
        if "key" in jira_story:
            ticket_key = jira_story.get("key")
            logger.info(f"Jira ticket created: {ticket_key}")
            ticket_status = "Created"
            jira_url = config["url"] + jira_story.get("key")
            with mysql_conn() as newpyconnection:
                with newpyconnection.cursor() as newpycursor:
                    newpycursor.execute(
                    "UPDATE JOB SET shortcut_ticket_id=%s, shortcut_ticket_url=%s, shortcut_ticket_status=%s,shortcut_ticket_created_on=%s WHERE id=%s",
                        (
                            ticket_key,
                            jira_url,
                            ticket_status,
                            ticket_details.get("todaysDateToSave"),
                            ticket_details.get("id"),
                        ),
                    )
                    newpyconnection.commit()
                    logger.info(f"Jira ticket updated successfully: {ticket_key}")
            return {
                "ticket_id": ticket_key,
                "ticket_url": jira_url,
            }
        return jira_story
    
    def generate_data_dbt_execute(job_id, platform_name, type, status, error_message, transaction_date):
        with mysql_conn() as newpyconnection:
            with newpyconnection.cursor() as newpycursor:
                newpycursor.execute(
                    "SELECT incident_ticket_id FROM DBT_JOB WHERE platform_name=%s AND type=%s AND DATE(transaction_date)=%s",
                    (
                        platform_name,
                        type,
                        transaction_date.strftime("%Y-%m-%d"),
                    ),
                )
                existing_record = newpycursor.fetchone()
                record_exists = existing_record is not None
                
                if record_exists:
                    newpycursor.execute(
                        "UPDATE DBT_JOB SET incident_ticket_status=%s, incident_ticket_created_on=%s, status=%s, error_message=%s WHERE platform_name=%s AND type=%s AND DATE(transaction_date)=%s",
                        (
                            status,
                            transaction_date.strftime("%Y-%m-%d"),
                            status,
                            error_message,
                            platform_name,
                            type,
                            transaction_date.strftime("%Y-%m-%d"),
                        ),
                    )
                    newpyconnection.commit()
                else:
                    newpycursor.execute(
                        "INSERT INTO DBT_JOB (job_id, platform_name, type, status, error_message, transaction_date) VALUES (%s, %s, %s, %s, %s, %s)",
                        (
                            job_id,
                            platform_name,
                            type,
                            status,
                            error_message,
                            transaction_date,
                        ),
                    )
                    newpyconnection.commit()

    @classmethod
    def create_dbt_jira_ticket(self, ticket_details):
        config = self.load_jira_config()
        
        jira_url = config["url"]
        platform_name = ticket_details.get("id")
        job_id = ticket_details.get("job_id")
        type = ticket_details.get('type')
        status = ticket_details.get('status')
        error_message = ticket_details.get('error_message')
        dateTime = ticket_details.get('dateTime')

        existing_record = None
        
        with mysql_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT incident_ticket_id FROM DBT_JOB WHERE platform_name=%s AND type=%s AND DATE(transaction_date)=%s",
                    (
                        platform_name,
                        type,
                        dateTime.strftime("%Y-%m-%d"),
                    ),
                )
                existing_record = cursor.fetchone()
                
                if existing_record is not None and existing_record[0] is not None:
                    return {
                        "ticket_id": existing_record[0],
                        "ticket_url": config["url"] + existing_record[0],
                        "message": "Ticket already exists"
                    }
        
        headers = self.create_authorization_header(config["email"], config["token"])
        payload = {
            "fields": {
                "project": {"key": config["project_name"]},
                "summary": ticket_details.get("name"),
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": ticket_details.get("desc")}],
                        }
                    ],
                },
                "issuetype": {"name": "BI Incident"},
                "priority": {"name": "High - 2-4 days due"},
                "labels": [config["label"], "Snowflake-Airflow"],
            },
        }
        response = requests.post(url=config["url_api"], json=payload, headers=headers)
        jira_story = response.json()
        if "id" in jira_story:
            ticket_key = jira_story.get("key")
            ticket_status = "Created"
            jira_url = config["url"]
            platform_name = ticket_details.get("id")
            type = ticket_details.get('type')
            status = ticket_details.get('status')
            error_message = ticket_details.get('error_message')
            dateTime = ticket_details.get('dateTime')
            with mysql_conn() as newpyconnection:
                with newpyconnection.cursor() as newpycursor:
                    if existing_record is None:
                        newpycursor.execute(
                            "INSERT INTO DBT_JOB (job_id, platform_name, type, status, error_message, incident_ticket_id, incident_ticket_url, incident_ticket_status, incident_ticket_created_on, transaction_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (
                                job_id,
                                platform_name,
                                type,
                                status,
                                error_message,
                                ticket_key,
                                jira_url + ticket_key,
                                ticket_status,
                                dateTime,
                                dateTime,
                            ),
                        )
                    else:
                        newpycursor.execute(
                            "UPDATE DBT_JOB SET status=%s, error_message=%s, incident_ticket_id=%s, incident_ticket_url=%s, incident_ticket_status=%s, incident_ticket_created_on=%s WHERE platform_name=%s AND type=%s AND DATE(transaction_date)=%s",
                            (
                                status,
                                error_message,
                                ticket_key,
                                jira_url + ticket_key,
                                ticket_status,
                                dateTime,
                                platform_name,
                                type,
                                dateTime.strftime("%Y-%m-%d"),
                            ),
                        )
                    newpyconnection.commit()
            return {
                "ticket_id": ticket_key,
                "ticket_url": jira_url + ticket_key,
            }
        return jira_story

    @classmethod
    def comment_dbt_jira_ticket(self, ticket_id, error_message):
        config = self.load_jira_config()
        headers = self.create_authorization_header(config["email"], config["token"])
        url = f"{config['url_api']}/{ticket_id}/comment"
        payload = {
            "body": {
                "content": [
                {
                    "content": [
                    {
                        "text": error_message,
                        "type": "text"
                    }
                    ],
                    "type": "paragraph"
                }
                ],
                "type": "doc",
                "version": 1
            }
        }
        
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code != 201:
            raise Exception(f"Failed to comment on Jira ticket: {response.text}")

    @classmethod
    def select_failed_dbt(self, dateTime):
        failed_dbt = []
        with mysql_conn() as connection:
            with connection.cursor(dictionary=True) as cursor:
                cursor.execute(
                    "SELECT * FROM DBT_JOB WHERE status='Failed' AND DATE(transaction_date)=%s",
                    (dateTime.strftime("%Y-%m-%d"),),
                )
                failed_dbt = cursor.fetchall()
        
        for dbt in failed_dbt:
            platform_name = dbt["platform_name"]
            
            # Check for previous failures for the same platform_name
            previous_failure = None
            with mysql_conn() as connection:
                with connection.cursor(dictionary=True) as cursor:
                    # Get the most recent failure before today for this platform
                    cursor.execute(
                        """SELECT * FROM DBT_JOB 
                           WHERE platform_name=%s AND status='Failed' AND transaction_date < %s
                           ORDER BY transaction_date DESC LIMIT 1""",
                        (platform_name, dateTime),
                    )
                    previous_failure = cursor.fetchone()
            
            ticket_id = None
            ticket_status = None
            
            # Determine what action to take based on previous failures
            if previous_failure and previous_failure["incident_ticket_id"]:
                ticket_id = previous_failure["incident_ticket_id"]
                ticket_status = previous_failure["incident_ticket_status"]
                
                # If ticket is complete, create a new one; otherwise comment on existing one
                if ticket_status and ticket_status.lower() == "complete":
                    # Create a new ticket because previous one is already complete
                    ticket_details = {
                        "id": platform_name,
                        "job_id": dbt["job_id"],
                        "type": dbt["type"],
                        "status": dbt["status"],
                        "error_message": dbt["error_message"],
                        "dateTime": dateTime,
                        "name": f"DBT Execute Failed: {platform_name}",
                        "desc": self.description_of_dbt(platform_name, dbt["status"], dbt["error_message"], dateTime)
                    }
                    ticket_result = self.create_dbt_jira_ticket(ticket_details)
                    new_ticket_id = ticket_result.get('ticket_id') if isinstance(ticket_result, dict) else ticket_result
                    
                    # Update the current record with the new ticket ID
                    with mysql_conn() as connection:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                """UPDATE DBT_JOB 
                                   SET incident_ticket_id=%s, incident_ticket_status='Unscheduled'
                                   WHERE id=%s""",
                                (new_ticket_id, dbt["id"]),
                            )
                            connection.commit()
                else:
                    # Comment on existing ticket since it's not complete
                    self.comment_dbt_jira_ticket(ticket_id, dbt["error_message"])
                    
                    # Update the current record with the existing ticket ID
                    with mysql_conn() as connection:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                """UPDATE DBT_JOB 
                                   SET incident_ticket_id=%s, incident_ticket_status=%s
                                   WHERE id=%s""",
                                (ticket_id, ticket_status or 'Unscheduled', dbt["id"]),
                            )
                            connection.commit()
            else:
                # No previous failure with a ticket, create a new one
                ticket_details = {
                    "id": platform_name,
                    "job_id": dbt["job_id"],
                    "type": dbt["type"],
                    "status": dbt["status"],
                    "error_message": dbt["error_message"],
                    "dateTime": dateTime,
                    "name": f"DBT Execute Failed: {platform_name}",
                    "desc": self.description_of_dbt(platform_name, dbt["status"], dbt["error_message"], dateTime)
                }
                ticket_result = self.create_dbt_jira_ticket(ticket_details)
                new_ticket_id = ticket_result.get('ticket_id') if isinstance(ticket_result, dict) else ticket_result
                
                # Update the current record with the new ticket ID
                with mysql_conn() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """UPDATE DBT_JOB 
                               SET incident_ticket_id=%s, incident_ticket_status='Unscheduled'
                               WHERE id=%s""",
                            (new_ticket_id, dbt["id"]),
                        )
                        connection.commit()

    @classmethod
    def description_of_dbt(self, platform_name, status, error_message, dateTime):
        formatted_date = dateTime.strftime("%Y-%m-%d %H:%M:%S")
        description = f"Platform: {platform_name}\n"
        description += f"Status: {status}\n"
        description += f"Time: {formatted_date}\n\n"
        description += "Error Details:\n"
        description += f"{error_message}"
        
        return description

    # NOTE: JIRA Ticket creation for daily sync alert
    @classmethod
    def create_daily_sync_alert_jira_ticket(self, issue_details):
        # Default JIRA configuration
        config = self.load_jira_config()
        headers = self.create_authorization_header(config["email"], config["token"])

        payload = {
            "fields": {
                "project": {"key": config["project_name"]},
                "summary": f"Daily Sync Alert: {issue_details['title']}",
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": issue_details['desc']}],
                        }
                    ],
                },
                "issuetype": {"name": "BI Incident"},
                "priority": {"name": "High - 2-4 days due"},
                # "labels": [config["label"], "Snowflake-Airflow"],
            },
        }

        # Create JIRA Ticket
        response = requests.post(url=config["url_api"], json=payload, headers=headers)