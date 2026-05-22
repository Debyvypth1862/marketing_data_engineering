import os

'''
This function generates a Snowflake query to refresh external tables based on the platform name provided.
It selects the appropriate streams from environment variables based on the platform and constructs an ALTER EXTERNAL TABLE query for each valid stream.
The query for refreshing all selected streams is then returned as a string.
'''
def generate_snowflake_query(plt_name: str) -> str:
    streams: list[str] = []

    if plt_name == "Cellxpert":
        streams = [os.getenv("cellxpert_dynamic_stream"), os.getenv("cellxpert_registration_stream")]
    if plt_name == "Alanbase":
        streams = [os.getenv("alanbase_common_statistic"), os.getenv("alanbase_conversions")]
    elif plt_name == "Sweep":
        streams = [os.getenv("sweep_dynamic_stream"), os.getenv("sweep_registration_stream"), os.getenv("sweep_ftd_registration_stream")]
    elif plt_name == "Ego":
        streams = [os.getenv('ego_stream')]
    elif plt_name == "Buffalo_partner":
        streams = [os.getenv('inhouse_stream')]
    elif plt_name == "Income_access":
        streams = [os.getenv('income_access_stream')]
    elif plt_name == "Mexos":
        streams = [os.getenv('mexos_stream')]
    elif plt_name == "Myaffiliates":
        streams = [os.getenv('myaffiliates_stream')]
    elif plt_name == "Netrefer":
        streams = [os.getenv('netrefer_stream')]
    elif plt_name == "Q":
        streams = [os.getenv('q_stream')]
    elif plt_name == "Softswiss":
        streams = [os.getenv('softswiss_stream')]
    elif plt_name == "Smartico":
        streams = [os.getenv('smartico_stream')]
    elif plt_name == "Voluum":
        streams = [
            os.getenv('voluum_affiliate_network_report_stream'),
            os.getenv('voluum_affiliate_networks_stream'),
            os.getenv('voluum_campaign_report_stream'),
            os.getenv('voluum_campaigns_stream'),
            os.getenv('voluum_conversions_stream'),
            os.getenv('voluum_flow_report_stream'),
            os.getenv('voluum_flows_stream'),
            os.getenv('voluum_lander_report_stream'),
            os.getenv('voluum_landers_stream'),
            os.getenv('voluum_offer_report_stream'),
            os.getenv('voluum_offers_stream'),
            os.getenv('voluum_traffic_source_report_stream'),
            os.getenv('voluum_traffic_sources_stream'),
            os.getenv('voluum_workspaces_stream')
        ]
    elif plt_name == "Google_Analytics_4":
        streams = [
            os.getenv('Google_Analytics_daily_active_users_stream'),
            os.getenv('Google_Analytics_devices_stream'),
            os.getenv('Google_Analytics_four_weekly_active_users_stream'),
            os.getenv('Google_Analytics_locations_stream'),
            os.getenv('Google_Analytics_pages_stream'),
            os.getenv('Google_Analytics_traffic_sources_stream'),
            os.getenv('Google_Analytics_website_overview_users_stream'),
            os.getenv('Google_Analytics_weekly_active_users_stream')
        ]
    elif plt_name == "Brc":
        streams = [
            os.getenv('BRC_admin_logins_stream'),
            os.getenv('BRC_admins_stream'),
            os.getenv('BRC_advertiser_payers_stream'),
            os.getenv('BRC_advertiser_payments_stream'),
            os.getenv('BRC_advertiser_payments_details_stream'),
            os.getenv('BRC_advertiser_payments_details_paid_stream'),
            os.getenv('BRC_advertisers_stream'),
            os.getenv('BRC_affiliate_systems_stream'),
            os.getenv('BRC_api_access_stream'),
            os.getenv('BRC_brands_stream'),
            os.getenv('BRC_campaign_deal_requests_stream'),
            os.getenv('BRC_campaign_deals_stream'),
            os.getenv('BRC_campaign_materials_stream'),
            os.getenv('BRC_campaign_materials_request_new_stream'),
            os.getenv('BRC_campaign_materials_used_stream'),
            os.getenv('BRC_campaign_products_stream'),
            os.getenv('BRC_campaign_tracker_deals_stream'),
            os.getenv('BRC_campaign_trackers_stream'),
            os.getenv('BRC_campaigns_stream'),
            os.getenv('BRC_clientarea_strings_stream'),
            os.getenv('BRC_currency_stream'),
            os.getenv('BRC_currency_old_usd_stream'),
            os.getenv('BRC_custom_tracker_data_stream'),
            os.getenv('BRC_daily_stats_total_stream'),
            os.getenv('BRC_email_sendout_stats_stream'),
            os.getenv('BRC_emails_stream'),
            os.getenv('BRC_import_queue_stream'),
            os.getenv('BRC_import_status_stream'),
            os.getenv('BRC_ip_block_stream'),
            os.getenv('BRC_payment_details_stream'),
            os.getenv('BRC_payment_history_stream'),
            os.getenv('BRC_payments_stream'),
            os.getenv('BRC_postback_3rd_party_click_log_stream'),
            os.getenv('BRC_postback_3rd_party_log_stream'),
            os.getenv('BRC_postback_3rd_party_urls_stream'),
            os.getenv('BRC_postback_advertiser_domain_stream'),
            os.getenv('BRC_postback_domains_stream'),
            os.getenv('BRC_postback_tracking_stream'),
            os.getenv('BRC_postback_tracking_values_stream'),
            os.getenv('BRC_promotions_stream'),
            os.getenv('BRC_publisher_logins_stream'),
            os.getenv('BRC_publisher_managers_stream'),
            os.getenv('BRC_publisher_news_stream'),
            os.getenv('BRC_publisher_news_seen_stream'),
            os.getenv('BRC_publishers_stream'),
            os.getenv('BRC_stats_errors_stream'),
            os.getenv('BRC_sub_publishers_stream'),
            os.getenv('BRC_tracker_data_stream'),
            os.getenv('BRC_tracker_data_backup_stream'),
            os.getenv('BRC_tracker_data_old_stream'),
            os.getenv('BRC_tracker_data_subid_stream'),
            os.getenv('BRC_tracker_logins_stream'),
            os.getenv('BRC_users_stream')
        ]

    query: str = "\n".join([f"ALTER EXTERNAL TABLE {stream} REFRESH;" for stream in streams if stream])

    return query
