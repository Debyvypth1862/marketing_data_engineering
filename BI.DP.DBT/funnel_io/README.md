# Funnel.io dbt Project

This dbt project handles data transformation and loading for Funnel.io Facebook Ads campaign data.

## Project Overview

This project contains dbt models to upsert Facebook Ads campaign summary data from Funnel.io source views into the RAW.FUNNEL_IO.ADS_CAMPAIGN_SUMMARY table.

### Models

- **ADS_CAMPAIGN_SUMMARY**: Incremental model that upserts Facebook Ads campaign data with unique keys on DATE and AD_ACCOUNT_NAME

### Data Sources

- **Source**: `ADS_DATA.FUNNEL__YOUR_FUNNEL_WORKSPACE_ID.PROD_AWS_RAW_STANDARD_WS_FACEBOOK_ADS_CAMPAIGN_SUMMARY_APOSTE_PREMIA`
- **Target**: `RAW.FUNNEL_IO.ADS_CAMPAIGN_SUMMARY`

## Field Mappings

| Source Field | Target Field | Description |
|--------------|--------------|-------------|
| FUNNEL_WORKSPACE_NAME | FUNNEL_WORKSPACE_NAME | Funnel workspace name |
| DATE | DATE | Campaign date (key field) |
| TRAFFIC_SOURCE | TRAFFIC_SOURCE | Traffic source (e.g., Facebook) |
| MEDIA_TYPE | MEDIA_TYPE | Media type (e.g., Social) |
| PAID__ORGANIC | PAID_ORGANIC | Paid or organic traffic indicator |
| CURRENCY | CURRENCY | Currency code |
| AD_ACCOUNT_NAME__FACEBOOK_ADS | AD_ACCOUNT_NAME | Facebook Ads account name (key field) |
| CAMPAIGN | CAMPAIGN | Campaign name |
| CAMPAIGN_ID__FACEBOOK_ADS | CAMPAIGN_ID | Facebook Ads campaign ID |
| AD_ACCOUNT_ID__FACEBOOK_ADS | AD_ACCOUNT_ID | Facebook Ads account ID |
| COST | COST | Campaign cost |
| CLICKS_ALL__FACEBOOK_ADS | CLICKS | Total clicks from Facebook Ads |
| IMPRESSIONS__FACEBOOK_ADS | IMPRESSIONS | Total impressions from Facebook Ads |

## Installation and Setup

### Installing dbt

1. Activate your virtual environment and run `pip install -r requirements.txt`
2. Copy the `profiles.yml` to your `~/.dbt/profiles.yml` directory
3. Configure your Snowflake connection details in the profiles

### Environment Variables

Ensure the following environment variables are set:

- `DBT_ACCOUNT`: Snowflake account identifier
- `DBT_USER`: Snowflake username
- `DBT_PASSWORD`: Snowflake password
- `DBT_ROLE`: Snowflake role
- `DBT_WAREHOUSE`: Snowflake warehouse
- `DBT_DATABASE`: Snowflake database

## Running dbt

### Command Line

1. `cd funnel_io`
2. Check setup: `dbt debug`
3. Install dependencies: `dbt deps`
4. Run models: `dbt run --models ADS_CAMPAIGN_SUMMARY`
5. Test models: `dbt test --models ADS_CAMPAIGN_SUMMARY`

### Docker

Build and run using Docker:

```bash
docker build -t funnel-io-dbt .
docker run funnel-io-dbt run --models ADS_CAMPAIGN_SUMMARY
```

## Scheduling

This project is scheduled to run daily via Airflow DAG (`funnel_io_ads_campaign_summary_daily`) at 6:00 AM PST to ensure fresh data is processed after the source systems are updated.

## Data Quality

The model includes:
- Null checks on key fields (DATE, AD_ACCOUNT_NAME)
- Incremental processing to handle large datasets efficiently
- Upsert functionality based on composite key (DATE, AD_ACCOUNT_NAME)

## Troubleshooting

### Common Issues

1. **Connection Issues**: Verify environment variables are set correctly
2. **Source Data Missing**: Check if the Funnel.io view exists and has data
3. **Permission Errors**: Ensure the dbt role has access to both source and target schemas

### Logs

- Check dbt logs in the `../logs` directory
- Airflow logs available in the Airflow UI for scheduled runs