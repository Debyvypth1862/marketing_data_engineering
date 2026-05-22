# Marketing Data Engineering Platform

A production-grade **Marketing Data Engineering** platform built with modern data stack principles. This project demonstrates an end-to-end ELT pipeline for ingesting, orchestrating, and transforming marketing/affiliate data from 15+ data sources into a Snowflake data warehouse.

## Architecture Overview

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│   Data Sources      │     │    Orchestration      │     │   Data Warehouse    │
│                     │     │                       │     │                     │
│  • Cellxpert        │     │   Apache Airflow      │     │     Snowflake       │
│  • Netrefer         │────▶│   (CeleryExecutor)    │────▶│                     │
│  • Income Access    │     │                       │     │  ┌───────────────┐  │
│  • MyAffiliates     │     │   ┌───────────────┐   │     │  │  RAW Schema   │  │
│  • Buffalo Partners │     │   │  Airbyte Sync │   │     │  └───────┬───────┘  │
│  • Google Analytics │     │   │    Tasks      │   │     │          │          │
│  • Voluum           │     │   └───────┬───────┘   │     │  ┌───────▼───────┐  │
│  • Softswiss        │     │           │           │     │  │   dbt Models  │  │
│  • Smartico         │     │   ┌───────▼───────┐   │     │  │  (Transform)  │  │
│  • Mexos            │     │   │  dbt Trigger  │   │     │  └───────┬───────┘  │
│  • Referon          │     │   │    Tasks      │   │     │          │          │
│  • EGO              │     │   └───────────────┘   │     │  ┌───────▼───────┐  │
│  • Q Platform       │     │                       │     │  │  Analytics    │  │
│  • Funnel.io        │     │                       │     │  │  Ready Tables │  │
│  • Redtrack         │     │                       │     │  └───────────────┘  │
│  • Spree            │     │                       │     │                     │
└─────────────────────┘     └──────────────────────┘     └─────────────────────┘
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Ingestion** | [Airbyte](https://airbyte.com/) | Custom source connectors (YAML-based & low-level Python) |
| **Orchestration** | [Apache Airflow](https://airflow.apache.org/) 2.5.1 | DAG scheduling, dependency management, monitoring |
| **Transformation** | [dbt](https://www.getdbt.com/) 1.8+ | SQL-based data modeling with Snowflake adapter |
| **Data Warehouse** | [Snowflake](https://www.snowflake.com/) | Cloud data warehouse with external tables |
| **Containerization** | Docker | All components containerized for deployment |
| **CI/CD** | GitHub Actions / AWS CodeBuild | Automated build and deployment pipelines |
| **Monitoring** | Slack Alerts, Jira Integration | Failure notifications and auto-ticket creation |
| **Data Quality** | Great Expectations, Custom Validators | Data validation and quality checks |

## Project Structure

```
Marketing_Data_Engineering/
├── BI.DP.Airbyte/          # Custom Airbyte source connectors
├── BI.DP.Airflow/          # Airflow DAGs and orchestration logic
└── BI.DP.DBT/              # dbt transformation projects
```

---

## BI.DP.Airbyte — Custom Source Connectors

Custom Airbyte connectors built using both YAML-based (low-code) and Python (low-level) approaches for extracting data from marketing and affiliate platforms.

### Connectors

| Connector | Type | Description |
|-----------|------|-------------|
| `airbyte-buffalopartner` | YAML | Buffalo Partners affiliate data |
| `airbyte-cellxpert` | YAML | Cellxpert affiliate tracking |
| `airbyte-customcomponents` | Python | Shared custom components library |
| `airbyte-ego` | YAML | EGO affiliate network |
| `airbyte-google-analytics-data-api` | Python | Google Analytics 4 Data API |
| `airbyte-incomeaccess` | YAML | Income Access affiliate platform |
| `airbyte-mexos` | Low-level Python | Mexos data extraction |
| `airbyte-myaffiliates` | YAML | MyAffiliates tracking |
| `airbyte-netrefer` | YAML | NetRefer affiliate marketing |
| `airbyte-q` | YAML | Q Platform integration |
| `airbyte-referon` | YAML | Referon referral tracking |
| `airbyte-smartico` | YAML | Smartico gamification platform |
| `airbyte-softswiss` | YAML | Softswiss gaming platform |
| `airbyte-voluum` | Python | Voluum ad tracker |

### Deployment

Connectors are containerized and deployed to AWS ECR. A utility script (`script.sh`) automates registering/updating source definitions in the Airbyte workspace via API.

```bash
# Environment variables required
export AIRBYTE_API_URL=<airbyte-server-url>
export WORKSPACE_ID=<workspace-id>
export SOURCE_NAME=<connector-name>
export IMAGE_TAG=<version-tag>
export REGISTRY=<ecr-registry>
export REPOSITORY=<ecr-repo>

./script.sh
```

---

## BI.DP.Airflow — Orchestration Layer

Apache Airflow orchestrates the entire pipeline with DAGs for each data source, handling ingestion triggers, dbt transformations, data validation, and failure alerting.

### Key Features

- **Dynamic DAG Generation**: Operator accounts and connections fetched from a metadata database
- **Batched Execution**: Configurable batch sizes per platform to manage API rate limits
- **Cascading Triggers**: Airbyte sync DAGs trigger downstream dbt DAGs upon completion
- **Reprocessing Support**: Dedicated reprocess DAGs for historical data backfills
- **Monitoring & Alerting**: Slack notifications on failure, automatic Jira ticket creation
- **Data Quality Checks**: Validation DAGs that run quality checks across all platforms

### DAG Categories

| Category | Example DAG | Description |
|----------|-------------|-------------|
| **Ingestion** | `CellxpertExecuteAllOperatorAccounts` | Triggers Airbyte sync for all operator accounts |
| **Transformation** | `CellxpertDbtTriggered` | Runs dbt models after successful ingestion |
| **Reprocessing** | `ReprocessCellxpertExecuteAllOperatorAccounts` | Historical data reprocessing |
| **Validation** | `ValidateAllOperatorAccounts` | Cross-platform data quality validation |
| **Maintenance** | `MaintenanceCleanupLongRunningDags` | Cleans up stuck/long-running DAG runs |
| **Utilities** | `GeoipToAwsSnowflake` | GeoIP data sync to Snowflake |

### Supported Platforms (15+)

Buffalo Partners, Cellxpert, EGO, Funnel.io, Google Analytics 4, Income Access, Mexos, MyAffiliates, Netrefer, Q Platform, Redtrack, Referon, Sapphirebet, Smartico, Softswiss, Spree, Sweep, Voluum

### Local Development

```bash
cd BI.DP.Airflow

# Start Airflow locally with Docker Compose
docker-compose up -d

# Access Airflow UI at http://localhost:8080
# Default credentials: airflow / airflow
```

### Requirements

Key dependencies include:
- `apache-airflow-providers-airbyte` — Airbyte operator integration
- `apache-airflow-providers-snowflake` — Snowflake hooks and operators
- `apache-airflow-providers-cncf-kubernetes` — K8s pod operators for dbt
- `astronomer-cosmos` — dbt + Airflow integration
- `great_expectations` — Data quality framework
- `dbt-core` + `dbt-snowflake` — dbt execution within Airflow

---

## BI.DP.DBT — Data Transformation

dbt (data build tool) projects for transforming raw ingested data into analytics-ready models in Snowflake. Each platform has its own isolated dbt project with dedicated models, macros, and tests.

### Projects

| Project | Description |
|---------|-------------|
| `alanbase` | Alanbase affiliate data models |
| `api_endpoint` | API endpoint data transformations |
| `brc` | BRC platform models |
| `brt` / `brt_api` | BRT platform models |
| `buffalo_partner` | Buffalo Partners affiliate models |
| `cellxpert` | Cellxpert affiliate tracking models |
| `crypto_cashback` | Crypto cashback program models |
| `ego` | EGO affiliate network models |
| `fact_operator` | Operator fact table aggregations |
| `funnel_io` | Funnel.io Facebook Ads campaign data |
| `ga4` | Google Analytics 4 event data |
| `income_access` | Income Access affiliate models |
| `mexos` | Mexos platform models |
| `myaffiliates` | MyAffiliates tracking models |
| `netrefer` | NetRefer marketing models |
| `q_platform` | Q Platform models |
| `redtrack` | Redtrack conversion data |
| `referon` | Referon referral models |
| `sapphirebet` | Sapphirebet models |
| `smartico` | Smartico gamification models |
| `softswiss` | Softswiss gaming platform models |
| `spree` | Spree commerce models |
| `sweep` | Sweep platform models |
| `voluum` | Voluum ad tracker models |

### dbt Project Structure (per platform)

```
<platform>/
├── Dockerfile              # Container build for CI/CD
├── requirements.txt        # dbt-core + dbt-snowflake
└── <platform>/
    ├── dbt_project.yml     # Project configuration
    ├── packages.yml        # dbt package dependencies
    ├── profiles.yml        # Connection profiles (env var based)
    ├── macros/             # Custom Jinja macros
    ├── models/             # SQL transformation models
    └── README.md           # Platform-specific docs
```

### Running dbt Locally

```bash
cd BI.DP.DBT/<platform>/<platform>

# Set environment variables
export DBT_ACCOUNT=<snowflake-account>
export DBT_USER=<username>
export DBT_PASSWORD=<password>
export DBT_ROLE=<role>
export DBT_WAREHOUSE=<warehouse>
export DBT_DATABASE=<database>

# Run dbt
dbt debug          # Verify connection
dbt deps           # Install packages
dbt run            # Execute models
dbt test           # Run data tests
```

### Docker Execution

```bash
cd BI.DP.DBT/<platform>
docker build -t dbt-<platform> .
docker run -e DBT_ACCOUNT=... -e DBT_USER=... dbt-<platform> run
```

---

## Pipeline Flow

```
1. Airflow DAG triggers on schedule (cron)
2. Airbyte sync tasks extract data from source APIs → Snowflake RAW tables
3. On successful sync, downstream dbt DAG is triggered
4. dbt refreshes Snowflake external table metadata
5. dbt runs incremental/full-refresh models (staging → marts)
6. Data quality checks execute post-transformation
7. On failure: Slack alert + Jira ticket auto-creation
8. Reprocess DAGs available for historical backfills
```

## Release Flow

```
dev2 → dev → qa → stag → prod
```

Each branch push triggers automated CI/CD:
- **Airbyte**: Docker image build → push to ECR → manual update in Airbyte UI
- **Airflow**: Auto-deploy to corresponding environment
- **dbt**: Docker image build → deployed as K8s pods via Airflow

## Environment Variables

| Variable | Description |
|----------|-------------|
| `AIRBYTE_API_URL` | Airbyte server API endpoint |
| `WORKSPACE_ID` | Airbyte workspace identifier |
| `DBT_ACCOUNT` | Snowflake account identifier |
| `DBT_USER` | Snowflake username |
| `DBT_PASSWORD` | Snowflake password |
| `DBT_ROLE` | Snowflake role |
| `DBT_WAREHOUSE` | Snowflake warehouse |
| `DBT_DATABASE` | Snowflake database |

## Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Access to Snowflake data warehouse
- Airbyte instance (self-hosted or cloud)
- AWS account (for ECR, CodeBuild)

## License

This project is proprietary. All rights reserved.
