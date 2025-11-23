# PostgreSQL to Snowflake Batch Ingestion

This project implements an on-demand batch ingestion process to transfer data from a PostgreSQL database to Snowflake using Python.

## Prerequisites

-   **Python 3.9+**
-   **Docker** (optional, for containerized execution)
-   **PostgreSQL Database** (Source)
-   **Snowflake Account** (Destination)

## Setup

1.  Clone the repository.
2.  Create a `.env` file in the root directory with your credentials (see `.env.example` below).

### Environment Variables (.env)

```ini
# PostgreSQL
DB_HOST=your_postgres_host
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password

# Snowflake
SNOWFLAKE_ACCOUNT=your_snowflake_account
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=your_snowflake_db
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_AUTHENTICATOR=snowflake
```

## Usage

### Local Execution

1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
2.  Run the ingestion script:
    ```bash
    python ingest.py
    ```

### Docker Execution

1.  Build the Docker image:
    ```bash
    docker build -t pg-to-snowflake .
    ```
2.  Run the container (passing the .env file):
    ```bash
    docker run --env-file .env pg-to-snowflake
    ```

## Verification

To verify the ingestion:
1.  Check the console output for "Success! Wrote X rows...".
2.  Query your Snowflake table:
    ```sql
    SELECT * FROM T_NSE_FII_DII_EQ_DATA;
    ```
