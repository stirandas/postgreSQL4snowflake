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

## Validation

To ensure data accuracy, specifically for timestamps between PostgreSQL and Snowflake, you can run the validation script.

1.  Ensure you have run the ingestion script first.
2.  Run the validation test:
    ```bash
    python tests/validate_timestamps.py
    ```
    This script compares the `i_ts` and `u_ts` columns in both databases, accounting for timezone differences (IST), and reports any mismatches.

## Technical Note: Timestamp Handling

This project handles a specific nuance in transferring `TIMESTAMPTZ` data from PostgreSQL to Snowflake to ensure accuracy for the **Asia/Kolkata (IST)** timezone.

-   **PostgreSQL**: Stores timestamps with timezone information. When queried, it returns an aware datetime object.
-   **Snowflake**: When using the standard pandas connector, timestamps can sometimes be interpreted incorrectly if not explicitly handled, especially when mixed with naive and aware datetimes.
-   **The Solution**:
    -   The `ingest.py` script explicitly converts `I_TS` and `U_TS` columns to **UTC** and makes them **timezone-naive** before uploading.
    -   This ensures Snowflake treats the values as absolute UTC points in time.
    -   When querying Snowflake (as seen in the validation script), we convert these UTC values back to `'Asia/Kolkata'` to verify they match the original source of truth from PostgreSQL.
