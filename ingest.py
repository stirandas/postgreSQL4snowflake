import os
import sys
import pandas as pd
import psycopg2
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

TABLE_NAME = "t_nse_fii_dii_eq_data"

def ingest():
    print(f"Starting ingestion for: {TABLE_NAME}")
    
    # 1. Connect to Postgres
    try:
        pg_conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
    except Exception as e:
        print(f"Postgres Connection Error: {e}")
        return

    # 2. Fetch Data
    try:
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", pg_conn)
        # Convert columns to uppercase to avoid case sensitivity issues in Snowflake
        df.columns = [c.upper() for c in df.columns]
        print(f"Fetched {len(df)} rows from Postgres.")
    except Exception as e:
        print(f"Postgres Read Error: {e}")
        pg_conn.close()
        return
    finally:
        pg_conn.close()

    if df.empty:
        print("No data to ingest.")
        return

    # 3. Connect to Snowflake
    try:
        sf_conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR")
        )
    except Exception as e:
        print(f"Snowflake Connection Error: {e}")
        return

    # 4. Upload
    try:
        success, nchunks, nrows, _ = write_pandas(
            sf_conn,
            df,
            TABLE_NAME.upper(),
            auto_create_table=True,
            overwrite=True
        )
        if success:
            print(f"Success! Wrote {nrows} rows to Snowflake table {TABLE_NAME.upper()}.")
        else:
            print("Upload failed.")
    except Exception as e:
        print(f"Snowflake Write Error: {e}")
    finally:
        sf_conn.close()

if __name__ == "__main__":
    ingest()
