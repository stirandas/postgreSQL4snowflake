import os
import psycopg2
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR")
    )

def validate_timestamps():
    run_dt = '2025-11-24'
    
    # Postgres Query
    pg_query = f"""
        SELECT 
            to_char(i_ts AT TIME ZONE 'Asia/Kolkata', 'DD-Mon-YYYY HH12:MI:SS.MS AM TZ') AS i_ts_ist,
            to_char(u_ts AT TIME ZONE 'Asia/Kolkata', 'DD-Mon-YYYY HH12:MI:SS.MS AM TZ') AS u_ts_ist
        FROM t_nse_fii_dii_eq_data
        WHERE run_dt = '{run_dt}';
    """
    
    # Snowflake Query
    sf_query = f"""
        SELECT 
            TO_CHAR(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', i_ts), 'DD-Mon-YYYY hh12:mi:ss.FF3 AM') AS i_ts_ist,
            TO_CHAR(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', u_ts), 'DD-Mon-YYYY hh12:mi:ss.FF3 AM') AS u_ts_ist
        FROM t_nse_fii_dii_eq_data
        WHERE run_dt = '{run_dt}';
    """
    
    print(f"Validating timestamps for run_dt: {run_dt}")
    
    # Fetch from Postgres
    try:
        pg_conn = get_postgres_connection()
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(pg_query)
        pg_results = pg_cursor.fetchall()
        pg_conn.close()
    except Exception as e:
        print(f"Error fetching from Postgres: {e}")
        return

    # Fetch from Snowflake
    try:
        sf_conn = get_snowflake_connection()
        sf_cursor = sf_conn.cursor()
        sf_cursor.execute(sf_query)
        sf_results = sf_cursor.fetchall()
        sf_conn.close()
    except Exception as e:
        print(f"Error fetching from Snowflake: {e}")
        return

    # Compare
    if len(pg_results) != len(sf_results):
        print(f"Row count mismatch! Postgres: {len(pg_results)}, Snowflake: {len(sf_results)}")
        return

    mismatches = 0
    for i, (pg_row, sf_row) in enumerate(zip(pg_results, sf_results)):
        # pg_row is (i_ts_ist, u_ts_ist) string with TZ
        # sf_row is (i_ts_ist, u_ts_ist) string without TZ
        
        # Normalize Postgres string to match Snowflake format for comparison
        # Remove the timezone suffix (last word) from Postgres result
        # Example PG: "24-Nov-2025 10:00:00.123 AM IST" -> "24-Nov-2025 10:00:00.123 AM"
        
        pg_i_ts = pg_row[0].rsplit(' ', 1)[0] if pg_row[0] else None
        pg_u_ts = pg_row[1].rsplit(' ', 1)[0] if pg_row[1] else None
        
        sf_i_ts = sf_row[0]
        sf_u_ts = sf_row[1]
        
        if pg_i_ts != sf_i_ts or pg_u_ts != sf_u_ts:
            print(f"Mismatch at row {i}:")
            print(f"  Postgres: i_ts='{pg_row[0]}' (Norm: '{pg_i_ts}'), u_ts='{pg_row[1]}' (Norm: '{pg_u_ts}')")
            print(f"  Snowflake: i_ts='{sf_i_ts}', u_ts='{sf_u_ts}'")
            mismatches += 1
            
    if mismatches == 0:
        print("Validation Successful: All timestamps match.")
    else:
        print(f"Validation Failed: {mismatches} mismatches found.")

if __name__ == "__main__":
    validate_timestamps()
