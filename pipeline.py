import os
import pandas as pd
import logging
import json
import io
from sqlalchemy import create_engine, text

# --- 1. Configuration & Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Final Connection Details ---
DB_USER = "user"
DB_PASSWORD = "password"
DB_HOST = "127.0.0.1" # Use 127.0.0.1 explicitly
DB_PORT = "5432"
DB_NAME = "analytics_db"
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DATA_DIR = "./sample_data"
SOURCE_FILES = {
    "app_events": {"name": "app_events.json", "type": "json"},
    "customers": {"name": "customers.csv", "type": "csv"},
    "subscriptions": {"name": "subscriptions.csv", "type": "csv"}
}

# --- 2. SQL Transformation Queries ---
TRANSFORM_SQL = {
    "dim_customer": """
        INSERT INTO analytics.dim_customer (customer_id, company_name, country, address, signup_date)
        SELECT
            customer_id,
            company_name,
            country,
            address,
            signup_date
        FROM stg.customers_raw
        ON CONFLICT (customer_id) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            country = EXCLUDED.country,
            address = EXCLUDED.address,
            signup_date = EXCLUDED.signup_date;
    """,
    "dim_plan": """
        WITH ranked_subscriptions AS (
            SELECT
                plan_name,
                period,
                amount,
                currency,
                ROW_NUMBER() OVER(PARTITION BY plan_name, period ORDER BY amount DESC) as rn
            FROM stg.subscriptions_raw
        )
        INSERT INTO analytics.dim_plan (plan_name, period, base_amount, currency)
        SELECT
            plan_name,
            period,
            amount AS base_amount,
            currency
        FROM ranked_subscriptions
        WHERE rn = 1
        ON CONFLICT (plan_name, period) DO UPDATE SET
            base_amount = EXCLUDED.base_amount,
            currency = EXCLUDED.currency;
    """,
    "fact_app_usage": """
        DELETE FROM analytics.fact_app_usage;
        INSERT INTO analytics.fact_app_usage (event_timestamp, date_key, customer_id, event_type, feature_name, device_type)
        SELECT
            e.event_timestamp,
            d.date_key,
            e.customer_id,
            e.event_type,
            e.feature_name,
            e.metadata ->> 'device' AS device_type
        FROM stg.app_events_raw e
        JOIN analytics.dim_date d ON e.event_timestamp::date = d.full_date;
    """,
    "fact_subscription_events": """
        DELETE FROM analytics.fact_subscription_events;
        INSERT INTO analytics.fact_subscription_events (date_key, customer_id, plan_key, subscription_id, mrr_amount, churn_flag, is_new_mrr_flag, status)
        WITH subscription_details AS (
            SELECT
                s.subscription_id,
                s.status,
                s.start_date,
                s.end_date,
                -- Normalize amount to a monthly value (MRR)
                CASE
                    WHEN s.period = 'yearly' THEN s.amount / 12.0
                    ELSE s.amount
                END as mrr_amount,
                c.customer_id,
                p.plan_key
            FROM stg.subscriptions_raw s
            JOIN stg.customers_raw c ON s.subscription_id = c.subscription_id
            JOIN analytics.dim_plan p ON s.plan_name = p.plan_name AND s.period = p.period
        )
        SELECT
            d.date_key,
            sd.customer_id,
            sd.plan_key,
            sd.subscription_id,
            sd.mrr_amount,
            (sd.status = 'cancelled' OR sd.end_date IS NOT NULL) AS churn_flag,
            (DATE_TRUNC('month', sd.start_date) = DATE_TRUNC('month', d.full_date)) AS is_new_mrr_flag,
            sd.status
        FROM subscription_details sd
        JOIN analytics.dim_date d ON sd.start_date = d.full_date;
    """
}

def extract_data_from_files():
    """Reads all source files from disk and returns a dictionary of DataFrames."""
    logging.info("--- Step 1: Reading all data from source files ---")
    dataframes = {}
    for source, info in SOURCE_FILES.items():
        file_path = os.path.join(DATA_DIR, info["name"])
        logging.info(f"Reading file: {file_path}")
        try:
            if info["type"] == "json":
                df = pd.read_json(file_path, lines=True)
                df['metadata'] = df['metadata'].apply(json.dumps)
            else:
                df = pd.read_csv(file_path)
            
            df.columns = [col.lower().strip() for col in df.columns]
            dataframes[source] = df
            logging.info(f"Successfully read {len(df)} rows from {info['name']}")
        except Exception as e:
            logging.error(f"Failed to process {file_path}. Error: {e}")
            raise
    return dataframes

def load_data_to_staging_with_sql(conn, dataframes):
    """Loads DataFrames into staging tables using pandas to_sql."""
    logging.info("--- Step 2: Loading data into staging tables using to_sql ---")

    for source, df in dataframes.items():
        table_name = f"{source}_raw"
        logging.info(f"Truncating and loading table: stg.{table_name}")

        # 1. Truncate using execute
        conn.execute(text(f"TRUNCATE TABLE stg.{table_name} RESTART IDENTITY;"))

        # 2. Load data using to_sql
        df.to_sql(table_name, conn, schema='stg', if_exists='append', index=False)

        logging.info(f"Successfully loaded {len(df)} rows into stg.{table_name}")

def transform_data_in_analytics(conn):
    """Runs SQL queries to transform data from staging to analytics layer."""
    logging.info("--- Step 3: Transforming data into analytics layer ---")
    transform_order = ["dim_customer", "dim_plan", "fact_app_usage", "fact_subscription_events"]
    for query_name in transform_order:
        try:
            query = text(TRANSFORM_SQL[query_name])
            logging.info(f"Executing transform for: {query_name}")
            conn.execute(query)
        except Exception as e:
            logging.error(f"Failed to execute transform for {query_name}. Error: {e}")
            raise
    logging.info("Analytics layer loaded successfully.")

def create_schema(conn):
    """Creates database schema from a SQL file."""
    logging.info("--- Step 0: Creating schema if it does not exist --")
    try:
        with open("schema/schema.sql", "r") as f:
            sql = f.read()
        conn.execute(text(sql))
        logging.info("Schema created successfully or already exists.")
    except Exception as e:
        logging.error(f"Failed to create schema. Error: {e}")
        raise

def main():
    """Main pipeline orchestration function."""
    logging.info("===== Starting Data Pipeline Run =====")
    try:
        all_data = extract_data_from_files()

        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # Run schema creation in its own transaction
            with conn.begin() as transaction:
                create_schema(conn)
            
            # Run data loading in a separate transaction
            with conn.begin() as transaction:
                load_data_to_staging_with_sql(conn, all_data)
                transform_data_in_analytics(conn)
        
        logging.info("===== Data Pipeline Run Finished Successfully =====")

    except Exception as e:
        logging.error(f"PIPELINE FAILED. Error: {e}")

if __name__ == "__main__":
    main()