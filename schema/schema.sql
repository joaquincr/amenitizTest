CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Staging Table for App Events Data
CREATE TABLE IF NOT EXISTS stg.app_events_raw (
    event_timestamp TIMESTAMPTZ,
    event_type VARCHAR(50),
    feature_name VARCHAR(50),
    customer_id VARCHAR(50),
    metadata JSON,
    load_timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC'),
    source_file VARCHAR(255)
);

-- Staging Table for Customer Data
CREATE TABLE IF NOT EXISTS stg.customers_raw (
    customer_id VARCHAR(50) NOT NULL,
    company_name VARCHAR(255),
    country VARCHAR(50),
    address VARCHAR(255),
    signup_date DATE,
    subscription_id VARCHAR(50),
    load_timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC'),
    source_file VARCHAR(255)
);

-- Staging Table for Subscription Data
CREATE TABLE IF NOT EXISTS stg.subscriptions_raw (
    subscription_id VARCHAR(50) NOT NULL,
    status VARCHAR(50),
    start_date DATE,
    end_date DATE,
    currency VARCHAR(10),
    amount NUMERIC(10, 2),
    period VARCHAR(10),
    plan_name VARCHAR(50),
    load_timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC'),
    source_file VARCHAR(255)
);

-- Dimension Table: Customer
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    company_name VARCHAR(255),
    country VARCHAR(50),
    address VARCHAR(255),
    signup_date DATE,
    first_subscription_date DATE,
    dw_load_timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC')
);

-- Dimension Table: Date
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(10)
);

-- Dimension Table: Plan
CREATE TABLE IF NOT EXISTS analytics.dim_plan (
    plan_key SERIAL PRIMARY KEY, -- Use a simple integer surrogate key
    plan_name VARCHAR(50) NOT NULL,
    period VARCHAR(10),
    base_amount NUMERIC(10, 2),
    currency VARCHAR(10),
    dw_load_timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC'),
    UNIQUE(plan_name, period) -- Ensure plan uniqueness
);

-- Fact Table: App Usage
CREATE TABLE IF NOT EXISTS analytics.fact_app_usage (
    usage_sk BIGSERIAL PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    date_key INT REFERENCES analytics.dim_date(date_key),
    customer_id VARCHAR(50) REFERENCES analytics.dim_customer(customer_id),
    event_type VARCHAR(50),
    feature_name VARCHAR(50),
    device_type VARCHAR(50),
    dw_load_timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC')
);

-- Fact Table: Subscription Events
CREATE TABLE IF NOT EXISTS analytics.fact_subscription_events (
    event_sk BIGSERIAL PRIMARY KEY,
    date_key INT REFERENCES analytics.dim_date(date_key),
    customer_id VARCHAR(50) REFERENCES analytics.dim_customer(customer_id),
    plan_key INT REFERENCES analytics.dim_plan(plan_key), -- Changed from plan_name
    subscription_id VARCHAR(50) NOT NULL,
    mrr_amount NUMERIC(10, 2) NOT NULL,
    churn_flag BOOLEAN NOT NULL DEFAULT FALSE,
    is_new_mrr_flag BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR(50),
    dw_load_timestamp TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC')
);