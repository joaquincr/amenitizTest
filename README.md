# Amenitiz Data Engineering Challenge

This repository contains a complete data pipeline solution for the Amenitiz Data Engineering Challenge. The project extracts data from multiple sources, loads it into a PostgreSQL database, and transforms it into a star schema for analytical queries.

## 1. Project Structure 📂
```
.
├── .gitignore
├── docker-compose.yml
├── pipeline.py
├── README.md
├── requirements.txt
├── LICENSE
├── sample_data/
│   ├── app_events.json
│   ├── customers.csv
│   └── subscriptions.csv
└── schema/
    ├── schema.sql
```

## 2. How to Run 🚀

### Prerequisites
* Python 3.9+
* Docker Desktop

### Recommended Tools (Software for verification and analysis):
* A PostgreSQL client such as pgAdmin 4 or DBeaver.

### Setup Instructions
1.  **Clone the repository:**
    ```bash
    git clone https://github.com/joaquincr/amenitizTest.git
    cd amenitizTest
    ```
2.  **Build and start the PostgreSQL database:**
    ```bash
    docker compose up -d
    ```
3.  **Create and activate a Python virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
4.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
5.  **Run the entire pipeline:** Run the main ETL pipeline, which will create the schema and load the data.
    ```bash
    # Run the ETL pipeline
    python pipeline.py
    ```
6.  **Verify the results:** Connect to the database using a client like DBeaver or pgAdmin (credentials in `pipeline.py`) and run the queries from the "Analytical Queries" section below.

# Exercises

## 1. Data Sources 📊

I´ve created some sample data following the strunctions and stored it on `./sample_data`.

## 2. Database Design 💾

### Justification for 2 layers schema:

The requirement for a Staging and Analytics layer is met. This separation ensures raw data immutability and data governance, allowing for data reprocessing without affecting the production reporting tables.


### Justification for star schema:

Simplicity and Performance. Analytical queries like MRR trends and feature usage require aggregating data by dimensions (like Date, Customer, Plan). A Star Schema, with its single join between a large Fact table and smaller Dimension tables, offers the fastest query performance and is the most common model for metrics like Monthly Recurring Revenue (MRR) and usage analysis.


### CONSIDERATIONS
- If there is a possibility for duplicates or some edge cases that may produce duplicates or other sort of messy data, I will implement a third layer in between `stg` and `analytics`in which I will develop some pipelines that clean that data. In order words we can use the Medallion architecture (Bronze → Silver → Gold), where the Staging Layer (Bronze) is raw storage. Transformations (if needed i.e: normalization, validation, de-duplication) are performed in an intermediate Silver Layer before the cleaned, conformed data is loaded into the Analytics Layer (Gold). To keep it simple, I have decided going for 2 layers in this example.

- If in further analytics we are interested on those customers that during time have changed their features, we can add a surrogate primary key, in that case customer_id does not need to be unique now and we can have the history of changes experiences by the customer.
We will add a new column as well named `is_current` (BOOL) which will be true only for the latest state of the user.

### Schema

```mermaid
erDiagram
    DIM_CUSTOMER ||--o{ FACT_SUBSCRIPTION_EVENTS : "tracks"
    DIM_DATE ||--o{ FACT_SUBSCRIPTION_EVENTS : "occurs on"
    DIM_PLAN ||--o{ FACT_SUBSCRIPTION_EVENTS : "uses"

    DIM_CUSTOMER ||--o{ FACT_APP_USAGE : "performs"
    DIM_DATE ||--o{ FACT_APP_USAGE : "occurs on"

    DIM_CUSTOMER {
        varchar customer_id PK "Customer's unique ID"
        varchar company_name
        varchar country
        varchar address
        date signup_date
        date first_subscription_date
    }

    DIM_DATE {
        int date_key PK "e.g., 20251014"
        date full_date
        int year
        int month
        varchar month_name
    }
    
    DIM_PLAN {
        int plan_key PK "Surrogate key for the plan"
        varchar plan_name "e.g., enterprise, standard"
        varchar period "monthly or yearly"
        numeric base_amount "Base price of the plan"
        varchar currency
    }

    FACT_APP_USAGE {
        bigserial usage_sk PK "Usage event surrogate key"
        timestamptz event_timestamp
        int date_key FK "FK to DIM_DATE"
        varchar customer_id FK "FK to DIM_CUSTOMER"
        varchar event_type "e.g., click, view"
        varchar feature_name "e.g., backlog, board"
        varchar device_type
    }

    FACT_SUBSCRIPTION_EVENTS {
        bigserial event_sk PK "Subscription event surrogate key"
        int date_key FK "FK to DIM_DATE"
        varchar customer_id FK "FK to DIM_CUSTOMER"
        int plan_key FK "FK to DIM_PLAN"
        varchar subscription_id
        numeric mrr_amount "Calculated MRR for this event"
        boolean churn_flag
        boolean is_new_mrr_flag
        varchar status
    }
````

## 3. Data Ingestion Pipeline 🔄

`./pipeline.py` will first create the schema if it does not exist and then extracts data from `./sample_data/*` and load it into the database, and transform them into the analytic tables, handling errors and logging.
By truncating the stg tables everytime we run `./pipeline.py` we make the pipeline idempotent as well.

## 4. Analytical Queries 📊

### 1. New MRR Trend by Month 📈
This query calculates the total new Monthly Recurring Revenue (MRR) for each month.
```
SELECT
    TO_CHAR(d.full_date, 'YYYY-MM') AS mrr_month,
    SUM(fse.mrr_amount) AS new_mrr
FROM
    analytics.fact_subscription_events AS fse
JOIN
    analytics.dim_date d ON fse.date_key = d.date_key
WHERE
    fse.is_new_mrr_flag = TRUE
GROUP BY
    mrr_month
ORDER BY
    mrr_month;
```

### 2. Top Features Used by Enterprise Customers 🚀
This query finds the most frequently (10) used features by customers on an 'enterprise' plan.
```
SELECT
    fau.feature_name,
    COUNT(*) AS usage_count
FROM
    analytics.fact_app_usage AS fau
WHERE
    fau.customer_id IN (
        SELECT DISTINCT fse.customer_id
        FROM analytics.fact_subscription_events AS fse
        JOIN analytics.dim_plan AS dp ON fse.plan_key = dp.plan_key
        WHERE dp.plan_name = 'enterprise'
    )
GROUP BY
    fau.feature_name
ORDER BY
    usage_count DESC
LIMIT 10;
```

## 3. Average Features Used in First 30 Days 💡
This query calculates the average number of distinct features each customer uses within their first 30 days after signing up.
```
WITH user_first_30_days_activity AS (
    SELECT
        fau.customer_id,
        COUNT(DISTINCT fau.feature_name) AS distinct_features_used
    FROM
        analytics.fact_app_usage AS fau
    JOIN
        analytics.dim_customer AS dc ON fau.customer_id = dc.customer_id
    WHERE
        fau.event_timestamp <= dc.signup_date + INTERVAL '30 days'
    GROUP BY
        fau.customer_id
)
SELECT
    AVG(distinct_features_used) AS avg_distinct_features_in_first_30_days
FROM
    user_first_30_days_activity;
```