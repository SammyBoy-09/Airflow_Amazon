from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import your ETL functions
import sys
sys.path.append("/opt/airflow/scripts")

from Extract import extract
from TransformAmazon import transform_amazon
from Load import load


# ---------------------------------------
# DEFAULT DAG SETTINGS
# ---------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

# ---------------------------------------
# DEFINE DAG FOR AMAZON DATASET
# ---------------------------------------
with DAG(
    dag_id="amazon_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",       # Runs daily (you can change)
    catchup=False
):

    # --------- TASK 1: EXTRACT ---------
    def extract_task():
        input_path = "/opt/airflow/data/raw/amazon.csv"   # Amazon dataset
        df = extract(input_path)
        print(f"Extracted {len(df)} rows from amazon.csv")
        print(f"Columns: {df.columns.tolist()}")
        return df.to_json()   # pass dataframe to next task

    extract_op = PythonOperator(
        task_id="extract_amazon_data",
        python_callable=extract_task
    )

    # --------- TASK 2: TRANSFORM ---------
    def transform_task(ti):
        df_json = ti.xcom_pull(task_ids="extract_amazon_data")
        import pandas as pd
        df = pd.read_json(df_json)
        print(f"Transforming {len(df)} rows...")
        df = transform_amazon(df)
        print(f"Transformed data shape: {df.shape}")
        return df.to_json()

    transform_op = PythonOperator(
        task_id="transform_amazon_data",
        python_callable=transform_task
    )

    # --------- TASK 3: LOAD ---------
    def load_task(ti):
        df_json = ti.xcom_pull(task_ids="transform_amazon_data")
        import pandas as pd
        df = pd.read_json(df_json)
        load(
            df,
            csv_path="/opt/airflow/data/processed/amazon_cleaned_data.csv",
            xlsx_path="/opt/airflow/data/processed/amazon_cleaned_data.xlsx",
            load_type="full",  # Options: 'full' or 'incremental'
            bulk_chunk_size=1000  # Bulk load in chunks of 1000 rows
        )
        print(f"Loaded {len(df)} rows to output files")

    load_op = PythonOperator(
        task_id="load_amazon_data",
        python_callable=load_task
    )

    # ORDER OF EXECUTION
    extract_op >> transform_op >> load_op
