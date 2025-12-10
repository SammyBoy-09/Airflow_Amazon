import pandas as pd
from sqlalchemy import create_engine

def load(
    df: pd.DataFrame,
    csv_path: str = "cleaned_data.csv",
    xlsx_path: str = "cleaned_data.xlsx"
):
    """
    Save cleaned data to CSV, Excel, and PostgreSQL table.
    """

    # 1) Save to files (optional – keep if you still want them)
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    print(f"[LOAD] Saved files: {csv_path}, {xlsx_path}")

    # 2) Save to PostgreSQL (inside Docker)
    # Replace YOUR_PASSWORD with the same value as POSTGRES_PASSWORD in .env
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    )

    # Write to table 'customers_cleaned'
    df.to_sql("customers_cleaned", engine, if_exists="replace", index=False)
    print("[LOAD] Written to PostgreSQL table: customers_cleaned")