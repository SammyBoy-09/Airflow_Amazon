"""
Data Preparation Script
Prepares the relational dataset for ETL pipeline testing:
1. Adds Age and Email columns to Customers table
2. Adds duplicate customer rows for testing duplicate removal
3. Mixes Birthday date formats for format validation testing
4. Adds missing values in Age and Email for testing data quality
5. Updates Exchange_Rates.csv with realistic Jan 2026 rates
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Configuration
DATA_FOLDER = "data/raw/dataset"
RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# Email domains for realistic Gmail generation
GMAIL_DOMAINS = ["gmail.com"]

# Date formats to use for Birthday column
DATE_FORMATS = [
    "%m/%d/%Y",  # 1/1/1980
    "%d/%m/%Y",  # 1/1/1980
    "%Y-%m-%d",  # 1980-01-01
    "%d-%m-%Y",  # 1-01-1980
]

# Realistic Jan 2026 exchange rates (USD = 1.0 baseline)
JAN_2026_RATES = {
    "USD": 1.0,
    "CAD": 1.32,      # Canadian Dollar
    "AUD": 1.55,      # Australian Dollar
    "EUR": 0.92,      # Euro
    "GBP": 0.79,      # British Pound
}


def calculate_age(birthday_str):
    """Calculate age from birthday string."""
    try:
        # Try parsing with multiple formats
        birth_date = None
        for date_fmt in DATE_FORMATS:
            try:
                birth_date = datetime.strptime(birthday_str.strip(), date_fmt)
                break
            except ValueError:
                continue
        
        if birth_date is None:
            return None
        
        today = datetime.now()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age
    except:
        return None


def generate_email(first_name, last_name, customer_key):
    """Generate a realistic Gmail address."""
    # Create base email using first name + key (to ensure uniqueness normally)
    base_email = f"{first_name.lower()}{customer_key}@gmail.com"
    return base_email


def prepare_customers():
    """
    Prepare Customers table:
    1. Add Age column (calculated from Birthday, with some values in "45" format)
    2. Add Email column (Gmail addresses with some missing and duplicates)
    3. Add duplicate rows (same CustomerKey)
    4. Mix Birthday date formats
    """
    print("Preparing Customers table...")
    
    # Read original data with encoding handling
    df = pd.read_csv(os.path.join(DATA_FOLDER, "Customers.csv"), encoding='latin-1')
    original_count = len(df)
    print(f"Original Customers count: {original_count}")
    
    # Calculate Age from Birthday
    df['Age'] = df['Birthday'].apply(calculate_age)
    
    # Email generation
    df['Email'] = df.apply(
        lambda row: generate_email(row['Name'].split()[0], row['Name'].split()[-1], row['CustomerKey']),
        axis=1
    )
    
    # Add some missing values in Age (10% missing)
    missing_age_indices = np.random.choice(df.index, size=int(len(df) * 0.10), replace=False)
    df.loc[missing_age_indices, 'Age'] = np.nan
    
    # Add some missing values in Email (5% missing)
    missing_email_indices = np.random.choice(df.index, size=int(len(df) * 0.05), replace=False)
    df.loc[missing_email_indices, 'Email'] = np.nan
    
    # Convert some Age values to quoted string format "45"
    typecast_indices = np.random.choice(
        [i for i in df.index if pd.notna(df.loc[i, 'Age'])],
        size=int(len(df) * 0.08),  # 8% of rows with age data
        replace=False
    )
    df.loc[typecast_indices, 'Age'] = df.loc[typecast_indices, 'Age'].astype(int).astype(str).apply(lambda x: f'"{x}"')
    
    # Mix Birthday date formats
    # Keep original format for 50%, change to other formats for the rest
    format_indices = np.random.choice(df.index, size=int(len(df) * 0.5), replace=False)
    for idx in format_indices:
        try:
            # Parse original date
            birth_date = datetime.strptime(df.loc[idx, 'Birthday'].strip(), "%m/%d/%Y")
            # Pick random format
            new_format = random.choice(DATE_FORMATS[1:])  # Skip the original format
            df.loc[idx, 'Birthday'] = birth_date.strftime(new_format)
        except:
            pass
    
    # Add duplicate rows (same CustomerKey for testing duplicate removal)
    # Select 5% of rows to duplicate
    duplicate_count = int(len(df) * 0.05)
    duplicate_indices = np.random.choice(df.index, size=duplicate_count, replace=False)
    duplicate_rows = df.loc[duplicate_indices].copy()
    
    # Append duplicates to dataframe
    df = pd.concat([df, duplicate_rows], ignore_index=True)
    
    final_count = len(df)
    print(f"After adding duplicates: {final_count} (added {final_count - original_count} duplicate rows)")
    
    # Save enhanced Customers file
    output_path = os.path.join(DATA_FOLDER, "Customers_Enhanced.csv")
    df.to_csv(output_path, index=False)
    print(f"✓ Saved to {output_path}")
    
    return df


def update_exchange_rates():
    """
    Update Exchange_Rates.csv to realistic Jan 2026 rates.
    Keep all original dates but update rates to Jan 2026 realistic values.
    """
    print("\nUpdating Exchange_Rates table...")
    
    df = pd.read_csv(os.path.join(DATA_FOLDER, "Exchange_Rates.csv"))
    original_count = len(df)
    
    # Update all rates to Jan 2026 values
    for currency, rate in JAN_2026_RATES.items():
        mask = df['Currency'] == currency
        df.loc[mask, 'Exchange'] = rate
    
    # Save updated Exchange_Rates file (overwrite original)
    output_path = os.path.join(DATA_FOLDER, "Exchange_Rates.csv")
    df.to_csv(output_path, index=False)
    print(f"✓ Updated {original_count} exchange rates")
    print(f"✓ Saved to {output_path}")
    print(f"   Rates: USD={JAN_2026_RATES['USD']}, CAD={JAN_2026_RATES['CAD']}, " +
          f"AUD={JAN_2026_RATES['AUD']}, EUR={JAN_2026_RATES['EUR']}, GBP={JAN_2026_RATES['GBP']}")


def verify_datasets():
    """Verify all dataset files are valid."""
    print("\nVerifying datasets...")
    
    files_to_check = [
        ("Customers_Enhanced.csv", "Customers"),
        ("Sales.csv", "Sales"),
        ("Products.csv", "Products"),
        ("Stores.csv", "Stores"),
        ("Exchange_Rates.csv", "Exchange_Rates"),
    ]
    
    for filename, table_name in files_to_check:
        filepath = os.path.join(DATA_FOLDER, filename)
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            print(f"✓ {table_name:20s}: {len(df):7,d} rows, {len(df.columns):2d} columns")
        else:
            print(f"✗ {filename} not found")


def main():
    """Main execution."""
    print("=" * 70)
    print("DATA PREPARATION SCRIPT - Relational Dataset Enhancement")
    print("=" * 70)
    
    # Change to workspace directory
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Prepare Customers table
    customers_df = prepare_customers()
    
    # Update Exchange_Rates
    update_exchange_rates()
    
    # Verify all datasets
    verify_datasets()
    
    print("\n" + "=" * 70)
    print("DATA PREPARATION COMPLETE")
    print("=" * 70)
    print("\nSummary:")
    print("✓ Added Age and Email columns to Customers")
    print("✓ Added duplicate rows for duplicate removal testing")
    print("✓ Mixed Birthday date formats")
    print("✓ Injected missing values (Age: 10%, Email: 5%)")
    print("✓ Added Age typecasting (quoted format like \"45\")")
    print("✓ Updated Exchange_Rates.csv with Jan 2026 realistic rates")
    print("\nNext steps:")
    print("1. Review Customers_Enhanced.csv for data quality")
    print("2. Create customers_etl_dag.py to process enhanced data")
    print("3. Create sales_etl_dag.py for Sales with Product join")
    print("4. Create reporting DAG for combined analysis")


if __name__ == "__main__":
    main()
