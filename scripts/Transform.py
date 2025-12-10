import pandas as pd
import numpy as np
from cleaning_utils import DataCleaner

def transform(df: pd.DataFrame):
    """
    Applies all cleaning operations using cleaning_utils functions.
    """
    
    # 1. REMOVE EMPTY/MISSING VALUES
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.dropna(subset=["name", "email", "age"])
    
    # 2. REMOVE DUPLICATES
    df = DataCleaner.remove_duplicates(df, subset=["email"], keep="first")
    
    # 3. VALIDATE EMAIL FORMAT
    df = DataCleaner.validate_email(df, email_column="email", drop_invalid=True)
    
    # 4. CONVERT NAMES TO UPPERCASE
    df["name"] = df["name"].str.upper()
    
    # 5. TRIM SPACES IN ALL STRING COLUMNS
    df = DataCleaner.trim_whitespace(df)
    
    # 6. SORT BY MOST RECENT ORDER DATE
    df = DataCleaner.typecast_column(df, "last_order_date", "datetime")
    df = df.sort_values(by="last_order_date", ascending=False)
    
    return df
