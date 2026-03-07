import pandas as pd
import numpy as np
import sys
import os

# Add project root to sys.path to allow importing from rule_baseline.utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from rule_baseline.utils import config

def analyze_data_quality(file_path):
    print(f"Loading data from {file_path}...")
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    print("\n--- Basic Info ---")
    print(f"Rows: {df.shape[0]}")
    print(f"Columns: {df.shape[1]}")
    print("\nColumn Data Types:")
    print(df.dtypes)

    print("\n--- Missing Values ---")
    missing = df.isnull().sum()
    if missing.sum() == 0:
        print("No missing values found.")
    else:
        print(missing[missing > 0])

    print("\n--- Duplicate Rows ---")
    duplicates = df.duplicated().sum()
    print(f"Number of duplicate rows: {duplicates}")

    print("\n--- Numerical Statistics ---")
    # Select numerical columns
    num_cols = ['price', 'horizon_hours', 'delta_hours', 'r_std', 'y']
    # Filter to only those that exist
    num_cols = [c for c in num_cols if c in df.columns]
    if num_cols:
        print(df[num_cols].describe())
    
    print("\n--- Categorical Analysis ---")
    if 'category' in df.columns:
        print("\nUnique Categories:")
        print(df['category'].value_counts())
    
    print("\n--- Logic Checks ---")
    # Check target variable 'y'
    if 'y' in df.columns:
        unique_y = df['y'].unique()
        print(f"\nUnique values in 'y' (expecting 0/1): {sorted(unique_y)}")
        invalid_y = df[~df['y'].isin([0, 1])]
        if not invalid_y.empty:
            print(f"WARNING: Found {len(invalid_y)} rows with invalid 'y' values.")

    # Check prices
    if 'price' in df.columns:
        invalid_prices = df[(df['price'] < 0) | (df['price'] > 1)]
        if not invalid_prices.empty:
            print(f"WARNING: Found {len(invalid_prices)} rows with price outside [0, 1].")
        else:
            print("All prices are within [0, 1].")

    # Check timestamps
    date_cols = ['scheduled_end', 'closedTime']
    for col in date_cols:
        if col in df.columns:
            try:
                # fast parse if iso format
                df[col] = pd.to_datetime(df[col], errors='coerce')
                nat_count = df[col].isna().sum()
                if nat_count > 0:
                     print(f"WARNING: {col} has {nat_count} unparseable dates.")
                else:
                    print(f"{col} parsed successfully. Range: {df[col].min()} to {df[col].max()}")
            except Exception as e:
                print(f"Error parsing {col}: {e}")

    if 'closedTime' in df.columns and 'scheduled_end' in df.columns:
         closed_before_sched = df[df['closedTime'] < df['scheduled_end']]
         closed_after_sched = df[df['closedTime'] > df['scheduled_end']]
         print(f"\nRows where closedTime < scheduled_end: {len(closed_before_sched)}")
         print(f"Rows where closedTime > scheduled_end: {len(closed_after_sched)}")

         print("\n--- Date Distribution Analysis (Binned by Month) ---")
         for col in ['scheduled_end', 'closedTime']:
             if col in df.columns:
                 print(f"\nDistribution for {col}:")
                 month_counts = df[col].dt.to_period('M').value_counts().sort_index()
                 print(month_counts)


    # Analyze correlations (numerical only)
    print("\n--- Correlation Matrix ---")
    print(df.select_dtypes(include=[np.number]).corr())

if __name__ == "__main__":
    file_path = config.SNAPSHOTS_PATH
    analyze_data_quality(file_path)
