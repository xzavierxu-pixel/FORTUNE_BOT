import pandas as pd
import numpy as np
import argparse
import sys
from pathlib import Path

def run_dqc(df: pd.DataFrame, name: str):
    print(f"\n{'='*20} Data Quality Report: {name} {'='*20}")
    
    # Set pandas to show all rows in these specific prints
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # 1. Dimensions
    print(f"[1] Dimensions: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # 2. Duplicate Rows
    dup_rows = df.duplicated().sum()
    print(f"[2] Duplicate Rows: {dup_rows} ({dup_rows/len(df) if len(df)>0 else 0:.2%})")
    
    # 3. Duplicate Columns
    print(f"[3] Duplicate Columns (Exact Equality):")
    dup_groups = {}
    if df.shape[1] < 2000:
        for col in df.columns:
            try:
                col_values = tuple(df[col].values)
                if col_values not in dup_groups:
                    dup_groups[col_values] = []
                dup_groups[col_values].append(col)
            except:
                continue
    
    dup_count = 0
    for val, cols in dup_groups.items():
        if len(cols) > 1:
            dup_count += (len(cols) - 1)
            print(f"    - Group: {', '.join(cols)}")
    
    print(f"    - Total {dup_count} duplicate columns found.")

    # 4. Missing Values & Infinity
    null_counts = df.isnull().sum()
    inf_counts = (df.select_dtypes(include=[np.number]) == np.inf).sum() + \
                 (df.select_dtypes(include=[np.number]) == -np.inf).sum()
    
    null_pct = (null_counts / len(df)) * 100 if len(df)>0 else null_counts * 0
    missing_report = pd.DataFrame({
        'column': df.columns,
        'null_count': null_counts.values,
        'inf_count': [inf_counts.get(c, 0) for c in df.columns],
        'null_pct': null_pct.values
    }).sort_values('null_pct', ascending=False)
    
    high_missing = missing_report[(missing_report['null_pct'] > 0) | (missing_report['inf_count'] > 0)]
    print(f"[4] Columns with Missing or Inf Values: {len(high_missing)}")
    if not high_missing.empty:
        print(high_missing.to_string(index=False))

    # 5. Low Variance / Constant Features (Numeric)
    numeric_df = df.select_dtypes(include=[np.number])
    if not numeric_df.empty:
        nunique = numeric_df.nunique()
        constant_features = nunique[nunique <= 1].index.tolist()
        
        std_dev = numeric_df.std()
        low_var_features = std_dev[(std_dev > 0) & (std_dev < 1e-6)].index.tolist()
        
        print(f"[5] Constant Features (unique values <= 1): {len(constant_features)}")
        if constant_features:
            for feat in constant_features:
                print(f"    - {feat}")
                
        print(f"[6] Near-Constant Features (std < 1e-6): {len(low_var_features)}")
        if low_var_features:
            for feat in low_var_features:
                print(f"    - {feat}")

    # 6. Categorical Cardinality & Imbalance
    cat_df = df.select_dtypes(exclude=[np.number])
    if not cat_df.empty:
        cardinality = cat_df.nunique().sort_values(ascending=False)
        print(f"[7] High Cardinality Features (>50 unique): {len(cardinality[cardinality > 50])}")
        if not cardinality[cardinality > 50].empty:
            print(cardinality[cardinality > 50].to_string())
            
        print(f"[8] Highly Imbalanced Categorical (>99% same value):")
        for col in cat_df.columns:
            if not cat_df[col].empty:
                top_freq = cat_df[col].value_counts(normalize=True).iloc[0]
                if top_freq > 0.99:
                    print(f"    - {col}: {top_freq:.2%}")

    return {
        'shape': df.shape,
        'columns': set(df.columns),
        'missing_report': missing_report
    }

def compare_datasets(train_meta, valid_meta):
    print(f"\n{'='*20} Dataset Comparison {'='*20}")
    
    # Schema consistency
    train_cols = train_meta['columns']
    valid_cols = valid_meta['columns']
    
    only_in_train = train_cols - valid_cols
    only_in_valid = valid_cols - train_cols
    
    if not only_in_train and not only_in_valid:
        print("[OK] Schemas match perfectly.")
    else:
        if only_in_train:
            print(f"[WARN] Columns only in TRAIN: {only_in_train}")
        if only_in_valid:
            print(f"[WARN] Columns only in VALID: {only_in_valid}")

    # Missing value drift
    train_missing = train_meta['missing_report'].set_index('column')['null_pct']
    valid_missing = valid_meta['missing_report'].set_index('column')['null_pct']
    
    missing_drift = (valid_missing - train_missing).abs().sort_values(ascending=False)
    significant_drift = missing_drift[missing_drift > 5] # 5% threshold
    
    if not significant_drift.empty:
        print(f"[WARN] Significant missingness drift (>5% difference):")
        for col, diff in significant_drift.items():
            print(f"    - {col}: Train={train_missing.get(col, 0):.2f}%, Valid={valid_missing.get(col, 0):.2f}% (Diff={diff:.2f}%)")

class Tee:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

def main():
    parser = argparse.ArgumentParser(description="Data Quality Check for Parquet files")
    parser.add_argument("--train", type=str, default="polymarket_rule_engine/data/processed/train.parquet")
    parser.add_argument("--valid", type=str, default="polymarket_rule_engine/data/processed/valid.parquet")
    parser.add_argument("--output_dir", type=str, default="polymarket_rule_engine/data/analysis/quality_check")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "dqc_summary.txt"
    
    # Start capturing output
    sys.stdout = Tee(str(summary_path))

    train_path = Path(args.train)
    valid_path = Path(args.valid)

    if not train_path.exists():
        print(f"Error: Train file not found at {train_path}")
        return

    print(f"Loading {train_path}...")
    df_train = pd.read_parquet(train_path)
    train_meta = run_dqc(df_train, "TRAIN")

    if valid_path.exists():
        print(f"\nLoading {valid_path}...")
        df_valid = pd.read_parquet(valid_path)
        valid_meta = run_dqc(df_valid, "VALID")
        
        compare_datasets(train_meta, valid_meta)
    else:
        print(f"\n[INFO] Valid file not found at {valid_path}, skipping comparison.")

if __name__ == "__main__":
    main()
