import os
import sys

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config

file_path = config.RAW_MERGED_PATH

try:
    df = pd.read_csv(file_path, low_memory=False)
    print(f"Loaded {len(df)} records from {file_path}.")

    date_cols = ["startDate", "endDate", "closedTime", "creationDate"]
    existing_cols = [column for column in date_cols if column in df.columns]
    print(f"Date columns found: {existing_cols}")

    for column in existing_cols:
        df[column] = pd.to_datetime(df[column], utc=True, errors="coerce")

    print("\n--- Date Relationship Analysis ---")
    if "endDate" in df.columns:
        df["sched_end_used"] = df["endDate"]
    elif "endDateIso" in df.columns:
        df["sched_end_used"] = pd.to_datetime(df["endDateIso"], utc=True, errors="coerce")

    if "startDate" in df.columns and "sched_end_used" in df.columns:
        df["duration_hours"] = (df["sched_end_used"] - df["startDate"]).dt.total_seconds() / 3600
        print("\n[Duration: End - Start]")
        print(df["duration_hours"].describe())
        print(f"Negative Duration (End < Start): {len(df[df['duration_hours'] < 0])}")

    if "closedTime" in df.columns and "sched_end_used" in df.columns:
        df["resolution_diff_hours"] = (df["closedTime"] - df["sched_end_used"]).dt.total_seconds() / 3600
        print("\n[Resolution Diff: Closed - Scheduled End]")
        print(df["resolution_diff_hours"].describe())

    print("\n--- Missing Values ---")
    print(df[existing_cols].isnull().sum())

except FileNotFoundError:
    print(f"File not found: {file_path}")
except Exception as exc:
    print(f"An error occurred: {exc}")
