#!/usr/bin/env python3
"""Debug pbp data loading"""

import nfl_data_py as nfl
import pandas as pd

# Load a small sample
print("Loading sample data...")
pbp = nfl.import_pbp_data(years=[2023], columns=None, downcast=False, include_participation=False)

# Check the problematic columns
print("\nChecking drive_start_yard_line:")
print(f"Type: {pbp['drive_start_yard_line'].dtype}")
print(f"Sample values: {pbp['drive_start_yard_line'].head(10).tolist()}")

print("\nChecking other numeric columns that might have issues:")
for col in ['qtr', 'down', 'ydstogo', 'yardline_100', 'yards_gained']:
    if col in pbp.columns:
        print(f"\n{col}:")
        print(f"  Type: {pbp[col].dtype}")
        print(f"  Unique values (first 10): {pbp[col].dropna().unique()[:10]}")
        print(f"  Has nulls: {pbp[col].isna().any()}")