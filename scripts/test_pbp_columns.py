#!/usr/bin/env python3
"""Quick test to see what columns are in play-by-play data"""

import nfl_data_py as nfl
import pandas as pd

# Load just one week of recent data to see columns
print("Loading sample play-by-play data...")
try:
    # Try 2023 since 2024 may not be complete
    pbp = nfl.import_pbp_data(years=[2023], columns=None, downcast=False, include_participation=False)
except Exception as e:
    print(f"Error loading data: {e}")
    print("Trying with participation=False...")
    import sys
    sys.exit(1)

# Filter to just one game to reduce output
sample_game = pbp['game_id'].iloc[0]
sample = pbp[pbp['game_id'] == sample_game].head(5)

print(f"\nTotal columns: {len(pbp.columns)}")
print(f"Shape of full 2024 data: {pbp.shape}")
print(f"\nAll column names ({len(pbp.columns)} total):")
print(sorted(pbp.columns.tolist()))

# Group columns by category
print("\n\nColumns grouped by type:")

# Game info columns
game_cols = [c for c in pbp.columns if 'game' in c.lower() or 'week' in c or 'season' in c]
print(f"\nGame Info ({len(game_cols)}): {game_cols[:10]}")

# Play detail columns  
play_cols = [c for c in pbp.columns if 'play' in c.lower() or 'down' in c or 'yard' in c]
print(f"\nPlay Details ({len(play_cols)}): {play_cols[:10]}")

# Player columns
player_cols = [c for c in pbp.columns if 'player' in c.lower() or '_id' in c.lower()]
print(f"\nPlayer IDs ({len(player_cols)}): {player_cols[:10]}")

# EPA/Analytics columns
epa_cols = [c for c in pbp.columns if 'epa' in c.lower() or 'wp' in c.lower() or 'cpoe' in c.lower()]
print(f"\nAdvanced Metrics ({len(epa_cols)}): {epa_cols[:10]}")

# Fantasy columns
fantasy_cols = [c for c in pbp.columns if 'fantasy' in c.lower()]
print(f"\nFantasy ({len(fantasy_cols)}): {fantasy_cols}")

print("\n\nSample data types:")
print(pbp.dtypes.value_counts())