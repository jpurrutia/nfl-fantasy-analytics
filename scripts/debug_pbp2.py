#!/usr/bin/env python3
"""Debug pbp data loading - column order"""

import nfl_data_py as nfl
import pandas as pd
from datetime import datetime

# Load a small sample
print("Loading sample data...")
pbp = nfl.import_pbp_data(years=[2023], columns=None, downcast=False, include_participation=False)

# Select columns that match our schema
columns_to_keep = [
    # Game identifiers
    'game_id', 'play_id', 'drive', 'season', 'week', 
    'season_type', 'game_date', 'start_time',
    # Teams
    'home_team', 'away_team', 'posteam', 'defteam', 'posteam_type',
    # Game situation
    'qtr', 'quarter_seconds_remaining', 'game_seconds_remaining',
    'half_seconds_remaining', 'game_half', 'drive_start_yard_line',
    'drive_end_yard_line',
    # Play details
    'down', 'ydstogo', 'yardline_100', 'side_of_field', 'goal_to_go',
    'play_type', 'play_type_nfl',
    # Formation
    'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble',
    # Play outcome
    'yards_gained', 'yards_after_catch', 'air_yards', 'first_down',
    'touchdown', 'pass_touchdown', 'rush_touchdown', 'return_touchdown',
    # Passing
    'pass', 'pass_attempt', 'complete_pass', 'incomplete_pass',
    'passing_yards', 'passer_player_id', 'passer_player_name',
    'receiver_player_id', 'receiver_player_name', 'pass_length',
    'pass_location', 'interception',
    # Rushing
    'rush', 'rush_attempt', 'rushing_yards', 'rusher_player_id',
    'rusher_player_name', 'run_location', 'run_gap',
    # Scoring
    'td_player_id', 'td_player_name', 'td_team', 'two_point_attempt',
    'two_point_conv_result', 'extra_point_attempt', 'extra_point_result',
    'field_goal_attempt', 'field_goal_result', 'kick_distance',
    # Turnovers
    'fumble', 'fumble_lost', 'fumble_recovery_1_player_id',
    'fumble_recovery_1_team',
    # Penalties
    'penalty', 'penalty_type', 'penalty_yards', 'penalty_team',
    # Advanced metrics
    'epa', 'wp', 'wpa', 'success', 'cpoe',
    'air_epa', 'yac_epa', 'comp_air_epa', 'comp_yac_epa',
    'total_home_epa', 'total_away_epa',
    # Win probability
    'vegas_wp', 'vegas_home_wp', 'home_wp', 'away_wp',
    # Scoring probabilities
    'td_prob', 'fg_prob', 'safety_prob', 'no_score_prob',
    # Fantasy
    'fantasy', 'fantasy_player_id', 'fantasy_player_name',
    # Special teams
    'special_teams_play', 'st_play_type', 'kickoff_attempt',
    'punt_attempt', 'return_yards',
    # Sacks
    'sack', 'sack_player_id', 'sack_player_name', 'qb_hit',
    # Score state
    'score_differential', 'score_differential_post', 'posteam_score',
    'defteam_score', 'total_home_score', 'total_away_score'
]

# Keep only columns that exist in the dataframe
available_cols = [col for col in columns_to_keep if col in pbp.columns]
pbp_filtered = pbp[available_cols].copy()

# Add the 'desc' column as 'play_desc' to avoid SQL reserved word conflict
if 'desc' in pbp.columns:
    pbp_filtered['play_desc'] = pbp['desc']

# Add created_at timestamp
pbp_filtered['created_at'] = datetime.now()

print(f"\nColumns in filtered dataframe: {len(pbp_filtered.columns)}")
print(f"Columns: {pbp_filtered.columns.tolist()}")

print("\nChecking column order and types:")
for i, col in enumerate(pbp_filtered.columns):
    dtype = pbp_filtered[col].dtype
    sample = pbp_filtered[col].iloc[0] if len(pbp_filtered) > 0 else None
    print(f"{i:3}: {col:30} {str(dtype):15} Sample: {sample}")