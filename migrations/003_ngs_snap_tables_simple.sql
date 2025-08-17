-- Migration 003: Add NGS and Snap Tables (Simplified)
-- Date: 2025-01-17
-- Description: Add NGS/snap tables to bronze, move opportunity to silver

-- Create snap count table
CREATE TABLE IF NOT EXISTS bronze.nfl_snap_counts (
    game_id VARCHAR NOT NULL,
    pfr_game_id VARCHAR,
    season INTEGER NOT NULL,
    game_type VARCHAR,
    week INTEGER NOT NULL,
    player VARCHAR NOT NULL,
    pfr_player_id VARCHAR,
    position VARCHAR,
    team VARCHAR,
    opponent VARCHAR,
    offense_snaps DOUBLE,
    offense_pct DOUBLE,
    defense_snaps DOUBLE,
    defense_pct DOUBLE,
    st_snaps DOUBLE,
    st_pct DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, player)
);

-- Create NGS passing table
CREATE TABLE IF NOT EXISTS bronze.nfl_ngs_passing (
    season INTEGER NOT NULL,
    season_type VARCHAR,
    week INTEGER NOT NULL,
    player_display_name VARCHAR NOT NULL,
    player_position VARCHAR,
    team_abbr VARCHAR,
    avg_time_to_throw DOUBLE,
    avg_completed_air_yards DOUBLE,
    avg_intended_air_yards DOUBLE,
    avg_air_yards_differential DOUBLE,
    aggressiveness DOUBLE,
    max_completed_air_distance DOUBLE,
    attempts INTEGER,
    completions INTEGER,
    completion_percentage DOUBLE,
    pass_yards INTEGER,
    pass_touchdowns INTEGER,
    interceptions INTEGER,
    passer_rating DOUBLE,
    expected_completion_percentage DOUBLE,
    completion_percentage_above_expectation DOUBLE,
    avg_air_distance DOUBLE,
    max_air_distance DOUBLE,
    player_gsis_id VARCHAR,
    player_first_name VARCHAR,
    player_last_name VARCHAR,
    player_jersey_number INTEGER,
    player_short_name VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season, week, player_gsis_id)
);

-- Create NGS rushing table
CREATE TABLE IF NOT EXISTS bronze.nfl_ngs_rushing (
    season INTEGER NOT NULL,
    season_type VARCHAR,
    week INTEGER NOT NULL,
    player_display_name VARCHAR NOT NULL,
    player_position VARCHAR,
    team_abbr VARCHAR,
    efficiency DOUBLE,
    percent_attempts_gte_eight_defenders DOUBLE,
    avg_time_to_los DOUBLE,
    expected_rush_yards DOUBLE,
    rush_yards_over_expected DOUBLE,
    rush_yards_over_expected_per_att DOUBLE,
    rush_pct_over_expected DOUBLE,
    rush_attempts INTEGER,
    rush_yards INTEGER,
    avg_rush_yards DOUBLE,
    rush_touchdowns INTEGER,
    player_gsis_id VARCHAR,
    player_first_name VARCHAR,
    player_last_name VARCHAR,
    player_jersey_number INTEGER,
    player_short_name VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season, week, player_gsis_id)
);

-- Create NGS receiving table
CREATE TABLE IF NOT EXISTS bronze.nfl_ngs_receiving (
    season INTEGER NOT NULL,
    season_type VARCHAR,
    week INTEGER NOT NULL,
    player_display_name VARCHAR NOT NULL,
    player_position VARCHAR,
    team_abbr VARCHAR,
    avg_cushion DOUBLE,
    avg_separation DOUBLE,
    avg_intended_air_yards DOUBLE,
    percent_share_of_intended_air_yards DOUBLE,
    receptions INTEGER,
    targets INTEGER,
    catch_percentage DOUBLE,
    yards INTEGER,
    rec_touchdowns INTEGER,
    avg_yac DOUBLE,
    avg_expected_yac DOUBLE,
    avg_yac_above_expectation DOUBLE,
    player_gsis_id VARCHAR,
    player_first_name VARCHAR,
    player_last_name VARCHAR,
    player_jersey_number INTEGER,
    player_short_name VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season, week, player_gsis_id)
);

-- Drop the empty bronze opportunity table
DROP TABLE IF EXISTS bronze.nfl_player_opportunity;

-- Create silver opportunity table
CREATE TABLE IF NOT EXISTS silver.player_opportunity (
    player_id VARCHAR NOT NULL,
    week INTEGER NOT NULL,
    season INTEGER NOT NULL,
    position VARCHAR,
    team VARCHAR,
    snap_count INTEGER,
    snap_pct DECIMAL(5,2),
    touches INTEGER,
    total_yards INTEGER,
    targets INTEGER,
    target_share DECIMAL(5,2),
    receptions INTEGER,
    carries INTEGER,
    carry_share DECIMAL(5,2),
    rz_touches INTEGER,
    rz_share DECIMAL(5,2),
    rz_targets INTEGER,
    rz_carries INTEGER,
    air_yards INTEGER,
    air_yards_share DECIMAL(5,2),
    routes_run INTEGER,
    route_participation DECIMAL(5,2),
    pass_attempts INTEGER,
    dropbacks INTEGER,
    time_to_throw DOUBLE,
    aggressiveness_score DOUBLE,
    stacked_box_rate DECIMAL(5,2),
    light_box_rate DECIMAL(5,2),
    yards_over_expected DOUBLE,
    avg_separation DOUBLE,
    avg_cushion DOUBLE,
    yac_above_expected DOUBLE,
    data_sources VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id, season, week)
);

-- Rename player_id_mapping to nfl_player_mapping
CREATE TABLE IF NOT EXISTS bronze.nfl_player_mapping AS SELECT * FROM bronze.player_id_mapping;
DROP TABLE IF EXISTS bronze.player_id_mapping;