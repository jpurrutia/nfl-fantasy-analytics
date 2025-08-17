-- Phase 4: Advanced Metrics Tables (NGS and Snap Data)
-- These tables store Next Gen Stats and snap count data for advanced analytics

-- ============================================================================
-- BRONZE LAYER: Raw Advanced Metrics Data
-- ============================================================================

-- Snap count data from Pro Football Reference via nfl-data-py
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

-- Next Gen Stats: Passing metrics for QBs
CREATE TABLE IF NOT EXISTS bronze.nfl_ngs_passing (
    season INTEGER NOT NULL,
    season_type VARCHAR,
    week INTEGER NOT NULL,
    player_display_name VARCHAR NOT NULL,
    player_position VARCHAR,
    team_abbr VARCHAR,
    -- Time and pressure metrics
    avg_time_to_throw DOUBLE,
    avg_completed_air_yards DOUBLE,
    avg_intended_air_yards DOUBLE,
    avg_air_yards_differential DOUBLE,
    aggressiveness DOUBLE,  -- Throws into tight windows
    max_completed_air_distance DOUBLE,
    -- Performance metrics
    attempts INTEGER,
    completions INTEGER,
    completion_percentage DOUBLE,
    pass_yards INTEGER,
    pass_touchdowns INTEGER,
    interceptions INTEGER,
    passer_rating DOUBLE,
    -- Expected metrics
    expected_completion_percentage DOUBLE,
    completion_percentage_above_expectation DOUBLE,
    avg_air_distance DOUBLE,
    max_air_distance DOUBLE,
    -- Player identifiers
    player_gsis_id VARCHAR,
    player_first_name VARCHAR,
    player_last_name VARCHAR,
    player_jersey_number INTEGER,
    player_short_name VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season, week, player_gsis_id)
);

-- Next Gen Stats: Rushing metrics for RBs
CREATE TABLE IF NOT EXISTS bronze.nfl_ngs_rushing (
    season INTEGER NOT NULL,
    season_type VARCHAR,
    week INTEGER NOT NULL,
    player_display_name VARCHAR NOT NULL,
    player_position VARCHAR,
    team_abbr VARCHAR,
    -- Efficiency metrics
    efficiency DOUBLE,  -- Yards over expected
    percent_attempts_gte_eight_defenders DOUBLE,  -- Stacked box rate
    avg_time_to_los DOUBLE,  -- Speed to line of scrimmage
    expected_rush_yards DOUBLE,
    rush_yards_over_expected DOUBLE,
    rush_yards_over_expected_per_att DOUBLE,
    rush_pct_over_expected DOUBLE,
    -- Performance metrics
    rush_attempts INTEGER,
    rush_yards INTEGER,
    avg_rush_yards DOUBLE,
    rush_touchdowns INTEGER,
    -- Player identifiers
    player_gsis_id VARCHAR,
    player_first_name VARCHAR,
    player_last_name VARCHAR,
    player_jersey_number INTEGER,
    player_short_name VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season, week, player_gsis_id)
);

-- Next Gen Stats: Receiving metrics for WR/TE
CREATE TABLE IF NOT EXISTS bronze.nfl_ngs_receiving (
    season INTEGER NOT NULL,
    season_type VARCHAR,
    week INTEGER NOT NULL,
    player_display_name VARCHAR NOT NULL,
    player_position VARCHAR,
    team_abbr VARCHAR,
    -- Separation and coverage metrics
    avg_cushion DOUBLE,  -- Distance from defender at snap
    avg_separation DOUBLE,  -- Distance from defender at catch
    avg_intended_air_yards DOUBLE,
    percent_share_of_intended_air_yards DOUBLE,  -- Air yards market share
    -- Performance metrics
    receptions INTEGER,
    targets INTEGER,
    catch_percentage DOUBLE,
    yards INTEGER,
    rec_touchdowns INTEGER,
    -- YAC metrics
    avg_yac DOUBLE,
    avg_expected_yac DOUBLE,
    avg_yac_above_expectation DOUBLE,
    -- Player identifiers
    player_gsis_id VARCHAR,
    player_first_name VARCHAR,
    player_last_name VARCHAR,
    player_jersey_number INTEGER,
    player_short_name VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (season, week, player_gsis_id)
);

-- ============================================================================
-- SILVER LAYER: Calculated Opportunity Metrics
-- ============================================================================

-- Player opportunity metrics aggregated from multiple bronze sources
CREATE TABLE IF NOT EXISTS silver.player_opportunity (
    player_id VARCHAR NOT NULL,
    week INTEGER NOT NULL,
    season INTEGER NOT NULL,
    position VARCHAR,
    team VARCHAR,
    
    -- Snap data (from bronze.nfl_snap_counts)
    snap_count INTEGER,
    snap_pct DECIMAL(5,2),
    
    -- Universal opportunity metrics (from bronze.nfl_play_by_play)
    touches INTEGER,  -- carries + receptions
    total_yards INTEGER,  -- rushing + receiving yards
    
    -- Target/carry data (from bronze.nfl_play_by_play aggregation)
    targets INTEGER,
    target_share DECIMAL(5,2),  -- player targets / team targets
    receptions INTEGER,
    carries INTEGER,
    carry_share DECIMAL(5,2),  -- player carries / team carries
    
    -- Red zone data (from bronze.nfl_play_by_play aggregation)
    rz_touches INTEGER,
    rz_share DECIMAL(5,2),
    rz_targets INTEGER,
    rz_carries INTEGER,
    
    -- Air yards (from bronze.nfl_play_by_play)
    air_yards INTEGER,
    air_yards_share DECIMAL(5,2),
    
    -- Route data (from bronze.nfl_ngs_receiving + snap data)
    routes_run INTEGER,
    route_participation DECIMAL(5,2),
    
    -- QB specific metrics (from bronze.nfl_ngs_passing)
    pass_attempts INTEGER,
    dropbacks INTEGER,
    time_to_throw DOUBLE,
    aggressiveness_score DOUBLE,
    
    -- RB specific metrics (from bronze.nfl_ngs_rushing)
    stacked_box_rate DECIMAL(5,2),  -- 8+ defenders
    light_box_rate DECIMAL(5,2),  -- 6 or fewer defenders
    yards_over_expected DOUBLE,
    
    -- WR/TE specific metrics (from bronze.nfl_ngs_receiving)
    avg_separation DOUBLE,
    avg_cushion DOUBLE,
    yac_above_expected DOUBLE,
    
    -- Metadata
    data_sources VARCHAR,  -- Track which bronze tables contributed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (player_id, season, week)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_snap_counts_player ON bronze.nfl_snap_counts(player);
CREATE INDEX IF NOT EXISTS idx_snap_counts_season_week ON bronze.nfl_snap_counts(season, week);
CREATE INDEX IF NOT EXISTS idx_snap_counts_team ON bronze.nfl_snap_counts(team);

CREATE INDEX IF NOT EXISTS idx_ngs_passing_player ON bronze.nfl_ngs_passing(player_display_name);
CREATE INDEX IF NOT EXISTS idx_ngs_passing_season_week ON bronze.nfl_ngs_passing(season, week);

CREATE INDEX IF NOT EXISTS idx_ngs_rushing_player ON bronze.nfl_ngs_rushing(player_display_name);
CREATE INDEX IF NOT EXISTS idx_ngs_rushing_season_week ON bronze.nfl_ngs_rushing(season, week);

CREATE INDEX IF NOT EXISTS idx_ngs_receiving_player ON bronze.nfl_ngs_receiving(player_display_name);
CREATE INDEX IF NOT EXISTS idx_ngs_receiving_season_week ON bronze.nfl_ngs_receiving(season, week);

CREATE INDEX IF NOT EXISTS idx_player_opp_player ON silver.player_opportunity(player_id);
CREATE INDEX IF NOT EXISTS idx_player_opp_season_week ON silver.player_opportunity(season, week);
CREATE INDEX IF NOT EXISTS idx_player_opp_position ON silver.player_opportunity(position);
CREATE INDEX IF NOT EXISTS idx_player_opp_team ON silver.player_opportunity(team);