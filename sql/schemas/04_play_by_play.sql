-- Phase 3: Play-by-Play Data for Advanced Analytics
-- This table stores granular play-level data for deep analysis

CREATE TABLE IF NOT EXISTS bronze.nfl_play_by_play (
    -- Game identifiers
    game_id VARCHAR NOT NULL,
    play_id BIGINT NOT NULL,
    drive DOUBLE,
    season INTEGER NOT NULL,
    week INTEGER NOT NULL,
    season_type VARCHAR,
    game_date DATE,
    start_time VARCHAR,
    
    -- Teams
    home_team VARCHAR,
    away_team VARCHAR,
    posteam VARCHAR,  -- Possession team
    defteam VARCHAR,  -- Defensive team
    posteam_type VARCHAR,  -- home/away
    
    -- Game situation
    qtr DOUBLE,
    quarter_seconds_remaining DOUBLE,
    game_seconds_remaining DOUBLE,
    half_seconds_remaining DOUBLE,
    game_half VARCHAR,
    drive_start_yard_line VARCHAR,
    drive_end_yard_line VARCHAR,
    
    -- Play details
    down DOUBLE,
    ydstogo DOUBLE,
    yardline_100 DOUBLE,  -- Yards from opponent's end zone
    side_of_field VARCHAR,
    goal_to_go BOOLEAN,
    
    -- Play description
    play_desc TEXT,
    play_type VARCHAR,
    play_type_nfl VARCHAR,
    
    -- Formation
    shotgun BOOLEAN,
    no_huddle BOOLEAN,
    qb_dropback BOOLEAN,
    qb_scramble BOOLEAN,
    
    -- Play outcome
    yards_gained DOUBLE,
    yards_after_catch DOUBLE,
    air_yards DOUBLE,
    first_down BOOLEAN,
    touchdown BOOLEAN,
    pass_touchdown BOOLEAN,
    rush_touchdown BOOLEAN,
    return_touchdown BOOLEAN,
    
    -- Passing play details
    pass BOOLEAN,
    pass_attempt BOOLEAN,
    complete_pass BOOLEAN,
    incomplete_pass BOOLEAN,
    passing_yards DOUBLE,
    passer_player_id VARCHAR,
    passer_player_name VARCHAR,
    receiver_player_id VARCHAR,
    receiver_player_name VARCHAR,
    pass_length VARCHAR,  -- short/deep
    pass_location VARCHAR,  -- left/middle/right
    interception BOOLEAN,
    
    -- Rushing play details
    rush BOOLEAN,
    rush_attempt BOOLEAN,
    rushing_yards DOUBLE,
    rusher_player_id VARCHAR,
    rusher_player_name VARCHAR,
    run_location VARCHAR,  -- left/middle/right
    run_gap VARCHAR,  -- end/tackle/guard/center
    
    -- Scoring
    td_player_id VARCHAR,
    td_player_name VARCHAR,
    td_team VARCHAR,
    two_point_attempt BOOLEAN,
    two_point_conv_result VARCHAR,
    extra_point_attempt BOOLEAN,
    extra_point_result VARCHAR,
    field_goal_attempt BOOLEAN,
    field_goal_result VARCHAR,
    kick_distance DOUBLE,
    
    -- Turnovers
    fumble BOOLEAN,
    fumble_lost BOOLEAN,
    fumble_recovery_1_player_id VARCHAR,
    fumble_recovery_1_team VARCHAR,
    
    -- Penalties
    penalty BOOLEAN,
    penalty_type VARCHAR,
    penalty_yards DOUBLE,
    penalty_team VARCHAR,
    
    -- Advanced metrics
    epa DOUBLE,  -- Expected Points Added
    wp DOUBLE,   -- Win Probability
    wpa DOUBLE,  -- Win Probability Added
    success BOOLEAN,  -- Successful play (positive EPA)
    cpoe DOUBLE,  -- Completion Percentage Over Expected
    
    -- EPA breakdowns
    air_epa DOUBLE,
    yac_epa DOUBLE,
    comp_air_epa DOUBLE,
    comp_yac_epa DOUBLE,
    total_home_epa DOUBLE,
    total_away_epa DOUBLE,
    
    -- Win probability
    vegas_wp DOUBLE,
    vegas_home_wp DOUBLE,
    home_wp DOUBLE,
    away_wp DOUBLE,
    
    -- Scoring probabilities
    td_prob DOUBLE,
    fg_prob DOUBLE,
    safety_prob DOUBLE,
    no_score_prob DOUBLE,
    
    -- Fantasy
    fantasy VARCHAR,  -- Comma-separated list of players involved
    fantasy_player_id VARCHAR,
    fantasy_player_name VARCHAR,
    
    -- Special teams
    special_teams_play BOOLEAN,
    st_play_type VARCHAR,
    kickoff_attempt BOOLEAN,
    punt_attempt BOOLEAN,
    return_yards DOUBLE,
    
    -- Sacks
    sack BOOLEAN,
    sack_player_id VARCHAR,
    sack_player_name VARCHAR,
    qb_hit BOOLEAN,
    
    -- Score state
    score_differential DOUBLE,
    score_differential_post DOUBLE,
    posteam_score DOUBLE,
    defteam_score DOUBLE,
    total_home_score DOUBLE,
    total_away_score DOUBLE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (game_id, play_id)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_pbp_season_week ON bronze.nfl_play_by_play(season, week);
CREATE INDEX IF NOT EXISTS idx_pbp_game_id ON bronze.nfl_play_by_play(game_id);
CREATE INDEX IF NOT EXISTS idx_pbp_passer ON bronze.nfl_play_by_play(passer_player_id);
CREATE INDEX IF NOT EXISTS idx_pbp_receiver ON bronze.nfl_play_by_play(receiver_player_id);
CREATE INDEX IF NOT EXISTS idx_pbp_rusher ON bronze.nfl_play_by_play(rusher_player_id);
CREATE INDEX IF NOT EXISTS idx_pbp_team ON bronze.nfl_play_by_play(posteam);
CREATE INDEX IF NOT EXISTS idx_pbp_play_type ON bronze.nfl_play_by_play(play_type);
CREATE INDEX IF NOT EXISTS idx_pbp_touchdown ON bronze.nfl_play_by_play(touchdown);
CREATE INDEX IF NOT EXISTS idx_pbp_fantasy ON bronze.nfl_play_by_play(fantasy_player_id);